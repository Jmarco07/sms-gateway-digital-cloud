"""Dispatch worker: takes messages from the queue and submits them to the carrier.

Run with:  python -m app.worker   (start as many copies as you like)

Safety properties
-----------------
* Nothing is lost: a message is saved in PostgreSQL before it is queued. A worker that
  dies mid-flight leaves a lease that expires, and the reconciler queues the message
  again. Redis can be wiped completely; the reconciler rebuilds the queue.
* One worker at a time: `claim()` is one atomic UPDATE that sets a lease. A second copy
  of the same id (possible after a reconcile) finds the lease and does nothing.
* Carrier limit: every submit first takes a slot from the GLOBAL sliding-window limiter
  in Redis, so N worker processes together never exceed CARRIER_RPS.
* Fairness: slots are handed to tenants round-robin (see queue.py).
"""
import asyncio
import logging
import random
import signal
import time
from dataclasses import dataclass

import httpx

from app.config import Settings, get_settings
from app.events import publish
from app.queue import QueueItem
from app.state import State, build_state
from app.transitions import add_event, apply_stored_receipts, mark_failed, mark_queued, mark_submitted

log = logging.getLogger("worker")


@dataclass
class SubmitResult:
    kind: str                  # "ok" | "retry" | "permanent"
    carrier_id: str | None = None
    reason: str = ""


class CarrierClient:
    """Talks to the carrier and classifies the answer. It never decides about retries;
    the dispatcher does."""

    def __init__(self, http: httpx.AsyncClient, url: str, timeout: float):
        self.http, self.url, self.timeout = http, url.rstrip("/"), timeout

    async def submit(self, payload: dict) -> SubmitResult:
        try:
            resp = await self.http.post(f"{self.url}/submit", json=payload, timeout=self.timeout)
        except httpx.TimeoutException:
            # DANGEROUS case: the carrier may have accepted the message. We retry with the
            # SAME message_id and rely on carrier-side de-duplication. See DESIGN.md.
            return SubmitResult("retry", reason="TIMEOUT")
        except httpx.TransportError as exc:
            return SubmitResult("retry", reason=f"NETWORK_{type(exc).__name__}")
        code = resp.status_code
        if code == 200:
            try:
                return SubmitResult("ok", carrier_id=resp.json()["carrier_id"])
            except (ValueError, KeyError, TypeError):
                return SubmitResult("retry", reason="BAD_CARRIER_RESPONSE")
        if code == 429 or code >= 500:
            return SubmitResult("retry", reason=f"HTTP_{code}")
        try:
            error = resp.json().get("error", "")
        except ValueError:
            error = ""
        return SubmitResult("permanent", reason=f"CARRIER_{code}_{error}".rstrip("_"))


class Dispatcher:
    def __init__(self, state: State, carrier: CarrierClient):
        self.state, self.carrier = state, carrier
        self.settings: Settings = state.settings

    # ------------------------------------------------------------------ claim
    async def claim(self, message_id: str):
        return await self.state.pool.fetchrow(
            """UPDATE messages
                  SET lease_until = now() + make_interval(secs => $2), attempts = attempts + 1,
                      updated_at = now()
                WHERE id = $1 AND status IN ('ACCEPTED', 'QUEUED')
                  AND (lease_until IS NULL OR lease_until < now())
                  AND (next_attempt_at IS NULL OR next_attempt_at <= now() + interval '1 second')
            RETURNING id, tenant_id, to_number, sender, text, encoding, attempts""",
            message_id, float(self.settings.lease_seconds))

    def backoff(self, attempt: int) -> float:
        delay = min(self.settings.backoff_cap, self.settings.backoff_base * 2 ** (attempt - 1))
        return delay * random.uniform(0.5, 1.0)

    # ---------------------------------------------------------------- process
    async def process(self, item: QueueItem) -> str:
        """Returns 'skipped', 'submitted', 'retry', 'failed'."""
        row = await self.claim(item.message_id)
        if row is None:
            return "skipped"
        try:
            payload = {"message_id": str(row["id"]), "to": row["to_number"], "from": row["sender"],
                       "text": row["text"], "encoding": row["encoding"]}
            result = await self.carrier.submit(payload)
            if result.kind == "ok":
                await self._on_submitted(row, result.carrier_id)
                return "submitted"
            if result.kind == "permanent":
                await self._on_failed(row, result.reason)
                return "failed"
            return await self._on_retry(row, result.reason)
        except Exception:
            # Leave the lease in place. When it expires the reconciler queues the message
            # again; the carrier de-duplicates on message_id.
            log.exception("processing %s failed; the lease will expire", item.message_id)
            return "error"

    async def _on_submitted(self, row, carrier_id: str) -> None:
        async with self.state.pool.acquire() as conn:
            async with conn.transaction():
                ev = await mark_submitted(conn, row["id"], carrier_id)
            # Second, separate transaction: a DLR that arrived before this point is stored in
            # dlr_receipts and is applied now that the carrier_id is saved. It must run AFTER
            # the commit above (see the race analysis in DESIGN.md).
            async with conn.transaction():
                late = await apply_stored_receipts(conn, carrier_id)
        await publish(self.state.redis, ([ev] if ev else []) + late)

    async def _on_failed(self, row, reason: str) -> None:
        async with self.state.pool.acquire() as conn:
            async with conn.transaction():
                ev = await mark_failed(conn, row["id"], reason)
        await publish(self.state.redis, [ev] if ev else [])

    async def _on_retry(self, row, reason: str) -> str:
        attempts = row["attempts"]
        if attempts >= self.settings.max_attempts:
            await self._on_failed(row, f"MAX_ATTEMPTS_REACHED (last error: {reason})")
            return "failed"
        delay = self.backoff(attempts)
        async with self.state.pool.acquire() as conn:
            async with conn.transaction():
                status = await conn.fetchval(
                    """UPDATE messages
                          SET lease_until = NULL, last_error = $2, updated_at = now(),
                              next_attempt_at = now() + make_interval(secs => $3)
                        WHERE id = $1 AND status IN ('ACCEPTED', 'QUEUED') RETURNING status""",
                    row["id"], reason, delay)
                if status:
                    await add_event(conn, row["id"], status,
                                    {"attempt": attempts, "error": reason, "retry_in_seconds": round(delay, 2)},
                                    kind="ATTEMPT")
        if status:
            await self.state.queue.schedule_retry(str(row["tenant_id"]), str(row["id"]), delay)
        return "retry"

    # -------------------------------------------------------------- reconcile
    async def reconcile_once(self, grace_seconds: float = 3.0) -> int:
        """Queue messages that are in PostgreSQL but not in Redis: a crash between COMMIT and
        enqueue, a worker that died holding a lease, or a Redis that lost its data."""
        rows = await self.state.pool.fetch(
            """SELECT id, tenant_id, status FROM messages
                WHERE status IN ('ACCEPTED', 'QUEUED')
                  AND (lease_until IS NULL OR lease_until < now())
                  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                  AND created_at < now() - make_interval(secs => $1)
                ORDER BY created_at LIMIT 2000""", grace_seconds)
        if not rows:
            return 0
        known = await self.state.queue.known([str(r["id"]) for r in rows])
        missing = [r for r in rows if str(r["id"]) not in known]
        if not missing:
            return 0
        accepted = [r["id"] for r in missing if r["status"] == "ACCEPTED"]
        if accepted:
            async with self.state.pool.acquire() as conn:
                async with conn.transaction():
                    events = await mark_queued(conn, accepted)
            await publish(self.state.redis, events)
        await self.state.queue.enqueue([(str(r["tenant_id"]), str(r["id"])) for r in missing])
        log.warning("reconciler re-queued %d message(s)", len(missing))
        return len(missing)

    # ------------------------------------------------------------------- loops
    async def run(self, stop: asyncio.Event) -> None:
        sem = asyncio.Semaphore(self.settings.worker_concurrency)
        tasks: set[asyncio.Task] = set()
        helpers = [asyncio.create_task(self._mover_loop(stop)),
                   asyncio.create_task(self._reconcile_loop(stop))]
        log.info("worker started: limit %s submits/s, concurrency %s",
                 self.settings.carrier_rps, self.settings.worker_concurrency)
        queue = self.state.queue
        while not stop.is_set():
            await sem.acquire()
            token = None
            try:
                if not await queue.has_work():
                    sem.release()
                    await asyncio.sleep(0.05)
                    continue
                # 1) get permission from the global limiter, 2) pick the next message.
                ok, value = await queue.acquire_slot(self.settings.carrier_rps)
                if not ok:
                    sem.release()
                    await asyncio.sleep(min(int(value), 250) / 1000)
                    continue
                token = value
                item = await queue.dequeue()
                if item is None:
                    await queue.release_slot(token)
                    sem.release()
                    continue
            except Exception:
                log.exception("dispatch loop error")
                if token:
                    await queue.release_slot(token)
                sem.release()
                await asyncio.sleep(1)
                continue
            task = asyncio.create_task(self._run_item(item, token, sem))
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        log.info("stopping: waiting for %d in-flight submit(s)", len(tasks))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for h in helpers:
            h.cancel()

    async def _run_item(self, item: QueueItem, token: str, sem: asyncio.Semaphore) -> None:
        try:
            outcome = await self.process(item)
            if outcome == "skipped":
                await self.state.queue.release_slot(token)  # nothing was sent: give the slot back
        finally:
            sem.release()

    async def _mover_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.state.queue.move_due()
            except Exception:
                log.exception("move_due failed")
            await asyncio.sleep(0.2)

    async def _reconcile_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.reconcile_once()
            except Exception:
                log.exception("reconcile failed")
            await asyncio.sleep(5)

    # ------------------------------------------------------------- test hook
    async def run_until_idle(self, timeout: float = 15.0) -> None:
        """Process everything that is queued or delayed, ignoring the rate limiter.
        For tests only."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await self.state.queue.move_due()
            item = await self.state.queue.dequeue()
            if item:
                await self.process(item)
                continue
            if await self.state.redis.zcard("sms:delayed") == 0:
                return
            await asyncio.sleep(0.01)
        raise TimeoutError("queue did not drain")


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    state = await build_state(settings, pool_size=30)
    http = httpx.AsyncClient(limits=httpx.Limits(max_connections=settings.worker_concurrency + 20,
                                                 max_keepalive_connections=settings.worker_concurrency))
    dispatcher = Dispatcher(state, CarrierClient(http, settings.carrier_url, settings.carrier_timeout))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await dispatcher.run(stop)
    finally:
        await http.aclose()
        await state.close()


if __name__ == "__main__":
    asyncio.run(main())
