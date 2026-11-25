"""Tenant webhook delivery. Run with:  python -m app.webhooks

Why a slow tenant endpoint cannot slow down sending
---------------------------------------------------
* The callback is a row in `webhook_deliveries` (a transactional outbox), written in the
  same transaction as the final status change. The send path never calls a tenant URL.
* Delivery runs in this separate process, with its own database connections, a global
  concurrency limit and a smaller per-tenant limit, so one hanging endpoint cannot use up
  all delivery slots either.
* Failures are retried with exponential backoff; after WEBHOOK_MAX_ATTEMPTS the row is
  marked FAILED and kept for inspection.

Each request is signed:  X-Signature: sha256=<hex HMAC-SHA256 of the body, key = tenant's
webhook secret>. Delivery is at-least-once, so a tenant should de-duplicate on message_id.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import random
import signal

import asyncpg
import httpx

from app.config import Settings, get_settings
from app.db import create_pool

log = logging.getLogger("webhooks")

GLOBAL_CONCURRENCY = 50
PER_TENANT_CONCURRENCY = 5
CLAIM_SECONDS = 30  # a claimed row is invisible to other claimers for this long


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookSender:
    def __init__(self, pool: asyncpg.Pool, http: httpx.AsyncClient, settings: Settings):
        self.pool, self.http, self.settings = pool, http, settings
        self.global_sem = asyncio.Semaphore(GLOBAL_CONCURRENCY)
        self.tenant_sems: dict = {}

    async def claim_batch(self, limit: int = 100) -> list[asyncpg.Record]:
        """Claim due rows. The claim pushes next_attempt_at forward, so a crashed sender's
        rows become due again by themselves. SKIP LOCKED lets several senders run."""
        return await self.pool.fetch(
            """UPDATE webhook_deliveries d
                  SET attempts = attempts + 1,
                      next_attempt_at = now() + make_interval(secs => $2)
                WHERE d.id IN (SELECT id FROM webhook_deliveries
                                WHERE state = 'PENDING' AND next_attempt_at <= now()
                                ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED)
            RETURNING d.id, d.tenant_id, d.url, d.payload, d.attempts,
                      (SELECT webhook_secret FROM tenants t WHERE t.id = d.tenant_id) AS secret""",
            limit, float(CLAIM_SECONDS))

    async def deliver(self, row: asyncpg.Record) -> None:
        tenant_sem = self.tenant_sems.setdefault(row["tenant_id"], asyncio.Semaphore(PER_TENANT_CONCURRENCY))
        async with tenant_sem, self.global_sem:
            body = json.dumps(row["payload"], separators=(",", ":"), sort_keys=True).encode()
            error = None
            try:
                resp = await self.http.post(
                    row["url"], content=body, timeout=self.settings.webhook_timeout,
                    headers={"Content-Type": "application/json",
                             "X-Signature": sign(row["secret"], body),
                             "X-Delivery-Attempt": str(row["attempts"])})
                if 200 <= resp.status_code < 300:
                    await self.pool.execute(
                        "UPDATE webhook_deliveries SET state = 'DELIVERED', delivered_at = now(), "
                        "last_error = NULL WHERE id = $1", row["id"])
                    return
                error = f"HTTP {resp.status_code}"
            except httpx.HTTPError as exc:
                error = type(exc).__name__ or "HTTPError"
        await self.fail_or_retry(row, error)

    async def fail_or_retry(self, row: asyncpg.Record, error: str) -> None:
        if row["attempts"] >= self.settings.webhook_max_attempts:
            await self.pool.execute(
                "UPDATE webhook_deliveries SET state = 'FAILED', last_error = $2 WHERE id = $1",
                row["id"], error)
            log.warning("webhook %s gave up after %s attempts: %s", row["id"], row["attempts"], error)
            return
        delay = min(self.settings.webhook_backoff_cap,
                    self.settings.webhook_backoff_base * 2 ** (row["attempts"] - 1))
        delay *= random.uniform(0.5, 1.0)
        await self.pool.execute(
            "UPDATE webhook_deliveries SET last_error = $2, "
            "next_attempt_at = now() + make_interval(secs => $3) WHERE id = $1",
            row["id"], error, delay)

    async def run(self, stop: asyncio.Event) -> None:
        pending: set[asyncio.Task] = set()
        while not stop.is_set():
            try:
                rows = await self.claim_batch()
            except Exception:
                log.exception("claim failed")
                rows = []
            for row in rows:
                task = asyncio.create_task(self.deliver(row))
                pending.add(task)
                task.add_done_callback(pending.discard)
            await asyncio.sleep(0.2 if rows else 1.0)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    pool = await create_pool(settings.database_url, max_size=10)
    http = httpx.AsyncClient(limits=httpx.Limits(max_connections=GLOBAL_CONCURRENCY + 10))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    log.info("webhook sender started")
    try:
        await WebhookSender(pool, http, settings).run(stop)
    finally:
        await http.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
