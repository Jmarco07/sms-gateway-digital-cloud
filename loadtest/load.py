"""Load script: a ShopFast campaign and an Acme OTP every second, at the same time.

    docker compose --profile tools run --rm loadtest                 # 10,000 messages
    docker compose --profile tools run --rm loadtest --messages 500  # quick smoke run

Talks to the public API only (plus the mock carrier's /stats). When every message has
reached a final state it prints:
  * total time and the average submit rate to the carrier
  * OTP time from ACCEPTED to SUBMITTED (p50 / p95)
  * message counts per final state
  * a billing check per tenant (balance == opening - ledger, nobody charged twice)
The exit code is 1 if a billing check fails.
"""
import argparse
import asyncio
import math
import os
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import httpx

FINAL = ("DELIVERED", "UNDELIVERED", "EXPIRED", "FAILED")


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def pct(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, math.ceil(p / 100 * len(ordered)) - 1))]


def micros(value: int) -> str:
    return f"{value / 1_000_000:,.4f}"


async def wait_for_gateway(client: httpx.AsyncClient) -> None:
    for _ in range(60):
        try:
            if (await client.get("/healthz")).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1)
    sys.exit("gateway is not reachable")


async def list_all(client, headers, since: str) -> list[dict]:
    items, offset = [], 0
    while True:
        r = await client.get("/v1/messages", headers=headers,
                             params={"created_from": since, "limit": 500, "offset": offset})
        r.raise_for_status()
        page = r.json()["items"]
        items += page
        if len(page) < 500:
            return items
        offset += 500


async def full_ledger(client, headers) -> list[dict]:
    entries, after = [], 0
    while True:
        r = await client.get("/v1/ledger", headers=headers, params={"after_id": after, "limit": 5000})
        r.raise_for_status()
        body = r.json()
        entries += body["items"]
        if not body["next_after_id"]:
            return entries
        after = body["next_after_id"]


async def run(args) -> int:
    gateway = os.environ.get("GATEWAY_URL", "http://localhost:8000")
    carrier_url = os.environ.get("CARRIER_URL", "http://localhost:9000")
    acme = {"X-API-Key": os.environ.get("ACME_API_KEY", "acme-demo-key-0001")}
    shop = {"X-API-Key": os.environ.get("SHOPFAST_API_KEY", "shopfast-demo-key-0001")}
    run_id = uuid.uuid4().hex[:8]
    n = args.messages

    async with httpx.AsyncClient(base_url=gateway, timeout=120,
                                 limits=httpx.Limits(max_connections=50)) as c, \
            httpx.AsyncClient(base_url=carrier_url, timeout=10) as carrier:
        await wait_for_gateway(c)
        try:
            await carrier.post("/stats/reset")
        except httpx.HTTPError:
            print("warning: carrier stats are not reachable; submit-rate numbers will be missing")

        before = {name: (await c.get("/v1/balance", headers=h)).json() for name, h in (("Acme", acme), ("ShopFast", shop))}
        since = (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat()
        print(f"run {run_id}: sending {n} ShopFast messages + 1 Acme OTP per second")
        print(f"balances before: Acme {micros(before['Acme']['balance_micros'])}, "
              f"ShopFast {micros(before['ShopFast']['balance_micros'])}\n")

        t0 = time.monotonic()
        campaign_done = asyncio.Event()
        shop_ids: list[str] = []
        otp_ids: list[str] = []
        otp_errors: list[str] = []

        async def campaign() -> None:
            messages = []
            for i in range(n):
                text = (f"Flash sale {run_id} #{i}: 50% off today only!" if i % 10
                        else f"আপনার অর্ডার #{i} নিশ্চিত হয়েছে")          # every 10th is UCS-2
                messages.append({"to": f"+88017{i % 100_000_000:08d}", "from": "SHOPFAST",
                                 "text": text, "client_ref": f"{run_id}-shop-{i}"})
            batches = [messages[i:i + 1000] for i in range(0, n, 1000)]
            responses = await asyncio.gather(*[
                c.post("/v1/messages/batch", json={"messages": b}, headers={**shop, "Idempotency-Key": f"{run_id}-batch-{k}"})
                for k, b in enumerate(batches)])
            for r in responses:
                if r.status_code != 202:
                    sys.exit(f"batch rejected: {r.status_code} {r.text[:300]}")
                shop_ids.extend(m["id"] for m in r.json()["messages"])
            print(f"[{time.monotonic() - t0:6.1f}s] campaign accepted: {len(shop_ids)} messages")

        async def otp_loop() -> None:
            k = 0
            while not campaign_done.is_set():
                k += 1
                started = time.monotonic()
                r = await c.post("/v1/messages", headers={**acme, "Idempotency-Key": f"{run_id}-otp-{k}"},
                                 json={"to": "+8801711000001", "from": "ACMEBANK",
                                       "text": f"Your Acme Bank code is {100000 + k}. Do not share it.",
                                       "client_ref": f"{run_id}-otp-{k}"})
                if r.status_code == 202:
                    otp_ids.append(r.json()["id"])
                else:
                    otp_errors.append(f"{r.status_code} {r.text[:120]}")
                await asyncio.sleep(max(0.0, 1.0 - (time.monotonic() - started)))

        async def all_final(headers, expected: int | None) -> None:
            while True:
                counts = (await c.get("/v1/stats", headers=headers, params={"created_from": since})).json()["counts"]
                total, done = sum(counts.values()), sum(counts[s] for s in FINAL)
                if done == total and (expected is None or total >= expected):
                    return
                if headers is shop:
                    print(f"[{time.monotonic() - t0:6.1f}s] ShopFast final {done}/{total}   "
                          f"submitted {counts['SUBMITTED']}  queued {counts['QUEUED']}")
                if time.monotonic() - t0 > args.timeout:
                    sys.exit("timeout: messages did not reach a final state")
                await asyncio.sleep(3)

        otp_task = asyncio.create_task(otp_loop())
        await campaign()
        await all_final(shop, n)
        campaign_done.set()
        await otp_task
        await all_final(acme, None)
        total_seconds = time.monotonic() - t0

        # ---------------------------------------------------------------- collect
        shop_set, otp_set = set(shop_ids), set(otp_ids)
        shop_msgs = [m for m in await list_all(c, shop, since) if m["id"] in shop_set]
        acme_msgs = [m for m in await list_all(c, acme, since) if m["id"] in otp_set]
        carrier_stats = {}
        try:
            carrier_stats = (await carrier.get("/stats")).json()
        except httpx.HTTPError:
            pass

        otp_latency, otp_unsubmitted = [], 0
        for m in acme_msgs:
            detail = (await c.get(f"/v1/messages/{m['id']}", headers=acme)).json()
            marks = {e["status"]: ts(e["created_at"]) for e in detail["events"] if e["kind"] == "STATUS"}
            if "SUBMITTED" in marks:
                otp_latency.append((marks["SUBMITTED"] - marks["ACCEPTED"]).total_seconds())
            else:
                otp_unsubmitted += 1

        # ----------------------------------------------------------------- report
        print("\n" + "=" * 68)
        print(f"LOAD TEST REPORT  (run {run_id})")
        print("=" * 68)
        print(f"Messages: {len(shop_msgs)} ShopFast + {len(acme_msgs)} Acme OTP")
        print(f"Total time until every message was final: {total_seconds:.1f} s")

        submitted = [ts(m["submitted_at"]) for m in shop_msgs if m["submitted_at"]]
        created = [ts(m["created_at"]) for m in shop_msgs]
        if submitted:
            window = (max(submitted) - min(created)).total_seconds()
            print(f"ShopFast: all accepted by the carrier within {window:.1f} s "
                  f"-> {len(submitted) / window:.1f} messages/s accepted (average)")
        if carrier_stats:
            calls = carrier_stats.get("submits", 0)
            print(f"Carrier submit calls (all outcomes): {calls}"
                  + (f" -> average {calls / window:.1f} calls/s over the submit window" if submitted else ""))
            print(f"Carrier 429 (over its 50/s limit): {carrier_stats.get('429', 0)}   "
                  f"503: {carrier_stats.get('503', 0)}   400: {carrier_stats.get('400', 0)}   "
                  f"timeouts: {carrier_stats.get('timeouts', 0)}   "
                  f"re-submits de-duplicated: {carrier_stats.get('duplicate_submits', 0)}")
            print(f"DLRs sent: {carrier_stats.get('dlr_sent', 0)}   duplicates: {carrier_stats.get('dlr_duplicates', 0)}   "
                  f"early (before submit response): {carrier_stats.get('dlr_early', 0)}")

        print("\nOTP (Acme Bank) time from ACCEPTED to SUBMITTED while the campaign ran:")
        if otp_latency:
            print(f"  samples {len(otp_latency)}   p50 {pct(otp_latency, 50) * 1000:.0f} ms   "
                  f"p95 {pct(otp_latency, 95) * 1000:.0f} ms   max {max(otp_latency) * 1000:.0f} ms"
                  + (f"   (never submitted: {otp_unsubmitted})" if otp_unsubmitted else ""))
        else:
            print("  no OTP reached SUBMITTED")
        if otp_errors:
            print(f"  OTP requests rejected: {len(otp_errors)} e.g. {otp_errors[0]}")

        print("\nFinal states:")
        for name, msgs in (("ShopFast", shop_msgs), ("Acme", acme_msgs)):
            counts = Counter(m["status"] for m in msgs)
            line = "  ".join(f"{s} {counts.get(s, 0)}" for s in FINAL)
            print(f"  {name:9s} {line}   (retried at least once: {sum(1 for m in msgs if m['attempts'] > 1)})")

        # ------------------------------------------------------------ billing check
        print("\nBilling check:")
        failures = 0
        for name, headers, msgs in (("Acme", acme, acme_msgs), ("ShopFast", shop, shop_msgs)):
            bal = (await c.get("/v1/balance", headers=headers)).json()
            ledger = await full_ledger(c, headers)
            ledger_sum = sum(e["amount_micros"] for e in ledger)
            equation = bal["balance_micros"] == bal["opening_balance_micros"] - ledger_sum

            charges, refunds = Counter(), Counter()
            for e in ledger:
                (charges if e["entry_type"] == "CHARGE" else refunds)[e["message_id"]] += 1
            double_charged = [m for m, k in charges.items() if k > 1] + [m for m, k in refunds.items() if k > 1]
            run_ids = {m["id"] for m in msgs}
            all_charged = all(charges[i] == 1 for i in run_ids)
            failed_ids = {m["id"] for m in msgs if m["status"] == "FAILED"}
            refund_rule = {i for i in run_ids if refunds[i]} == failed_ids      # refund only FAILED
            spent = before[name]["balance_micros"] - bal["balance_micros"]
            ok = equation and not double_charged and all_charged and refund_rule and bal["balance_micros"] >= 0
            failures += 0 if ok else 1
            print(f"  {name:9s} {'PASS' if ok else 'FAIL'}  balance {micros(bal['balance_micros'])} = "
                  f"opening {micros(bal['opening_balance_micros'])} - ledger {micros(ledger_sum)}  [{'ok' if equation else 'MISMATCH'}]")
            print(f"            charged once each: {'yes' if all_charged else 'NO'}   "
                  f"double charge/refund entries: {len(double_charged)}   "
                  f"refunds only for FAILED ({len(failed_ids)}): {'yes' if refund_rule else 'NO'}   "
                  f"spent this run: {micros(spent)}")
        print("=" * 68)
        return 1 if failures else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", type=int, default=10_000, help="ShopFast campaign size")
    parser.add_argument("--timeout", type=float, default=1500, help="seconds to wait for final states")
    sys.exit(asyncio.run(run(parser.parse_args())))
