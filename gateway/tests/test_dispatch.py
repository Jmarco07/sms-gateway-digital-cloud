"""Dispatch: retries, DLR ordering and signatures, fairness, rate limit, crash recovery."""
import asyncio
import json

import httpx

from conftest import (
    assert_ledger_consistent, dlr_body, fetch, make_dispatcher, make_tenant, msg, post_dlr, send, sign,
    statuses, FakeCarrier,
)
from app.queue import DispatchQueue


# ------------------------------------------------------------------ retries
async def test_429_503_and_timeout_are_retried_until_success(client, state, tenant):
    mid = (await send(client, tenant)).json()["id"]
    carrier = FakeCarrier([429, 503, httpx.ReadTimeout("slow"), None])   # None -> accepted
    await make_dispatcher(state, carrier).run_until_idle()

    assert len(carrier.calls) == 4
    # every retry re-sends the SAME message_id (this is what makes a timeout safe)
    assert {c["message_id"] for c in carrier.calls} == {mid}
    m = await fetch(client, tenant, mid)
    assert m["status"] == "SUBMITTED" and m["attempts"] == 4
    assert [e["detail"]["error"] for e in m["events"] if e["kind"] == "ATTEMPT"] == \
        ["HTTP_429", "HTTP_503", "TIMEOUT"]
    await assert_ledger_consistent(state, tenant)


async def test_400_is_not_retried_and_the_charge_is_refunded(client, state, tenant):
    mid = (await send(client, tenant)).json()["id"]
    carrier = FakeCarrier([400])
    await make_dispatcher(state, carrier).run_until_idle()

    assert len(carrier.calls) == 1                       # no retry
    m = await fetch(client, tenant, mid)
    assert m["status"] == "FAILED" and "400" in m["last_error"]
    assert await state.pool.fetchval("SELECT balance_micros FROM tenants WHERE id = $1", tenant.id) == tenant.opening
    entries = await state.pool.fetch("SELECT entry_type, amount_micros FROM ledger_entries ORDER BY id")
    assert [(e["entry_type"], e["amount_micros"]) for e in entries] == [("CHARGE", 6000), ("REFUND", -6000)]
    await assert_ledger_consistent(state, tenant)


async def test_message_fails_and_is_refunded_after_max_attempts(client, state, tenant):
    mid = (await send(client, tenant)).json()["id"]
    carrier = FakeCarrier([503] * 10)
    await make_dispatcher(state, carrier).run_until_idle()

    assert len(carrier.calls) == state.settings.max_attempts
    m = await fetch(client, tenant, mid)
    assert m["status"] == "FAILED" and "MAX_ATTEMPTS" in m["last_error"]
    assert await state.pool.fetchval("SELECT balance_micros FROM tenants WHERE id = $1", tenant.id) == tenant.opening
    await assert_ledger_consistent(state, tenant)


async def test_undelivered_and_expired_are_not_refunded(client, state, tenant):
    ids = [(await send(client, tenant, msg(f"m{i}"))).json()["id"] for i in range(2)]
    await make_dispatcher(state, FakeCarrier()).run_until_idle()
    for mid, status in zip(ids, ["UNDELIV", "EXPIRED"]):
        carrier_id = (await fetch(client, tenant, mid))["carrier_id"]
        assert (await post_dlr(client, dlr_body(carrier_id, status))).status_code == 200

    assert [(await fetch(client, tenant, i))["status"] for i in ids] == ["UNDELIVERED", "EXPIRED"]
    assert await state.pool.fetchval("SELECT count(*) FROM ledger_entries WHERE entry_type='REFUND'") == 0
    assert await state.pool.fetchval("SELECT balance_micros FROM tenants WHERE id = $1", tenant.id) \
        == tenant.opening - 2 * 6000
    await assert_ledger_consistent(state, tenant)


# ---------------------------------------------------------------------- DLR
async def test_dlr_before_submit_response_is_not_lost_and_duplicates_are_ignored(client, state, tenant):
    mid = (await send(client, tenant)).json()["id"]
    body = dlr_body("c-early", "DELIVRD")

    async def carrier_sends_dlr_first(request):
        # The receipt arrives while the submit call is still open, so we do not know the
        # carrier_id yet.
        resp = await post_dlr(client, body)
        assert resp.status_code == 200
        assert (await fetch(client, tenant, mid))["status"] == "QUEUED"      # nothing applied yet
        return httpx.Response(200, json={"carrier_id": "c-early"})

    await make_dispatcher(state, FakeCarrier([carrier_sends_dlr_first])).run_until_idle()

    m = await fetch(client, tenant, mid)
    assert m["status"] == "DELIVERED"                                         # not lost, not overwritten
    assert statuses(m) == ["ACCEPTED", "QUEUED", "SUBMITTED", "DELIVERED"]

    # the same DLR again, and a conflicting later one: no effect at all
    events_before = len(m["events"])
    assert (await post_dlr(client, body)).status_code == 200
    assert (await post_dlr(client, dlr_body("c-early", "UNDELIV", "2026-02-02T00:00:00+00:00"))).status_code == 200
    m = await fetch(client, tenant, mid)
    assert m["status"] == "DELIVERED" and len(m["events"]) == events_before


async def test_dlr_for_unknown_carrier_id_is_stored_and_acknowledged(client, state):
    assert (await post_dlr(client, dlr_body("c-never-seen"))).status_code == 200
    assert await state.pool.fetchval("SELECT count(*) FROM dlr_receipts") == 1


async def test_dlr_with_missing_or_wrong_signature_is_rejected_and_changes_nothing(client, state, tenant):
    mid = (await send(client, tenant)).json()["id"]
    await make_dispatcher(state, FakeCarrier()).run_until_idle()
    before = await fetch(client, tenant, mid)
    assert before["status"] == "SUBMITTED"
    body = dlr_body(before["carrier_id"], "DELIVRD")

    missing = await client.post("/carrier/dlr", content=body)                         # no header
    wrong = await post_dlr(client, body, signature="0" * 64)                           # bad value
    wrong_key = await post_dlr(client, body, signature=sign(body, "other-secret"))
    tampered = await post_dlr(client, dlr_body(before["carrier_id"], "UNDELIV"),       # signed for other body
                              signature=sign(body))
    assert [r.status_code for r in (missing, wrong, wrong_key, tampered)] == [401, 401, 401, 401]

    after = await fetch(client, tenant, mid)
    assert after["status"] == "SUBMITTED" and after["events"] == before["events"]
    assert await state.pool.fetchval("SELECT count(*) FROM dlr_receipts") == 0

    assert (await post_dlr(client, body)).status_code == 200                           # the real one works
    assert (await fetch(client, tenant, mid))["status"] == "DELIVERED"


async def test_status_never_moves_backwards(client, state, tenant):
    from app import transitions
    mid = (await send(client, tenant)).json()["id"]
    await make_dispatcher(state, FakeCarrier()).run_until_idle()
    carrier_id = (await fetch(client, tenant, mid))["carrier_id"]
    await post_dlr(client, dlr_body(carrier_id, "DELIVRD"))
    async with state.pool.acquire() as conn:
        # late "submitted" and "failed" signals for a delivered message do nothing
        assert await transitions.mark_submitted(conn, mid, "c-other") is None
        assert await transitions.mark_failed(conn, mid, "late failure") is None
    m = await fetch(client, tenant, mid)
    assert m["status"] == "DELIVERED" and m["carrier_id"] == carrier_id
    assert await state.pool.fetchval("SELECT count(*) FROM ledger_entries WHERE entry_type='REFUND'") == 0


# ------------------------------------------------------------ queue behaviour
async def test_a_small_tenant_is_not_blocked_by_a_big_campaign(state):
    queue = DispatchQueue(state.redis)
    await queue.enqueue([("shopfast", f"campaign-{i}") for i in range(5000)])
    await queue.enqueue([("acme", "otp-1")])

    order = []
    for _ in range(4):
        order.append((await queue.dequeue()).tenant_id)
    assert "acme" in order[:2]            # served on the next turn, not after 5000 messages


async def test_round_robin_with_many_tenants(state):
    queue = DispatchQueue(state.redis)
    for t in range(50):
        await queue.enqueue([(f"t{t}", f"m-{t}-{i}") for i in range(20)])
    first_round = [(await queue.dequeue()).tenant_id for _ in range(50)]
    assert len(set(first_round)) == 50     # every tenant gets one turn before anyone gets two


async def test_enqueue_is_idempotent(state):
    queue = DispatchQueue(state.redis)
    assert await queue.enqueue([("t", "m1"), ("t", "m1")]) == 1
    assert await queue.enqueue([("t", "m1")]) == 0
    assert (await queue.dequeue()).message_id == "m1"
    assert await queue.dequeue() is None


async def test_global_rate_limiter_never_grants_more_than_the_limit(state):
    queue = DispatchQueue(state.redis)
    results = await asyncio.gather(*[queue.acquire_slot(5) for _ in range(40)])   # 40 competing workers
    granted = [r for r in results if r[0]]
    assert len(granted) == 5
    denied_wait = [r[1] for r in results if not r[0]]
    assert all(0 < w <= 1000 for w in denied_wait)
    await queue.release_slot(granted[0][1])                 # a returned slot can be used again
    assert (await queue.acquire_slot(5))[0] is True


# ---------------------------------------------------------- crash recovery
async def test_messages_survive_a_lost_queue_and_a_dead_worker(client, state, tenant):
    ids = [(await send(client, tenant, msg(f"m{i}"))).json()["id"] for i in range(3)]
    dispatcher = make_dispatcher(state, FakeCarrier())

    # Crash 1: Redis loses everything.
    await state.redis.flushdb()
    # Crash 2: a worker claimed the first message, then died before saving the result.
    await dispatcher.claim(ids[0])
    assert await dispatcher.claim(ids[0]) is None                       # the lease protects it
    assert await dispatcher.reconcile_once(grace_seconds=0) == 2        # lease holder is left alone
    await state.pool.execute("UPDATE messages SET lease_until = now() - interval '1 second' WHERE id = $1", ids[0])
    assert await dispatcher.reconcile_once(grace_seconds=0) == 1        # lease expired: queued again

    await dispatcher.run_until_idle()
    assert [(await fetch(client, tenant, i))["status"] for i in ids] == ["SUBMITTED"] * 3
    await assert_ledger_consistent(state, tenant)


async def test_two_workers_cannot_submit_the_same_message(client, state, tenant):
    mid = (await send(client, tenant)).json()["id"]
    carrier = FakeCarrier()
    d1, d2 = make_dispatcher(state, carrier), make_dispatcher(state, carrier)
    item = await state.queue.dequeue()
    # the same id sits in the queue twice (e.g. after a reconcile) and two workers take it
    outcomes = await asyncio.gather(d1.process(item), d2.process(item))
    assert sorted(outcomes) == ["skipped", "submitted"]
    assert len(carrier.calls) == 1


# ----------------------------------------------------------------- webhooks
async def test_webhook_is_written_with_the_final_status_and_retried_with_a_signature(client, state):
    from app.webhooks import WebhookSender, sign
    t = await make_tenant(state, "Hooked", webhook_url="http://tenant.example/hook")
    mid = (await send(client, t, msg(client_ref="r-1"))).json()["id"]
    await make_dispatcher(state, FakeCarrier()).run_until_idle()
    assert await state.pool.fetchval("SELECT count(*) FROM webhook_deliveries") == 0      # not final yet

    carrier_id = (await fetch(client, t, mid))["carrier_id"]
    await post_dlr(client, dlr_body(carrier_id, "DELIVRD"))
    await post_dlr(client, dlr_body(carrier_id, "DELIVRD"))                               # duplicate DLR
    assert await state.pool.fetchval("SELECT count(*) FROM webhook_deliveries") == 1      # exactly one callback

    received = []
    responses = [500, 200]

    async def tenant_endpoint(request: httpx.Request):
        received.append(request)
        return httpx.Response(responses.pop(0))

    sender = WebhookSender(state.pool, httpx.AsyncClient(transport=httpx.MockTransport(tenant_endpoint)),
                           state.settings)
    for row in await sender.claim_batch():
        await sender.deliver(row)
    row = await state.pool.fetchrow("SELECT state, attempts, last_error FROM webhook_deliveries")
    assert (row["state"], row["attempts"], row["last_error"]) == ("PENDING", 1, "HTTP 500")   # will retry

    await state.pool.execute("UPDATE webhook_deliveries SET next_attempt_at = now()")
    for row in await sender.claim_batch():
        await sender.deliver(row)
    assert await state.pool.fetchval("SELECT state FROM webhook_deliveries") == "DELIVERED"

    req = received[-1]
    assert req.headers["X-Signature"] == sign(t.secret, req.content)
    payload = json.loads(req.content)
    assert payload["message_id"] == mid and payload["status"] == "DELIVERED" and payload["client_ref"] == "r-1"


async def test_a_failed_message_also_triggers_the_webhook(client, state):
    t = await make_tenant(state, "Hooked", webhook_url="http://tenant.example/hook")
    await send(client, t)
    await make_dispatcher(state, FakeCarrier([400])).run_until_idle()
    payload = await state.pool.fetchval("SELECT payload FROM webhook_deliveries")
    assert payload["status"] == "FAILED"
