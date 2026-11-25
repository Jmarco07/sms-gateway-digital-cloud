"""Send API: validation, idempotency, billing under concurrency, tenant isolation."""
import asyncio

from conftest import PRICE_BD, PRICE_UAE, assert_ledger_consistent, fetch, msg, send


# ---------------------------------------------------------------- validation
async def test_validation_errors_are_clear_and_nothing_is_charged(client, state, tenant):
    for body, field in [
        (msg(to="01711000001"), "to"),            # no '+'
        (msg(to="+0801711000001"), "to"),         # country code cannot start with 0
        (msg(to="+88017"), "to"),                 # too short
        (msg(to="+14155550100"), "to"),           # valid E.164 but no price
        (msg(text="   "), "text"),
    ]:
        r = await send(client, tenant, body)
        assert r.status_code == 422, (body, r.text)
        assert r.json()["error"]["code"] == "VALIDATION_ERROR"
        assert any(d["field"].endswith(field) for d in r.json()["error"]["details"]), r.text
    assert await state.pool.fetchval("SELECT count(*) FROM messages") == 0
    await assert_ledger_consistent(state, tenant)


async def test_price_is_segments_times_destination_price(client, tenant):
    one = await send(client, tenant, msg("short"))
    assert one.status_code == 202
    assert one.json()["segments"] == 1 and one.json()["price_micros"] == PRICE_BD
    # 161 characters: two GSM-7 segments, to the UAE
    two = await send(client, tenant, msg("a" * 161, to="+971501234567"))
    assert two.json()["segments"] == 2 and two.json()["price_micros"] == 2 * PRICE_UAE
    uni = await send(client, tenant, msg("আপনার কোড 😀"))
    assert uni.json()["encoding"] == "UCS2"


async def test_batch_is_all_or_nothing(client, state, tenant):
    body = {"messages": [msg("ok one"), msg("ok two"), msg("bad", to="123")]}
    r = await client.post("/v1/messages/batch", json=body, headers=tenant.headers)
    assert r.status_code == 422
    assert r.json()["error"]["details"][0]["field"] == "messages[2].to"
    assert await state.pool.fetchval("SELECT count(*) FROM messages") == 0

    good = {"messages": [msg(f"m{i}") for i in range(1000)]}
    r = await client.post("/v1/messages/batch", json=good, headers=tenant.headers)
    assert r.status_code == 202 and r.json()["count"] == 1000
    too_many = {"messages": [msg(f"m{i}") for i in range(1001)]}
    assert (await client.post("/v1/messages/batch", json=too_many, headers=tenant.headers)).status_code == 422
    await assert_ledger_consistent(state, tenant)


# --------------------------------------------------------------- idempotency
async def test_idempotency_same_key_same_body_creates_one_message_and_one_charge(client, state, tenant):
    headers = {"Idempotency-Key": "order-42"}
    first = await send(client, tenant, msg("otp 1"), **headers)
    second = await send(client, tenant, msg("otp 1"), **headers)

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()                       # the original response is replayed
    assert await state.pool.fetchval("SELECT count(*) FROM messages") == 1
    assert await state.pool.fetchval("SELECT count(*) FROM ledger_entries WHERE entry_type = 'CHARGE'") == 1
    assert await state.pool.fetchval("SELECT balance_micros FROM tenants WHERE id = $1", tenant.id) \
        == tenant.opening - PRICE_BD

    # same key, different body: an error, and still nothing new
    other = await send(client, tenant, msg("a different text"), **headers)
    assert other.status_code == 422 and other.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"
    assert await state.pool.fetchval("SELECT count(*) FROM messages") == 1
    await assert_ledger_consistent(state, tenant)


async def test_idempotency_survives_concurrent_duplicates(client, state, tenant):
    results = await asyncio.gather(*[
        send(client, tenant, msg("race"), **{"Idempotency-Key": "same-key"}) for _ in range(10)])
    assert {r.status_code for r in results} == {202}
    assert len({r.json()["id"] for r in results}) == 1
    assert await state.pool.fetchval("SELECT count(*) FROM messages") == 1
    assert await state.pool.fetchval("SELECT count(*) FROM ledger_entries") == 1


async def test_same_key_is_scoped_per_tenant(client, state, tenant, tenant_b):
    a = await send(client, tenant, msg("x"), **{"Idempotency-Key": "k"})
    b = await send(client, tenant_b, msg("x"), **{"Idempotency-Key": "k"})
    assert a.json()["id"] != b.json()["id"]


# ----------------------------------------------------------- balance / money
async def test_concurrent_sends_never_overspend(client, state):
    from conftest import make_tenant
    poor = await make_tenant(state, "Poor", balance=5 * PRICE_BD)   # enough for exactly 5 messages

    results = await asyncio.gather(*[send(client, poor, msg(f"concurrent {i}")) for i in range(30)])
    codes = [r.status_code for r in results]
    assert codes.count(202) == 5 and codes.count(402) == 25
    assert all(r.json()["error"]["code"] == "INSUFFICIENT_BALANCE" for r in results if r.status_code == 402)

    assert await state.pool.fetchval("SELECT balance_micros FROM tenants WHERE id = $1", poor.id) == 0
    assert await state.pool.fetchval("SELECT count(*) FROM messages") == 5
    await assert_ledger_consistent(state, poor)


async def test_insufficient_balance_creates_nothing(client, state):
    from conftest import make_tenant
    poor = await make_tenant(state, "Poor", balance=PRICE_BD)
    batch = {"messages": [msg("a"), msg("b")]}      # costs 2 x PRICE_BD, balance covers 1
    r = await client.post("/v1/messages/batch", json=batch, headers=poor.headers)
    assert r.status_code == 402
    assert await state.pool.fetchval("SELECT count(*) FROM messages") == 0
    assert await state.pool.fetchval("SELECT count(*) FROM ledger_entries") == 0
    await assert_ledger_consistent(state, poor)


async def test_database_refuses_a_negative_balance(state, tenant):
    """The CHECK constraint is the last line of defence even if application code is wrong."""
    import asyncpg
    import pytest
    with pytest.raises(asyncpg.CheckViolationError):
        await state.pool.execute("UPDATE tenants SET balance_micros = -1 WHERE id = $1", tenant.id)


# ---------------------------------------------------------- tenant isolation
async def test_tenant_cannot_read_another_tenants_data(client, state, tenant, tenant_b):
    mine = (await send(client, tenant, msg("secret otp"))).json()["id"]

    # the owner can read it
    assert (await fetch(client, tenant, mine))["text"] == "secret otp"
    # another tenant gets 404, not 403: it must not even learn that the id exists
    r = await client.get(f"/v1/messages/{mine}", headers=tenant_b.headers)
    assert r.status_code == 404
    # ... and cannot find it in lists, balance or ledger
    listing = await client.get("/v1/messages", headers=tenant_b.headers)
    assert listing.json()["items"] == []
    ledger = await client.get("/v1/ledger", headers=tenant_b.headers)
    assert ledger.json()["items"] == []
    bal = await client.get("/v1/balance", headers=tenant_b.headers)
    assert bal.json()["balance_micros"] == tenant_b.opening
    # unknown or missing keys are rejected
    assert (await client.get("/v1/messages", headers={"X-API-Key": "nope"})).status_code == 401
    assert (await client.get("/v1/messages")).status_code == 401
    # the admin API is not reachable with a tenant key
    assert (await client.get("/admin/tenants", headers=tenant.headers)).status_code == 401
    # a malformed id is a plain 404 too
    assert (await client.get("/v1/messages/not-a-uuid", headers=tenant.headers)).status_code == 404


async def test_list_filters(client, state, tenant):
    await send(client, tenant, msg("one", client_ref="ref-1"))
    await send(client, tenant, msg("two", client_ref="ref-2"))
    by_ref = await client.get("/v1/messages", params={"client_ref": "ref-2"}, headers=tenant.headers)
    assert [m["client_ref"] for m in by_ref.json()["items"]] == ["ref-2"]
    by_status = await client.get("/v1/messages", params={"status": "ACCEPTED"}, headers=tenant.headers)
    assert by_status.json()["items"] == [] or all(m["status"] == "ACCEPTED" for m in by_status.json()["items"])
    bad = await client.get("/v1/messages", params={"status": "NOPE"}, headers=tenant.headers)
    assert bad.status_code == 422
    future = await client.get("/v1/messages", params={"created_from": "2100-01-01T00:00:00Z"},
                              headers=tenant.headers)
    assert future.json()["items"] == []
