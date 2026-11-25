"""Test fixtures.

The tests use REAL PostgreSQL and Redis (a separate database `sms_test` and Redis db 15),
because most of the guarantees under test (unique keys, row locks, atomic Lua scripts)
live in those systems. Only the carrier is faked, with httpx.MockTransport.

Run:  docker compose run --rm tests
"""
import asyncio
import dataclasses
import hashlib
import hmac
import json
import os
import uuid
from dataclasses import dataclass

# Point the app at the test database BEFORE any app module reads its settings.
ADMIN_URL = os.environ.get("TEST_ADMIN_DATABASE_URL", "postgresql://sms:sms@localhost:5432/postgres")
TEST_DB = "sms_test"
TEST_DB_URL = ADMIN_URL.rpartition("/")[0] + "/" + TEST_DB
os.environ["DATABASE_URL"] = TEST_DB_URL
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")
os.environ["DLR_SECRET"] = "test-dlr-secret"
os.environ["ADMIN_KEY"] = "test-admin-key"

import asyncpg  # noqa: E402
import httpx  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.main import create_app  # noqa: E402
from app.migrate import apply_migrations  # noqa: E402
from app.state import build_state  # noqa: E402
from app.worker import CarrierClient, Dispatcher  # noqa: E402

DLR_SECRET = "test-dlr-secret"
PRICE_BD = 6_000      # per segment, +880
PRICE_UAE = 35_000    # per segment, +971


@pytest.fixture(scope="session", autouse=True)
def database():
    async def setup():
        conn = await asyncpg.connect(ADMIN_URL)
        try:
            await conn.execute(f'DROP DATABASE IF EXISTS "{TEST_DB}" WITH (FORCE)')
            await conn.execute(f'CREATE DATABASE "{TEST_DB}"')
        finally:
            await conn.close()
        conn = await asyncpg.connect(TEST_DB_URL)
        try:
            await apply_migrations(conn)
        finally:
            await conn.close()

    asyncio.run(setup())


@pytest_asyncio.fixture
async def state():
    settings = dataclasses.replace(
        get_settings(), carrier_rps=100_000, backoff_base=0.001, backoff_cap=0.01, max_attempts=4)
    st = await build_state(settings, pool_size=25)
    await st.pool.execute(
        "TRUNCATE webhook_deliveries, dlr_receipts, idempotency_keys, ledger_entries, "
        "message_events, messages, tenants, prices RESTART IDENTITY CASCADE")
    await st.pool.executemany(
        "INSERT INTO prices (prefix, country, price_micros) VALUES ($1, $2, $3)",
        [("880", "Bangladesh", PRICE_BD), ("971", "UAE", PRICE_UAE)])
    await st.redis.flushdb()
    yield st
    await st.close()


@pytest_asyncio.fixture
async def client(state):
    app = create_app(state.settings)
    app.state.gw = state  # the lifespan is not run by ASGITransport; inject the shared state
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c


@dataclass
class Tenant:
    id: uuid.UUID
    name: str
    key: str
    secret: str
    opening: int

    @property
    def headers(self) -> dict:
        return {"X-API-Key": self.key}


async def make_tenant(state, name: str, balance: int = 1_000_000_000, webhook_url: str | None = None) -> Tenant:
    key = "key-" + uuid.uuid4().hex
    row = await state.pool.fetchrow(
        """INSERT INTO tenants (name, api_key_hash, opening_balance_micros, balance_micros, webhook_url)
           VALUES ($1, $2, $3, $3, $4) RETURNING id, webhook_secret""",
        name, hashlib.sha256(key.encode()).hexdigest(), balance, webhook_url)
    return Tenant(row["id"], name, key, row["webhook_secret"], balance)


@pytest_asyncio.fixture
async def tenant(state):
    return await make_tenant(state, "Acme Test")


@pytest_asyncio.fixture
async def tenant_b(state):
    return await make_tenant(state, "ShopFast Test")


def msg(text: str = "hello", to: str = "+8801711000001", **extra) -> dict:
    return {"to": to, "from": "ACME", "text": text, **extra}


def sign(body: bytes, secret: str = DLR_SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def dlr_body(carrier_id: str, status: str = "DELIVRD", timestamp: str = "2026-01-01T00:00:00+00:00") -> bytes:
    return json.dumps({"carrier_id": carrier_id, "status": status, "timestamp": timestamp},
                      separators=(",", ":")).encode()


async def post_dlr(client, body: bytes, signature: str | None = None):
    headers = {"Content-Type": "application/json"}
    headers["X-Signature"] = sign(body) if signature is None else signature
    return await client.post("/carrier/dlr", content=body, headers=headers)


class FakeCarrier:
    """Scripted carrier. Each call takes the next item of `script`:
        int                   -> an HTTP response with that status
        Exception instance    -> raised (e.g. httpx.ReadTimeout)
        callable(request)     -> may be async; returns an httpx.Response
    When the script is empty, the submit is accepted with a fresh carrier id."""

    def __init__(self, script: list | None = None):
        self.script = list(script or [])
        self.calls: list[dict] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.calls.append(body)
        item = self.script.pop(0) if self.script else None
        if isinstance(item, Exception):
            raise item
        if callable(item):
            result = item(request)
            return await result if asyncio.iscoroutine(result) else result
        if isinstance(item, int):
            payload = {"error": "INVALID_DESTINATION"} if item == 400 else {"error": "x"}
            return httpx.Response(item, json=payload)
        return httpx.Response(200, json={"carrier_id": f"c-{body['message_id'][:8]}"})


def make_dispatcher(state, carrier: FakeCarrier) -> Dispatcher:
    http = httpx.AsyncClient(transport=httpx.MockTransport(carrier))
    return Dispatcher(state, CarrierClient(http, "http://carrier", 3.0))


async def send(client, tenant: Tenant, body: dict | None = None, **headers):
    return await client.post("/v1/messages", json=body or msg(), headers={**tenant.headers, **headers})


async def fetch(client, tenant: Tenant, message_id: str) -> dict:
    r = await client.get(f"/v1/messages/{message_id}", headers=tenant.headers)
    assert r.status_code == 200, r.text
    return r.json()


def statuses(message: dict) -> list[str]:
    return [e["status"] for e in message["events"] if e["kind"] == "STATUS"]


async def assert_ledger_consistent(state, tenant: Tenant) -> None:
    """balance == opening balance - sum(ledger), and no message is charged or refunded twice."""
    row = await state.pool.fetchrow(
        "SELECT balance_micros, opening_balance_micros FROM tenants WHERE id = $1", tenant.id)
    total = await state.pool.fetchval(
        "SELECT COALESCE(SUM(amount_micros), 0) FROM ledger_entries WHERE tenant_id = $1", tenant.id)
    assert row["balance_micros"] >= 0
    assert row["balance_micros"] == row["opening_balance_micros"] - total
    dupes = await state.pool.fetchval(
        "SELECT count(*) FROM (SELECT message_id, entry_type FROM ledger_entries WHERE tenant_id = $1 "
        "GROUP BY 1, 2 HAVING count(*) > 1) d", tenant.id)
    assert dupes == 0
