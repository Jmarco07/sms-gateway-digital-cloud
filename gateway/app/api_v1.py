"""Tenant-facing API (/v1) and the carrier DLR callback."""
import hashlib
import hmac
import json
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Header, Query, Request

from app import queries
from app.errors import ApiError
from app.events import publish
from app.schemas import BatchIn, MessageIn, WebhookIn
from app.service import accept_messages, dispatch_accepted, request_hash
from app.state import State
from app.transitions import DLR_STATUS, apply_stored_receipts

router = APIRouter()


def get_state(request: Request) -> State:
    return request.app.state.gw


async def current_tenant(request: Request, x_api_key: str | None = Header(default=None)) -> dict:
    if not x_api_key:
        raise ApiError(401, "UNAUTHENTICATED", "missing X-API-Key header")
    state = get_state(request)
    digest = hashlib.sha256(x_api_key.encode()).hexdigest()
    row = await state.pool.fetchrow("SELECT id, name FROM tenants WHERE api_key_hash = $1", digest)
    if not row:
        raise ApiError(401, "UNAUTHENTICATED", "invalid API key")
    return {"id": row["id"], "name": row["name"]}


def _idempotency_key(value: str | None) -> str | None:
    if value is None:
        return None
    if not value.strip() or len(value) > 255:
        raise ApiError(422, "VALIDATION_ERROR", "Idempotency-Key must be 1-255 characters")
    return value


def _parse_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError:
        raise ApiError(404, "NOT_FOUND", "message not found")


# ---------------------------------------------------------------------------
# send
# ---------------------------------------------------------------------------
@router.post("/v1/messages", status_code=202)
async def send_message(body: MessageIn, request: Request, tenant: dict = Depends(current_tenant),
                       idempotency_key: str | None = Header(default=None)):
    state = get_state(request)
    key = _idempotency_key(idempotency_key)
    result = await accept_messages(
        state, tenant["id"], [body], batch=False, idempotency_key=key,
        req_hash=request_hash("/v1/messages", body.model_dump(by_alias=True)),
    )
    if result.created:
        await dispatch_accepted(state, tenant["id"], result.created)
    return result.response


@router.post("/v1/messages/batch", status_code=202)
async def send_batch(body: BatchIn, request: Request, tenant: dict = Depends(current_tenant),
                     idempotency_key: str | None = Header(default=None)):
    state = get_state(request)
    if len(body.messages) > state.settings.max_batch:
        raise ApiError(422, "VALIDATION_ERROR", f"a batch holds at most {state.settings.max_batch} messages")
    key = _idempotency_key(idempotency_key)
    result = await accept_messages(
        state, tenant["id"], body.messages, batch=True, idempotency_key=key,
        req_hash=request_hash("/v1/messages/batch", body.model_dump(by_alias=True)),
    )
    if result.created:
        await dispatch_accepted(state, tenant["id"], result.created)
    return result.response


# ---------------------------------------------------------------------------
# read (always scoped to the calling tenant)
# ---------------------------------------------------------------------------
@router.get("/v1/messages")
async def list_messages(request: Request, tenant: dict = Depends(current_tenant),
                        status: str | None = Query(default=None),
                        created_from: datetime | None = Query(default=None),
                        created_to: datetime | None = Query(default=None),
                        client_ref: str | None = Query(default=None),
                        limit: int = Query(default=50, ge=1, le=500),
                        offset: int = Query(default=0, ge=0)):
    if status and status not in queries.STATUSES:
        raise ApiError(422, "VALIDATION_ERROR", f"status must be one of {', '.join(queries.STATUSES)}")
    state = get_state(request)
    async with state.pool.acquire() as conn:
        items = await queries.list_messages(
            conn, tenant_id=tenant["id"], status=status, created_from=created_from,
            created_to=created_to, client_ref=client_ref, limit=limit, offset=offset)
    return {"items": items, "limit": limit, "offset": offset}


@router.get("/v1/messages/{message_id}")
async def get_message(message_id: str, request: Request, tenant: dict = Depends(current_tenant)):
    state = get_state(request)
    async with state.pool.acquire() as conn:
        message = await queries.get_message(conn, _parse_uuid(message_id), tenant_id=tenant["id"])
    if not message:
        raise ApiError(404, "NOT_FOUND", "message not found")
    return message


@router.get("/v1/balance")
async def balance(request: Request, tenant: dict = Depends(current_tenant)):
    state = get_state(request)
    async with state.pool.acquire() as conn:
        money = (await queries.tenant_money(conn, tenant["id"]))[0]
    return {
        "balance_micros": money["balance_micros"],
        "opening_balance_micros": money["opening_balance_micros"],
        "ledger_total_micros": money["ledger_total_micros"],
        "balance_consistent": money["balance_consistent"],
    }


@router.get("/v1/ledger")
async def ledger(request: Request, tenant: dict = Depends(current_tenant),
                 after_id: int = Query(default=0, ge=0), limit: int = Query(default=1000, ge=1, le=5000)):
    state = get_state(request)
    rows = await state.pool.fetch(
        """SELECT id, message_id, entry_type, amount_micros, created_at FROM ledger_entries
            WHERE tenant_id = $1 AND id > $2 ORDER BY id LIMIT $3""",
        tenant["id"], after_id, limit)
    items = [dict(r) for r in rows]
    return {"items": items, "next_after_id": items[-1]["id"] if len(items) == limit else None}


@router.get("/v1/stats")
async def stats(request: Request, tenant: dict = Depends(current_tenant),
                created_from: datetime | None = Query(default=None)):
    state = get_state(request)
    async with state.pool.acquire() as conn:
        counts = (await queries.status_counts(conn, tenant["id"], created_from)).get(
            tenant["id"], {s: 0 for s in queries.STATUSES})
        money = (await queries.tenant_money(conn, tenant["id"]))[0]
    return {"balance_micros": money["balance_micros"], "counts": counts}


@router.get("/v1/webhook")
async def get_webhook(request: Request, tenant: dict = Depends(current_tenant)):
    row = await get_state(request).pool.fetchrow(
        "SELECT webhook_url, webhook_secret FROM tenants WHERE id = $1", tenant["id"])
    return {"url": row["webhook_url"], "signing_secret": row["webhook_secret"]}


@router.put("/v1/webhook")
async def set_webhook(body: WebhookIn, request: Request, tenant: dict = Depends(current_tenant)):
    row = await get_state(request).pool.fetchrow(
        "UPDATE tenants SET webhook_url = $2 WHERE id = $1 RETURNING webhook_url, webhook_secret",
        tenant["id"], body.url)
    return {"url": row["webhook_url"], "signing_secret": row["webhook_secret"]}


# ---------------------------------------------------------------------------
# carrier delivery receipts
# ---------------------------------------------------------------------------
@router.post("/carrier/dlr")
async def carrier_dlr(request: Request):
    state = get_state(request)
    raw = await request.body()
    given = request.headers.get("x-signature", "")
    expected = hmac.new(state.settings.dlr_secret.encode(), raw, hashlib.sha256).hexdigest()
    # Verify BEFORE parsing or touching anything. Constant-time comparison.
    if not given or not hmac.compare_digest(given.strip().lower().encode(), expected.encode()):
        raise ApiError(401, "INVALID_SIGNATURE", "missing or invalid X-Signature")

    try:
        data = json.loads(raw)
        carrier_id, status, timestamp = data["carrier_id"], data["status"], data["timestamp"]
        if not (isinstance(carrier_id, str) and isinstance(timestamp, str) and status in DLR_STATUS):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        raise ApiError(400, "BAD_DLR", "body must be {carrier_id, status: DELIVRD|UNDELIV|EXPIRED, timestamp}")

    # 1) Store the receipt and COMMIT. A duplicate hits the unique key and is ignored.
    await state.pool.execute(
        """INSERT INTO dlr_receipts (carrier_id, status, carrier_timestamp) VALUES ($1, $2, $3)
           ON CONFLICT (carrier_id, status, carrier_timestamp) DO NOTHING""",
        carrier_id, status, timestamp)
    # 2) Apply it in a second transaction. If the message does not have this carrier_id yet
    #    (receipt arrived before the submit response was saved), this finds nothing; the
    #    worker applies the stored receipt right after it saves the carrier_id.
    async with state.pool.acquire() as conn:
        async with conn.transaction():
            events = await apply_stored_receipts(conn, carrier_id)
    await publish(state.redis, events)
    return {"ok": True}


@router.get("/healthz")
async def healthz(request: Request):
    state = get_state(request)
    await state.pool.fetchval("SELECT 1")
    await state.redis.ping()
    return {"ok": True}
