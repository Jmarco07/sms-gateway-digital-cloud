"""Internal admin API used by the dashboard. Protected by X-Admin-Key.

The dashboard's nginx adds the key, so the browser never holds it. This API is for
operators and is not part of the tenant contract.
"""
import asyncio
import hmac
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import StreamingResponse

from app import encoding, queries
from app.api_v1 import get_state
from app.errors import ApiError
from app.events import CHANNEL
from app.pricing import E164, load_price_table
from app.schemas import MessageIn
from app.service import accept_messages, dispatch_accepted, request_hash


async def require_admin(request: Request, x_admin_key: str | None = Header(default=None)) -> None:
    expected = get_state(request).settings.admin_key.encode()
    if not x_admin_key or not hmac.compare_digest(x_admin_key.encode(), expected):
        raise ApiError(401, "UNAUTHENTICATED", "missing or invalid X-Admin-Key")


router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])


@router.get("/tenants")
async def tenants(request: Request):
    state = get_state(request)
    async with state.pool.acquire() as conn:
        money = await queries.tenant_money(conn)
        counts = await queries.status_counts(conn)
    for t in money:
        t["counts"] = counts.get(t["id"], {s: 0 for s in queries.STATUSES})
    return {"items": money, "statuses": list(queries.STATUSES), "queue_depth": await state.queue.depth()}


@router.get("/messages")
async def messages(request: Request,
                   tenant_id: uuid.UUID | None = Query(default=None),
                   status: str | None = Query(default=None),
                   created_from: datetime | None = Query(default=None),
                   created_to: datetime | None = Query(default=None),
                   client_ref: str | None = Query(default=None),
                   limit: int = Query(default=50, ge=1, le=500),
                   offset: int = Query(default=0, ge=0)):
    if status and status not in queries.STATUSES:
        raise ApiError(422, "VALIDATION_ERROR", "unknown status")
    state = get_state(request)
    async with state.pool.acquire() as conn:
        items = await queries.list_messages(
            conn, tenant_id=tenant_id, status=status, created_from=created_from,
            created_to=created_to, client_ref=client_ref, limit=limit, offset=offset)
    return {"items": items, "limit": limit, "offset": offset}


@router.get("/messages/{message_id}")
async def message_detail(message_id: uuid.UUID, request: Request):
    state = get_state(request)
    async with state.pool.acquire() as conn:
        message = await queries.get_message(conn, message_id)
    if not message:
        raise ApiError(404, "NOT_FOUND", "message not found")
    return message


@router.get("/quote")
async def quote(request: Request, to: str = Query(default=""), text: str = Query(default="")):
    """Live preview for the send form: encoding, segments and price."""
    out: dict = {"encoding": None, "length": 0, "segments": 0, "price_per_segment_micros": None,
                 "price_micros": None, "country": None, "problems": []}
    if text:
        a = encoding.analyze(text)
        out.update(encoding=a.encoding, length=a.length, segments=a.segments)
    if to:
        if not E164.match(to):
            out["problems"].append("destination must be E.164, e.g. +8801711000001")
        else:
            async with get_state(request).pool.acquire() as conn:
                hit = (await load_price_table(conn)).lookup(to)
            if hit is None:
                out["problems"].append("no price for this destination")
            else:
                out["country"], out["price_per_segment_micros"] = hit
                if out["segments"]:
                    out["price_micros"] = hit[1] * out["segments"]
    return out


@router.post("/tenants/{tenant_id}/messages", status_code=202)
async def send_test_message(tenant_id: uuid.UUID, body: MessageIn, request: Request):
    state = get_state(request)
    if not await state.pool.fetchval("SELECT 1 FROM tenants WHERE id = $1", tenant_id):
        raise ApiError(404, "NOT_FOUND", "tenant not found")
    result = await accept_messages(
        state, tenant_id, [body], batch=False, idempotency_key=None,
        req_hash=request_hash("/admin/send", body.model_dump(by_alias=True)))
    await dispatch_accepted(state, tenant_id, result.created)
    return result.response


@router.get("/stream")
async def stream(request: Request):
    """Server-Sent Events: one `data:` line per message status change."""
    state = get_state(request)
    pubsub = state.redis.pubsub()
    await pubsub.subscribe(CHANNEL)

    async def gen():
        try:
            yield "retry: 2000\n: connected\n\n"
            while True:
                if await request.is_disconnected():
                    break
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=10)
                if msg and msg.get("type") == "message":
                    yield f"data: {msg['data']}\n\n"
                elif msg is None:
                    yield ": keep-alive\n\n"
                else:
                    await asyncio.sleep(0)
        finally:
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
