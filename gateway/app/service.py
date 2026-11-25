"""Accepting messages: validate, price, charge and persist in ONE transaction.

Transaction layout for POST /v1/messages and /v1/messages/batch
---------------------------------------------------------------
 1. INSERT the idempotency key (unique per tenant). If it already exists the earlier
    response is replayed (same body) or rejected (different body).
 2. Validate every message and compute encoding, segments and price.
 3. INSERT messages, their ACCEPTED events and their CHARGE ledger rows.
 4. UPDATE tenants SET balance = balance - total WHERE balance >= total   (last!)
 5. Store the response on the idempotency key. COMMIT.

Step 4 is a single conditional UPDATE, so the balance check and the debit cannot be
separated by a concurrent request, and the CHECK (balance >= 0) constraint backs it
up. It is deliberately the LAST statement: the tenant row lock is held from there to
COMMIT, so concurrent senders of the same tenant wait for as short a time as possible.
If anything fails, the whole transaction rolls back: no message, no charge, no key.
"""
import hashlib
import json
import uuid
from dataclasses import dataclass

import asyncpg

from app import encoding
from app.errors import ApiError
from app.pricing import E164, SENDER, PriceTable, load_price_table
from app.schemas import MessageIn
from app.state import State
from app.transitions import mark_queued
from app.events import publish, make_event


@dataclass
class Prepared:
    id: uuid.UUID
    to: str
    sender: str
    text: str
    client_ref: str | None
    encoding: str
    segments: int
    price_micros: int


@dataclass
class AcceptResult:
    status_code: int
    response: dict
    created: list[Prepared]          # empty when the response was replayed


def request_hash(path: str, payload: dict) -> str:
    canonical = json.dumps({"path": path, "body": payload}, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def validate_and_price(items: list[MessageIn], prices: PriceTable, max_segments: int,
                       batch: bool) -> list[Prepared]:
    prepared, problems = [], []
    for index, item in enumerate(items):
        prefix = f"messages[{index}]." if batch else ""
        if not E164.match(item.to):
            problems.append({"field": f"{prefix}to", "message": "must be E.164, e.g. +8801711000001"})
            continue
        if not SENDER.match(item.from_):
            problems.append({"field": f"{prefix}from",
                             "message": "sender id must be 1-11 letters/digits, or a phone number"})
            continue
        analysis = encoding.analyze(item.text)
        if analysis.segments > max_segments:
            problems.append({"field": f"{prefix}text",
                             "message": f"too long: {analysis.segments} segments, maximum is {max_segments}"})
            continue
        hit = prices.lookup(item.to)
        if hit is None:
            problems.append({"field": f"{prefix}to", "message": "no price for this destination"})
            continue
        _, per_segment = hit
        prepared.append(Prepared(
            id=uuid.uuid4(), to=item.to, sender=item.from_, text=item.text,
            client_ref=item.client_ref, encoding=analysis.encoding, segments=analysis.segments,
            price_micros=analysis.segments * per_segment,
        ))
    if problems:
        raise ApiError(422, "VALIDATION_ERROR", "one or more messages are invalid", problems)
    return prepared


def _view(p: Prepared) -> dict:
    return {
        "id": str(p.id), "status": "ACCEPTED", "to": p.to, "from": p.sender,
        "client_ref": p.client_ref, "encoding": p.encoding, "segments": p.segments,
        "price_micros": p.price_micros,
    }


async def accept_messages(state: State, tenant_id, items: list[MessageIn], *, batch: bool,
                          idempotency_key: str | None, req_hash: str) -> AcceptResult:
    async with state.pool.acquire() as conn:
        async with conn.transaction():
            if idempotency_key:
                inserted = await conn.fetchval(
                    """INSERT INTO idempotency_keys (tenant_id, key, request_hash)
                       VALUES ($1, $2, $3) ON CONFLICT (tenant_id, key) DO NOTHING RETURNING 1""",
                    tenant_id, idempotency_key, req_hash,
                )
                if not inserted:
                    # If another request holds the same key right now, the INSERT above waited
                    # for its commit, so the stored response is complete here.
                    old = await conn.fetchrow(
                        "SELECT request_hash, status_code, response FROM idempotency_keys "
                        "WHERE tenant_id = $1 AND key = $2", tenant_id, idempotency_key,
                    )
                    if old["request_hash"] != req_hash:
                        raise ApiError(422, "IDEMPOTENCY_KEY_REUSED",
                                       "this Idempotency-Key was already used with a different request")
                    return AcceptResult(old["status_code"], old["response"], [])

            prices = await load_price_table(conn)
            prepared = validate_and_price(items, prices, state.settings.max_segments, batch)
            total = sum(p.price_micros for p in prepared)

            await conn.executemany(
                """INSERT INTO messages (id, tenant_id, client_ref, to_number, sender, text,
                                         encoding, segments, price_micros, status)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'ACCEPTED')""",
                [(p.id, tenant_id, p.client_ref, p.to, p.sender, p.text, p.encoding, p.segments,
                  p.price_micros) for p in prepared],
            )
            await conn.executemany(
                "INSERT INTO message_events (message_id, kind, status) VALUES ($1, 'STATUS', 'ACCEPTED')",
                [(p.id,) for p in prepared],
            )
            await conn.executemany(
                """INSERT INTO ledger_entries (tenant_id, message_id, entry_type, amount_micros)
                   VALUES ($1, $2, 'CHARGE', $3)""",
                [(tenant_id, p.id, p.price_micros) for p in prepared],
            )
            debited = await conn.fetchval(
                """UPDATE tenants SET balance_micros = balance_micros - $2
                    WHERE id = $1 AND balance_micros >= $2 RETURNING balance_micros""",
                tenant_id, total,
            )
            if debited is None:
                raise ApiError(402, "INSUFFICIENT_BALANCE",
                               f"balance is too low for this request (cost {total} micros)")

            if batch:
                response = {"count": len(prepared), "total_price_micros": total,
                            "messages": [_view(p) for p in prepared]}
            else:
                response = _view(prepared[0])
            if idempotency_key:
                await conn.execute(
                    "UPDATE idempotency_keys SET status_code = 202, response = $3 "
                    "WHERE tenant_id = $1 AND key = $2", tenant_id, idempotency_key, response,
                )
    events = [make_event(p.id, tenant_id, "ACCEPTED") for p in prepared]
    await publish(state.redis, events)
    return AcceptResult(202, response, prepared)


async def dispatch_accepted(state: State, tenant_id, prepared: list[Prepared]) -> None:
    """After COMMIT: ACCEPTED -> QUEUED, then hand the ids to the Redis queue.

    If the process dies between the commit and here, the messages stay safe in
    PostgreSQL and the reconciler queues them later.
    """
    ids = [p.id for p in prepared]
    async with state.pool.acquire() as conn:
        async with conn.transaction():
            events = await mark_queued(conn, ids)
    await state.queue.enqueue([(str(tenant_id), str(i)) for i in ids])
    await publish(state.redis, events)
