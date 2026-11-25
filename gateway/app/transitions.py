"""Every message status change goes through this module.

Rules enforced here, in SQL, so they hold no matter which process calls us:

* A status only moves forward. UPDATEs carry a guard on status_rank(), so a late or
  repeated event matches zero rows and does nothing.
* An event row, the ledger refund and the webhook outbox row are written in the SAME
  transaction as the status change. They exist if and only if the change happened.
* Each function returns the event to publish (or None when nothing changed). The
  caller publishes it AFTER the transaction commits.
"""
import asyncpg

from app.events import make_event

DLR_STATUS = {"DELIVRD": "DELIVERED", "UNDELIV": "UNDELIVERED", "EXPIRED": "EXPIRED"}
FINAL_STATUSES = ("DELIVERED", "UNDELIVERED", "EXPIRED", "FAILED")


async def add_event(conn: asyncpg.Connection, message_id, status: str, detail: dict | None = None,
                    kind: str = "STATUS") -> None:
    await conn.execute(
        "INSERT INTO message_events (message_id, kind, status, detail) VALUES ($1, $2, $3, $4)",
        message_id, kind, status, detail or {},
    )


async def _webhook_outbox(conn: asyncpg.Connection, row: asyncpg.Record) -> None:
    payload = {
        "message_id": str(row["id"]),
        "client_ref": row["client_ref"],
        "to": row["to_number"],
        "status": row["status"],
        "segments": row["segments"],
        "price_micros": row["price_micros"],
        "final_at": row["final_at"].isoformat() if row["final_at"] else None,
    }
    await conn.execute(
        """INSERT INTO webhook_deliveries (tenant_id, message_id, url, payload)
           SELECT t.id, $2, t.webhook_url, $3 FROM tenants t
            WHERE t.id = $1 AND t.webhook_url IS NOT NULL
           ON CONFLICT (message_id) DO NOTHING""",
        row["tenant_id"], row["id"], payload,
    )


async def mark_queued(conn: asyncpg.Connection, message_ids: list) -> list[dict]:
    rows = await conn.fetch(
        """UPDATE messages SET status = 'QUEUED', updated_at = now()
            WHERE id = ANY($1::uuid[]) AND status = 'ACCEPTED'
        RETURNING id, tenant_id""",
        message_ids,
    )
    if rows:
        await conn.executemany(
            "INSERT INTO message_events (message_id, kind, status) VALUES ($1, 'STATUS', 'QUEUED')",
            [(r["id"],) for r in rows],
        )
    return [make_event(r["id"], r["tenant_id"], "QUEUED") for r in rows]


async def mark_submitted(conn: asyncpg.Connection, message_id, carrier_id: str) -> dict | None:
    row = await conn.fetchrow(
        """UPDATE messages
              SET status = 'SUBMITTED', carrier_id = $2, submitted_at = now(), updated_at = now(),
                  lease_until = NULL, last_error = NULL
            WHERE id = $1 AND status_rank(status) < status_rank('SUBMITTED')
        RETURNING id, tenant_id""",
        message_id, carrier_id,
    )
    if not row:
        return None
    await add_event(conn, message_id, "SUBMITTED", {"carrier_id": carrier_id})
    return make_event(row["id"], row["tenant_id"], "SUBMITTED")


async def mark_failed(conn: asyncpg.Connection, message_id, reason: str) -> dict | None:
    """Permanent failure before the carrier accepted the message: FAILED + refund.

    Only ACCEPTED/QUEUED messages can fail. Once the carrier has accepted a message
    (SUBMITTED) we never refund, because the carrier has charged us.
    """
    row = await conn.fetchrow(
        """UPDATE messages
              SET status = 'FAILED', last_error = $2, final_at = now(), updated_at = now(),
                  lease_until = NULL
            WHERE id = $1 AND status IN ('ACCEPTED', 'QUEUED')
        RETURNING *""",
        message_id, reason,
    )
    if not row:
        return None
    refund_id = await conn.fetchval(
        """INSERT INTO ledger_entries (tenant_id, message_id, entry_type, amount_micros)
           VALUES ($1, $2, 'REFUND', $3)
           ON CONFLICT (message_id, entry_type) DO NOTHING
           RETURNING id""",
        row["tenant_id"], row["id"], -row["price_micros"],
    )
    if refund_id:  # credit the balance only when the refund row is new
        await conn.execute(
            "UPDATE tenants SET balance_micros = balance_micros + $2 WHERE id = $1",
            row["tenant_id"], row["price_micros"],
        )
    await add_event(conn, message_id, "FAILED", {"reason": reason, "refunded": bool(refund_id)})
    await _webhook_outbox(conn, row)
    return make_event(row["id"], row["tenant_id"], "FAILED")


async def apply_dlr(conn: asyncpg.Connection, carrier_id: str, new_status: str,
                    carrier_timestamp: str) -> dict | None:
    """Move a SUBMITTED message to a final state. No refund: the carrier already charged us."""
    row = await conn.fetchrow(
        """UPDATE messages
              SET status = $2, final_at = now(), updated_at = now()
            WHERE carrier_id = $1 AND status_rank(status) < status_rank($2)
        RETURNING *""",
        carrier_id, new_status,
    )
    if not row:
        return None
    await add_event(conn, row["id"], new_status, {"carrier_id": carrier_id,
                                                  "carrier_timestamp": carrier_timestamp})
    await _webhook_outbox(conn, row)
    return make_event(row["id"], row["tenant_id"], new_status)


async def apply_stored_receipts(conn: asyncpg.Connection, carrier_id: str) -> list[dict]:
    """Apply every stored DLR for this carrier id, oldest first. Idempotent."""
    events = []
    receipts = await conn.fetch(
        "SELECT status, carrier_timestamp FROM dlr_receipts WHERE carrier_id = $1 ORDER BY id",
        carrier_id,
    )
    for receipt in receipts:
        ev = await apply_dlr(conn, carrier_id, DLR_STATUS[receipt["status"]],
                             receipt["carrier_timestamp"])
        if ev:
            events.append(ev)
    return events
