"""Read queries shared by the tenant API and the admin (dashboard) API."""
from datetime import datetime

import asyncpg

STATUSES = ("ACCEPTED", "QUEUED", "SUBMITTED", "DELIVERED", "UNDELIVERED", "EXPIRED", "FAILED")

_MESSAGE_COLUMNS = """m.id, m.tenant_id, m.client_ref, m.to_number AS "to", m.sender AS "from",
    m.text, m.encoding, m.segments, m.price_micros, m.status, m.carrier_id, m.attempts,
    m.last_error, m.created_at, m.updated_at, m.submitted_at, m.final_at"""


def _row(record: asyncpg.Record) -> dict:
    return dict(record)


async def list_messages(conn: asyncpg.Connection, *, tenant_id=None, status: str | None = None,
                        created_from: datetime | None = None, created_to: datetime | None = None,
                        client_ref: str | None = None, limit: int = 50, offset: int = 0) -> list[dict]:
    where, args = [], []

    def add(clause: str, value) -> None:
        args.append(value)
        where.append(clause.format(n=len(args)))

    if tenant_id is not None:
        add("m.tenant_id = ${n}", tenant_id)
    if status:
        add("m.status = ${n}", status)
    if created_from:
        add("m.created_at >= ${n}", created_from)
    if created_to:
        add("m.created_at < ${n}", created_to)
    if client_ref:
        add("m.client_ref = ${n}", client_ref)
    sql = (f"SELECT {_MESSAGE_COLUMNS}, t.name AS tenant_name FROM messages m "
           "JOIN tenants t ON t.id = m.tenant_id")
    if where:
        sql += " WHERE " + " AND ".join(where)
    args += [limit, offset]
    sql += f" ORDER BY m.created_at DESC, m.id DESC LIMIT ${len(args) - 1} OFFSET ${len(args)}"
    return [_row(r) for r in await conn.fetch(sql, *args)]


async def get_message(conn: asyncpg.Connection, message_id, tenant_id=None) -> dict | None:
    """tenant_id=None is for the admin API only. A tenant always passes its own id, so a
    message of another tenant is simply 'not found'."""
    sql = (f"SELECT {_MESSAGE_COLUMNS}, t.name AS tenant_name FROM messages m "
           "JOIN tenants t ON t.id = m.tenant_id WHERE m.id = $1")
    args = [message_id]
    if tenant_id is not None:
        sql += " AND m.tenant_id = $2"
        args.append(tenant_id)
    record = await conn.fetchrow(sql, *args)
    if not record:
        return None
    message = _row(record)
    events = await conn.fetch(
        "SELECT id, kind, status, detail, created_at FROM message_events "
        "WHERE message_id = $1 ORDER BY id", message_id)
    message["events"] = [_row(e) for e in events]
    return message


async def status_counts(conn: asyncpg.Connection, tenant_id=None) -> dict:
    """{tenant_id: {status: count}}"""
    sql = "SELECT tenant_id, status, count(*) AS n FROM messages"
    args = []
    if tenant_id is not None:
        sql += " WHERE tenant_id = $1"
        args.append(tenant_id)
    sql += " GROUP BY tenant_id, status"
    out: dict = {}
    for r in await conn.fetch(sql, *args):
        out.setdefault(r["tenant_id"], {s: 0 for s in STATUSES})[r["status"]] = r["n"]
    return out


async def tenant_money(conn: asyncpg.Connection, tenant_id=None) -> list[dict]:
    """Balance, opening balance and ledger total for the reconciliation check:
    balance == opening - ledger_total."""
    sql = """SELECT t.id, t.name, t.balance_micros, t.opening_balance_micros,
                    COALESCE((SELECT SUM(amount_micros) FROM ledger_entries l WHERE l.tenant_id = t.id), 0)
                        AS ledger_total_micros
               FROM tenants t"""
    args = []
    if tenant_id is not None:
        sql += " WHERE t.id = $1"
        args.append(tenant_id)
    sql += " ORDER BY t.name"
    rows = []
    for r in await conn.fetch(sql, *args):
        d = _row(r)
        d["balance_consistent"] = d["balance_micros"] == d["opening_balance_micros"] - d["ledger_total_micros"]
        rows.append(d)
    return rows
