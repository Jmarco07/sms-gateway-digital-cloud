"""Apply SQL migrations, then load idempotent seed data.

Run with:  python -m app.migrate
"""
import asyncio
import hashlib
import os
import sys
from pathlib import Path

import asyncpg

from app.config import get_settings

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
LOCK_ID = 727001  # arbitrary advisory-lock id so two migrators never run at once

# Demo credentials. They are seed data for local development only.
DEMO_TENANTS = [
    # (name, env var for the key, default key, opening balance in micros)
    ("Acme Bank", "ACME_API_KEY", "acme-demo-key-0001", 50_000_000),        # 50.00
    ("ShopFast", "SHOPFAST_API_KEY", "shopfast-demo-key-0001", 200_000_000),  # 200.00
]

# prefix, country, price per segment in micros
PRICES = [
    ("880", "Bangladesh", 6_000),       # 0.0060
    ("971", "United Arab Emirates", 35_000),   # 0.0350
    ("63", "Philippines", 20_000),      # 0.0200
]


def hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def apply_migrations(conn: asyncpg.Connection) -> list[str]:
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations (
               version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"""
    )
    applied = {r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")}
    done = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if path.name in applied:
            continue
        async with conn.transaction():
            await conn.execute(path.read_text())
            await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", path.name)
        done.append(path.name)
    return done


async def seed(conn: asyncpg.Connection) -> None:
    for prefix, country, price in PRICES:
        await conn.execute(
            "INSERT INTO prices (prefix, country, price_micros) VALUES ($1, $2, $3) "
            "ON CONFLICT (prefix) DO NOTHING",
            prefix, country, price,
        )
    for name, env_var, default_key, balance in DEMO_TENANTS:
        key = os.environ.get(env_var, default_key)
        await conn.execute(
            "INSERT INTO tenants (name, api_key_hash, opening_balance_micros, balance_micros) "
            "VALUES ($1, $2, $3, $3) ON CONFLICT (name) DO NOTHING",
            name, hash_api_key(key), balance,
        )


async def main() -> None:
    settings = get_settings()
    last_error: Exception | None = None
    for _ in range(30):  # wait for Postgres to accept connections
        try:
            conn = await asyncpg.connect(settings.database_url)
            break
        except (OSError, asyncpg.PostgresError) as exc:
            last_error = exc
            await asyncio.sleep(1)
    else:
        print(f"cannot connect to database: {last_error}", file=sys.stderr)
        raise SystemExit(1)
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", LOCK_ID)
        done = await apply_migrations(conn)
        await seed(conn)
        await conn.execute("SELECT pg_advisory_unlock($1)", LOCK_ID)
        print(f"migrations applied: {done or 'none (up to date)'}; seed data loaded")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
