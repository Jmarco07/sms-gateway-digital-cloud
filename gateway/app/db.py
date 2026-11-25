"""asyncpg pool helpers."""
import json

import asyncpg


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Read and write json/jsonb as Python objects.
    for typ in ("json", "jsonb"):
        await conn.set_type_codec(
            typ, encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )


async def create_pool(database_url: str, min_size: int = 2, max_size: int = 20) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        database_url, min_size=min_size, max_size=max_size, init=_init_connection
    )
