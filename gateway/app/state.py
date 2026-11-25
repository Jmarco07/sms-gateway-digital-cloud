"""Shared resources (database pool, Redis, queue) for the API and the workers."""
from dataclasses import dataclass

import asyncpg
import redis.asyncio as aioredis

from app.config import Settings
from app.db import create_pool
from app.queue import DispatchQueue


@dataclass
class State:
    settings: Settings
    pool: asyncpg.Pool
    redis: aioredis.Redis
    queue: DispatchQueue

    async def close(self) -> None:
        await self.redis.aclose()
        await self.pool.close()


async def build_state(settings: Settings, pool_size: int = 20) -> State:
    pool = await create_pool(settings.database_url, max_size=pool_size)
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    return State(settings=settings, pool=pool, redis=redis, queue=DispatchQueue(redis))
