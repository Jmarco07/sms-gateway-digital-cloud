"""Redis dispatch layer: per-tenant queues served round-robin, a delayed set for
retries, and a global sliding-window rate limiter.

Redis is NOT the source of truth. Every message is first saved in PostgreSQL; Redis
only decides the order in which saved messages are sent. If Redis is lost, the
reconciler (see worker.py) rebuilds the queue from PostgreSQL.

Keys (all prefixed with "sms:"):
    q:{tenant}   list   message ids waiting for one tenant
    ring         list   tenants that currently have waiting messages (round-robin order)
    ring_set     set    same tenants, for O(1) "is it already in the ring" checks
    inq          set    ids that are queued or delayed (used by the reconciler)
    delayed      zset   "tenant|message" scored by the time (ms) a retry is allowed
    rl           zset   timestamps of recent carrier submits (the rate limiter)

The Lua scripts take dynamic key names, so they are for a single Redis node. That is
a deliberate simplification (see DESIGN.md).
"""
from dataclasses import dataclass

import redis.asyncio as aioredis

P = "sms:"

# ARGV: tenant1, id1, tenant2, id2, ...
# Skips an id that is already queued (SADD returns 0), so enqueueing twice is safe.
_ENQUEUE = """
local n = 0
for i = 1, #ARGV, 2 do
    local tenant, id = ARGV[i], ARGV[i + 1]
    if redis.call('SADD', 'sms:inq', id) == 1 then
        redis.call('RPUSH', 'sms:q:' .. tenant, id)
        if redis.call('SADD', 'sms:ring_set', tenant) == 1 then
            redis.call('RPUSH', 'sms:ring', tenant)
        end
        n = n + 1
    end
end
return n
"""

# Round-robin: take the tenant at the head of the ring, take ONE message from it, and
# put the tenant at the back of the ring if it still has messages.
_DEQUEUE = """
while true do
    local tenant = redis.call('LPOP', 'sms:ring')
    if not tenant then return nil end
    local id = redis.call('LPOP', 'sms:q:' .. tenant)
    if redis.call('LLEN', 'sms:q:' .. tenant) > 0 then
        redis.call('RPUSH', 'sms:ring', tenant)
    else
        redis.call('SREM', 'sms:ring_set', tenant)
    end
    if id then
        redis.call('SREM', 'sms:inq', id)
        return {tenant, id}
    end
end
"""

# Move retries whose time has come back into their tenant queue. The id is still in
# `inq` (it was added when the retry was scheduled), so no SADD here.
_MOVE_DUE = """
local due = redis.call('ZRANGEBYSCORE', 'sms:delayed', '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, member in ipairs(due) do
    redis.call('ZREM', 'sms:delayed', member)
    local sep = string.find(member, '|', 1, true)
    local tenant = string.sub(member, 1, sep - 1)
    local id = string.sub(member, sep + 1)
    redis.call('RPUSH', 'sms:q:' .. tenant, id)
    if redis.call('SADD', 'sms:ring_set', tenant) == 1 then
        redis.call('RPUSH', 'sms:ring', tenant)
    end
end
return #due
"""

# Sliding-window log: allow at most ARGV[2] grants in any ARGV[1] ms window, using the
# Redis clock so that all workers agree on time. Returns {1, token} or {0, wait_ms}.
_ACQUIRE = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
if redis.call('ZCARD', KEYS[1]) < limit then
    local seq = redis.call('INCR', KEYS[1] .. ':seq')
    local token = string.format('%d-%d', now, seq)
    redis.call('ZADD', KEYS[1], now, token)
    return {1, token}
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local wait = tonumber(oldest[2]) + window - now
if wait < 1 then wait = 1 end
return {0, wait}
"""


@dataclass(frozen=True)
class QueueItem:
    tenant_id: str
    message_id: str


class DispatchQueue:
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis
        self._enqueue = redis.register_script(_ENQUEUE)
        self._dequeue = redis.register_script(_DEQUEUE)
        self._move_due = redis.register_script(_MOVE_DUE)
        self._acquire = redis.register_script(_ACQUIRE)

    # ---- queue ----------------------------------------------------------
    async def enqueue(self, items: list[tuple[str, str]]) -> int:
        """items = [(tenant_id, message_id), ...]. Returns how many were newly added."""
        added = 0
        for start in range(0, len(items), 500):
            args: list[str] = []
            for tenant_id, message_id in items[start:start + 500]:
                args += [str(tenant_id), str(message_id)]
            added += int(await self._enqueue(args=args))
        return added

    async def dequeue(self) -> QueueItem | None:
        res = await self._dequeue()
        if not res:
            return None
        return QueueItem(tenant_id=_s(res[0]), message_id=_s(res[1]))

    async def schedule_retry(self, tenant_id: str, message_id: str, delay_seconds: float) -> None:
        import time

        ready_ms = int((time.time() + delay_seconds) * 1000)
        pipe = self.redis.pipeline()
        pipe.sadd(P + "inq", str(message_id))
        pipe.zadd(P + "delayed", {f"{tenant_id}|{message_id}": ready_ms})
        await pipe.execute()

    async def move_due(self, limit: int = 500) -> int:
        import time

        return int(await self._move_due(args=[int(time.time() * 1000), limit]))

    async def known(self, message_ids: list[str]) -> set[str]:
        """Which of these ids are currently queued or delayed?"""
        if not message_ids:
            return set()
        flags = await self.redis.smismember(P + "inq", message_ids)
        return {mid for mid, flag in zip(message_ids, flags) if flag}

    async def has_work(self) -> bool:
        return bool(await self.redis.llen(P + "ring"))

    async def depth(self) -> int:
        n = await self.redis.scard(P + "inq")
        return int(n)

    # ---- rate limiter -----------------------------------------------------
    async def acquire_slot(self, limit: int, window_ms: int = 1000) -> tuple[bool, str | int]:
        """(True, token) if a submit is allowed now, else (False, ms_to_wait)."""
        ok, value = await self._acquire(keys=[P + "rl"], args=[window_ms, limit])
        return (True, _s(value)) if int(ok) == 1 else (False, int(value))

    async def release_slot(self, token: str) -> None:
        """Give a slot back when we took one but did not submit anything."""
        await self.redis.zrem(P + "rl", token)


def _s(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
