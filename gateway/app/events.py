"""Live status events for the dashboard (Redis pub/sub -> Server-Sent Events).

These are notifications only. The truth is in PostgreSQL, so losing one is harmless:
the dashboard refetches on reconnect.
"""
import json
from datetime import datetime, timezone

import redis.asyncio as aioredis

CHANNEL = "sms:events"


def make_event(message_id, tenant_id, status: str) -> dict:
    return {
        "message_id": str(message_id),
        "tenant_id": str(tenant_id),
        "status": status,
        "at": datetime.now(timezone.utc).isoformat(),
    }


async def publish(redis: aioredis.Redis, events: list[dict]) -> None:
    if not events:
        return
    try:
        pipe = redis.pipeline(transaction=False)
        for event in events:
            pipe.publish(CHANNEL, json.dumps(event))
        await pipe.execute()
    except Exception:  # never let a dashboard notification break message processing
        pass
