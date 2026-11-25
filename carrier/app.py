"""Mock mobile carrier. A test tool: it is deliberately small and unreliable.

Contract:
    POST /submit  { message_id, to, from, text, encoding }  ->  200 { carrier_id }
Delivery receipts are pushed to  POST {GATEWAY_URL}/carrier/dlr  with an
X-Signature header (hex HMAC-SHA256 of the raw body, key = DLR_SECRET).

Every behaviour below is configurable with an environment variable.
"""
import asyncio
import hashlib
import hmac
import json
import os
import random
import time
import uuid
from collections import Counter, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field


def _f(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


class Config:
    gateway_url = os.environ.get("GATEWAY_URL", "http://localhost:8000").rstrip("/")
    dlr_secret = os.environ.get("DLR_SECRET", "dev-dlr-secret")
    latency_min = _f("LATENCY_MIN_MS", 20) / 1000
    latency_max = _f("LATENCY_MAX_MS", 300) / 1000
    rate_limit = int(_f("RATE_LIMIT_PER_SEC", 50))
    p_503 = _f("P_503", 0.03)
    p_400 = _f("P_400", 0.01)
    p_timeout = _f("P_TIMEOUT", 0.01)
    timeout_seconds = _f("TIMEOUT_SECONDS", 5)
    dlr_min = _f("DLR_MIN_SECONDS", 0.5)
    dlr_max = _f("DLR_MAX_SECONDS", 10)
    p_delivrd = _f("P_DELIVRD", 0.90)
    p_undeliv = _f("P_UNDELIV", 0.07)           # the rest is EXPIRED
    p_dlr_duplicate = _f("P_DLR_DUPLICATE", 0.10)
    p_dlr_early = _f("P_DLR_EARLY", 0.05)       # DLR arrives before the submit response
    # Many real carriers de-duplicate on a client reference. We do the same on
    # message_id. Set DEDUPE_BY_MESSAGE_ID=false to see what a carrier without it does.
    dedupe = os.environ.get("DEDUPE_BY_MESSAGE_ID", "true").lower() == "true"


class SubmitIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    message_id: str
    to: str
    from_: str = Field(alias="from")
    text: str
    encoding: Literal["GSM7", "UCS2"]


admitted: deque[float] = deque()      # times of submits that passed the rate limit
seen: dict[str, str] = {}             # message_id -> carrier_id
stats: Counter = Counter()
http: httpx.AsyncClient | None = None
tasks: set[asyncio.Task] = set()


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=5.0, limits=httpx.Limits(max_connections=200))
    yield
    await http.aclose()


app = FastAPI(title="Mock carrier", lifespan=lifespan)


def spawn(coro) -> None:
    task = asyncio.create_task(coro)
    tasks.add(task)
    task.add_done_callback(tasks.discard)


def pick_final_status() -> str:
    r = random.random()
    if r < Config.p_delivrd:
        return "DELIVRD"
    if r < Config.p_delivrd + Config.p_undeliv:
        return "UNDELIV"
    return "EXPIRED"


async def post_dlr(body: bytes) -> None:
    signature = hmac.new(Config.dlr_secret.encode(), body, hashlib.sha256).hexdigest()
    for _ in range(5):  # the carrier retries a DLR if the gateway is down
        try:
            resp = await http.post(
                f"{Config.gateway_url}/carrier/dlr",
                content=body,
                headers={"X-Signature": signature, "Content-Type": "application/json"},
            )
            if resp.status_code < 300:
                stats["dlr_sent"] += 1
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1)
    stats["dlr_failed"] += 1


async def deliver_receipt(carrier_id: str, delay: float) -> None:
    await asyncio.sleep(delay)
    body = json.dumps(
        {
            "carrier_id": carrier_id,
            "status": pick_final_status(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        separators=(",", ":"),
    ).encode()
    await post_dlr(body)
    if random.random() < Config.p_dlr_duplicate:
        await asyncio.sleep(random.uniform(0.1, 3.0))
        stats["dlr_duplicates"] += 1
        await post_dlr(body)  # identical bytes, identical signature


@app.post("/submit")
async def submit(body: SubmitIn):
    now = time.monotonic()
    while admitted and now - admitted[0] >= 1.0:
        admitted.popleft()
    if len(admitted) >= Config.rate_limit:
        stats["429"] += 1
        return JSONResponse({"error": "RATE_LIMITED"}, status_code=429, headers={"Retry-After": "1"})
    admitted.append(now)
    stats["submits"] += 1

    latency = random.uniform(Config.latency_min, Config.latency_max)

    if Config.dedupe and body.message_id in seen:
        stats["duplicate_submits"] += 1
        await asyncio.sleep(latency)
        return {"carrier_id": seen[body.message_id]}

    r = random.random()
    if r < Config.p_400:
        stats["400"] += 1
        await asyncio.sleep(latency)
        return JSONResponse({"error": "INVALID_DESTINATION"}, status_code=400)
    r -= Config.p_400
    if r < Config.p_503:
        stats["503"] += 1
        await asyncio.sleep(latency)
        return JSONResponse({"error": "TEMPORARY_FAILURE"}, status_code=503)
    r -= Config.p_503
    times_out = r < Config.p_timeout

    carrier_id = "c-" + uuid.uuid4().hex[:12]
    seen[body.message_id] = carrier_id
    stats["accepted"] += 1

    if random.random() < Config.p_dlr_early:
        # The DLR goes out first; the submit response is held back until after it.
        early_delay = random.uniform(0.01, 0.1)
        latency = max(latency, early_delay + 0.15)
        stats["dlr_early"] += 1
        spawn(deliver_receipt(carrier_id, early_delay))
    else:
        spawn(deliver_receipt(carrier_id, random.uniform(Config.dlr_min, Config.dlr_max)))

    if times_out:
        # Accepted and will be delivered, but the answer is too slow for the caller.
        stats["timeouts"] += 1
        await asyncio.sleep(Config.timeout_seconds)
    else:
        await asyncio.sleep(latency)
    return {"carrier_id": carrier_id}


@app.get("/stats")
async def get_stats():
    return dict(stats)


@app.post("/stats/reset")
async def reset_stats():
    stats.clear()
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True}
