# Mini SMS Gateway

A small SMS gateway for the Digital Cloud Communications take-home assignment: a send API, prepaid billing with
a ledger, a queue with a global carrier rate limit and per-tenant fairness, delivery receipts (DLR), tenant
webhooks, a mock carrier, an internal dashboard and a load script.

Python 3.12 (FastAPI, asyncpg) · PostgreSQL 16 · Redis 7 · React + TypeScript (Vite) · Docker Compose.

**Read [DESIGN.md](DESIGN.md) for the answers to the four design questions.**

## Run it

```bash
docker compose up --build        # starts everything with seed data
```

| What | URL |
|---|---|
| Dashboard | http://localhost:8080 |
| Gateway API (docs at `/docs`) | http://localhost:8000 |
| Mock carrier | http://localhost:9000 (`GET /stats`) |

Seed data (loaded by the `migrate` service, safe to run again): two tenants, their API keys, balances, and a price table.

| Tenant | API key (`X-API-Key`) | Opening balance |
|---|---|---|
| Acme Bank | `acme-demo-key-0001` | 50.00 |
| ShopFast | `shopfast-demo-key-0001` | 200.00 |

| Prefix | Country | Price per segment |
|---|---|---|
| +880 | Bangladesh | 0.0060 |
| +971 | United Arab Emirates | 0.0350 |
| +63 | Philippines | 0.0200 |

These keys and the `dev-*` secrets in `docker-compose.yml` are demo values. Override them with environment variables
(`ACME_API_KEY`, `SHOPFAST_API_KEY`, `DLR_SECRET`, `ADMIN_KEY`). Start fresh with `docker compose down -v`.

```bash
# send one message
curl -s -X POST localhost:8000/v1/messages -H "X-API-Key: acme-demo-key-0001" \
  -H "Idempotency-Key: order-1001" -H "Content-Type: application/json" \
  -d '{"to":"+8801711000001","from":"ACME","text":"Your code is 123456","client_ref":"otp-1001"}'

# read it back, with its event history (use the id from the answer)
curl -s localhost:8000/v1/messages/<id> -H "X-API-Key: acme-demo-key-0001"
```

### Tests (51 test cases)

```bash
docker compose --profile tools run --rm tests
```

They use the real PostgreSQL and Redis from the compose file (a separate `sms_test` database and Redis db 15). Only the carrier is faked.

| File | Covers |
|---|---|
| `test_encoding.py` | GSM-7 / UCS-2 detection, 160/153 and 70/67 boundaries, extension characters count 2, Bangla, emoji (surrogate pairs) |
| `test_api.py` | validation, price = segments × price, batch all-or-nothing, **idempotency** (same body, different body, 10 concurrent), **30 concurrent sends against a balance for 5**, DB refuses negative balance, **tenant isolation** |
| `test_dispatch.py` | **429/503/timeout retried, 400 not retried**, refund rules, **DLR before submit response + duplicate DLR**, **missing/wrong signature**, status never moves back, fairness, round-robin with 50 tenants, global limiter, recovery after losing Redis and after a dead worker, two workers racing, webhook outbox and signature |

### Load script

```bash
docker compose --profile tools run --rm loadtest                  # 10,000 ShopFast messages + 1 Acme OTP per second
docker compose --profile tools run --rm loadtest --messages 500   # quick smoke run
```

ShopFast needs about 60.00 of balance per 10,000 messages. If a run fails with `INSUFFICIENT_BALANCE`, reset with `docker compose down -v`.
Run it on a machine that does not go to sleep. A suspended Docker VM distorts the timings (`caffeinate -i` on macOS).

**Output of one real run** (the database was reset before the first of three runs on it, so balances and the ledger total below add up over those three runs; "spent this run" is this run only. Saved in `loadtest/last-run.txt`):

```text
====================================================================
LOAD TEST REPORT  (run 00bd07e0)
====================================================================
Messages: 10000 ShopFast + 278 Acme OTP
Total time until every message was final: 284.8 s
ShopFast: nothing left to submit after 271.9 s -> 36.4 messages/s accepted by the carrier (average)
Carrier submit calls (all outcomes, incl. retries and OTP): 10696 -> average 39.3 calls/s during the submit phase (limit 50/s)
Carrier 429 (over its 50/s limit): 8   503: 314   400: 112   timeouts: 104   re-submits de-duplicated: 104
DLRs sent: 11190   duplicates: 1024   early (before submit response): 493

OTP (Acme Bank) time from ACCEPTED to SUBMITTED while the campaign ran:
  samples 274   p50 413 ms   p95 619 ms   max 4437 ms   (never submitted: 4)

Final states:
  ShopFast  DELIVERED 8879  UNDELIVERED 705  EXPIRED 308  FAILED 108   (retried at least once: 403)
  Acme      DELIVERED 239  UNDELIVERED 25  EXPIRED 10  FAILED 4   (retried at least once: 12)

Billing check:
  Acme      PASS  balance 45.9920 = opening 50.0000 - ledger 4.0080  [ok]
            charged once each: yes   double charge/refund entries: 0   refunds only for FAILED (4): yes   spent this run: 1.6440
  ShopFast  PASS  balance 57.4880 = opening 200.0000 - ledger 142.5120  [ok]
            charged once each: yes   double charge/refund entries: 0   refunds only for FAILED (108): yes   spent this run: 59.3520
====================================================================
```

How to read it:

- The carrier answered 429 eight times (0.07% of calls). Those messages were retried like any 429. The gateway's own meter (worker log) shows **at most 43 submits in any 1 s window**, below the carrier's 50/s. The configured limit is 40/s. The 429s were counted by the mock carrier, which is a single Python process that is also sending 11,000 DLRs. I did not fully prove this cause. Treat it as the most likely explanation, not a fact.
- "never submitted: 4" are OTPs that got the carrier's permanent 400 (`INVALID_DESTINATION`). They were marked `FAILED` and refunded, as the rules say.
- `UNDELIVERED` and `EXPIRED` are not refunded (the carrier already charged us), so they stay in the ledger.
- The 104 timeouts were all re-submitted with the same `message_id` and the carrier de-duplicated all 104, so no message was sent twice.

## Architecture

```text
 client ──POST /v1/messages──▶ gateway API ──one transaction──▶ PostgreSQL
                                   │            (message + ACCEPTED event + CHARGE ledger row
                                   │             + debit + idempotency key)
                                   ▼ after COMMIT: ACCEPTED → QUEUED, push id
                                 Redis ── per-tenant lists + round-robin ring ── global 40/s limiter
                                   ▼
                              worker(s): dequeue → claim (lease) → limiter slot → POST carrier /submit
                                   │                                                   │
                       SUBMITTED / retry / FAILED+refund                    DLR later (0.5-10 s, signed)
                                   ▲                                                   ▼
        reconciler (re-queues what Redis lost) ◀── PostgreSQL ◀── POST /carrier/dlr (HMAC check, store, apply)
                                   │
                  webhook outbox row ──▶ webhook-worker ──▶ tenant URL (retries, signed)
                  status events ──▶ Redis pub/sub ──▶ SSE ──▶ dashboard
```

Status flow (forward only): `ACCEPTED → QUEUED → SUBMITTED → DELIVERED | UNDELIVERED | EXPIRED`, and `FAILED` (only from `ACCEPTED/QUEUED`: permanent carrier error or no attempts left, with refund). I kept the states from the brief.

### Data model (`gateway/migrations/001_init.sql`)

| Table | Purpose and the rule it enforces |
|---|---|
| `tenants` | `balance_micros` with `CHECK (>= 0)`, hashed API key, webhook url and secret. |
| `prices` | Price per segment by prefix, longest prefix wins. |
| `messages` | One row per message. `status_rank()` makes the forward-only guard a single `WHERE`. Lease and retry fields. Unique partial index on `carrier_id`. |
| `message_events` | Append-only history (`STATUS` changes and failed `ATTEMPT`s). |
| `ledger_entries` | `CHARGE` (positive) and `REFUND` (negative). `UNIQUE (message_id, entry_type)`. `balance = opening - SUM(amount)`. |
| `idempotency_keys` | Per tenant. Created in the same transaction as the message. |
| `dlr_receipts` | Every verified DLR is stored first, so an early DLR is not lost. Unique per (carrier id, status, timestamp). |
| `webhook_deliveries` | Transactional outbox. One row per message (`UNIQUE (message_id)`). |

Money is stored as `BIGINT` micros (1.000000 = 1,000,000), so there is no rounding.

### Where the transactions, constraints and locks are

- **Accept** (`service.py`): one transaction. The debit is `UPDATE tenants SET balance = balance - $total WHERE id = $1 AND balance >= $total`, the last statement, so the tenant row lock is held as briefly as possible. If it matches no row the request returns 402 and everything rolls back.
- **Idempotency**: `INSERT ... ON CONFLICT DO NOTHING` on `(tenant_id, key)`. A concurrent duplicate blocks on the unique index until the first commits, then replays the stored response.
- **Claim** (`worker.py`): one `UPDATE ... WHERE lease_until IS NULL OR expired RETURNING`. No explicit lock needed.
- **Status changes** (`transitions.py`): `UPDATE ... WHERE status_rank(status) < status_rank(new)`. The event, refund and webhook rows are in the same transaction.
- **Webhook claim**: `FOR UPDATE SKIP LOCKED`, so several senders can run.

## API

All tenant endpoints need `X-API-Key`. Errors look like `{"error": {"code", "message", "details"}}`.

| Method and path | Notes |
|---|---|
| `POST /v1/messages` | Body `{to, from, text, client_ref?}`. Returns **202** with `status: ACCEPTED`, encoding, segments and price. Optional `Idempotency-Key`. |
| `POST /v1/messages/batch` | `{messages: [...]}`, up to 1,000. All or nothing: any invalid item or too little balance rejects the whole batch. |
| `GET /v1/messages` | Filters `status`, `created_from`, `created_to`, `client_ref`, `limit`, `offset`. |
| `GET /v1/messages/{id}` | Includes the event history. Another tenant's id returns **404**. |
| `GET /v1/balance`, `GET /v1/ledger`, `GET /v1/stats` | Balance with ledger reconciliation, the ledger (paged by `after_id`), counts per status. |
| `GET/PUT /v1/webhook` | Set the callback URL. Callbacks carry `X-Signature: sha256=<HMAC of the body>` with the tenant's signing secret. |
| `POST /carrier/dlr` | Called by the carrier. `X-Signature` = hex HMAC-SHA256 of the raw body with `DLR_SECRET`. Wrong or missing: **401**, nothing changes. |
| `/admin/*` | Used by the dashboard (`X-Admin-Key`). |

Validation: the number must be E.164 (`+` and 7-15 digits, no leading 0); sender id is 1-11 letters/digits or a phone number; the text must be at most 10 segments; the destination must have a price. Bad input gets a 422 that names the field. The API never fixes input silently.

## Assumptions and trade-offs

- **Carrier de-duplicates on `message_id`.** The mock does, and my retry/crash recovery depends on it (see DESIGN.md, question 1). With `DEDUPE_BY_MESSAGE_ID=false` on the carrier you can see duplicates happen.
- **Our limit is 40/s, not 50/s.** A buffer of 20% for short event-loop pauses. Measured peak at the gateway: 43 in any 1 s.
- **Client timeout 3 s**, carrier "slow" answers take 5 s. Up to 6 attempts with exponential backoff and jitter (first retry after 0.5-1 s, doubling each time, capped at 20 s).
- **Redis for the queue, PostgreSQL for the truth.** A PostgreSQL-only queue (`SKIP LOCKED`) would be simpler, but I wanted the atomic round-robin and the shared sliding-window limiter, which are a few lines of Lua. The cost is a second system, and the reconciler that repairs the gap between the two.
- **Batch is all-or-nothing** and returns 402 if the whole batch is not affordable. Simple to reason about; a client with a partly valid batch has to resend.
- **Idempotency**: only successful (202) responses are stored. A 4xx is not stored, so a client can fix the request and reuse the key. Keys never expire (a real system would expire them).
- **`SUBMITTED` is recorded when the carrier answers**, so the "accept to SUBMITTED" numbers include the carrier's answer time.
- **Offset pagination**, fine for a demo, not for millions of rows.
- **Demo-level security**: API keys are stored as SHA-256 (fine for long random keys); the dashboard has no login, nginx adds the admin key (so the dashboard port must not be public); webhook URLs are not checked against private addresses (SSRF); message text, including OTP codes, is stored in plain text.
- **Lua scripts use dynamic key names**, so Redis Cluster is not supported as is.

## What is not done

- Bonus items other than the HMAC signature on webhooks: no second carrier or routing, no scheduled messages, no Prometheus (only the peak-rate log line), no templates.
- No frontend tests; I checked the dashboard by hand and with a headless browser screenshot.
- No per-tenant API rate limit, no key rotation, no idempotency-key expiry.

## What I would improve with more time

1. Weighted round-robin and a priority lane per tenant (see DESIGN.md, question 3).
2. Metrics: queue depth per tenant, submit rate, DLR delay, retry counts, as Prometheus gauges.
3. A second mock carrier with routing by prefix and failover.
4. Batch the claim and the writes to lift the per-process ceiling (DESIGN.md, question 4).
5. Fix the 429 noise at the source: give the mock carrier its own arrival timestamp, or run it with more workers, so a 429 always means a real overshoot.

## Layout

```text
docker-compose.yml     everything, one command
gateway/app/           API, service (billing), transitions, queue (Redis), worker, webhooks, migrate
gateway/migrations/    SQL migrations (applied by the migrate service)
gateway/tests/         51 automated test cases
carrier/               mock carrier
dashboard/             React + TypeScript (Vite), served by nginx
loadtest/              load.py and the output of the run above
DESIGN.md              the design note
```

## AI usage

This project was built with an AI coding assistant (Kiro). The full session transcript is provided separately
with the submission. I can explain and change any part of the code live.
