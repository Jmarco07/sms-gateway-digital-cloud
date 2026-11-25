# Design note

PostgreSQL is the source of truth. Redis only decides the order in which saved messages are sent, and
it can be wiped without losing a message. This one rule explains most of the answers below.

## 1. A worker submits, the carrier accepts, the worker crashes before saving the result

The message is still `QUEUED` in PostgreSQL and the worker's lease (30 s) is still on it.

1. The lease expires. The reconciler (runs every 5 s in every worker) finds a `QUEUED` message with no
   live lease that is not in Redis, and queues it again.
2. Another worker submits it again with the **same `message_id`**.
3. The carrier answers with the **same `carrier_id`** (it de-duplicates on `message_id`), so the
   customer gets one SMS. The DLR of the first accept carries that same `carrier_id`, so it matches.

**Can the customer get the SMS twice? Only if the carrier does not de-duplicate on `message_id`.**
The mock carrier does (`DEDUPE_BY_MESSAGE_ID=true`; set it to `false` to see the failure). A real carrier
may keep that memory for a limited time only. Nothing on our side can fully close this gap: it is the
classic "two generals" problem between us and the carrier. The mitigations are the stable `message_id`
and a short lease, and a rule I did not build: never auto-resubmit a message older than its useful life
(an OTP older than a few minutes).

**Timeout is the same case.** Our client timeout (3 s) is shorter than the carrier's slow answer (5 s).
We treat a timeout like a 503: retry with backoff, same `message_id`. Remaining risks:

- A carrier without de-duplication would send twice.
- If every attempt times out (probability about 0.01^6 per message) we mark the message `FAILED` and
  refund, although the carrier may have delivered it. The tenant gets one free SMS, and the late DLR
  waits unmatched in `dlr_receipts`.

## 2. Where is it "at least once", and where "exactly once"?

| Step | Guarantee | Why |
|---|---|---|
| Client request → message | exactly once | `Idempotency-Key` row is inserted in the same transaction as the message and charge. A concurrent duplicate waits on the unique key and then replays the stored response. |
| Charge | exactly once | `UNIQUE (message_id, entry_type)`, written in the same transaction as the message, and the debit is one conditional `UPDATE ... WHERE balance >= cost`. |
| Refund | exactly once | Same unique key. It is written only by the `UPDATE` that moves `ACCEPTED/QUEUED → FAILED`, so it cannot happen after the carrier accepted. |
| Redis queue entry | at least once | `SADD inq` makes enqueue idempotent, but the reconciler may add an id twice. The claim (below) absorbs it. |
| Claim | exactly one winner | One `UPDATE ... WHERE lease_until IS NULL OR expired RETURNING`. |
| Submit to carrier | **at least once** | Retries, timeouts and crash recovery. Effectively once only through the carrier's `message_id` de-duplication. |
| DLR from carrier | at least once (duplicates, early) | Stored first (`UNIQUE (carrier_id, status, timestamp)`), then applied. |
| Status change | exactly once | `UPDATE ... WHERE status_rank(status) < status_rank(new)`. The event row, refund and webhook outbox row are in the same transaction, so a repeated DLR matches zero rows and writes nothing. |
| Tenant webhook | **at least once** | Retried until 2xx. The tenant de-duplicates on `message_id`. |

**How I check it.** The constraints above are enforced by the database. The 51 automated tests cover idempotency
(also 10 concurrent identical requests), 30 concurrent sends against a balance for 5, DLR before the
submit response, duplicate and conflicting DLRs, bad signatures, retries and refunds, two workers racing
for one message, and recovery after losing Redis. I also broke the status guard on purpose and a test failed.
The load script checks the ledger after every run: `balance == opening - sum(ledger)`, one charge per
message, refunds only for `FAILED` messages (see `loadtest/last-run.txt`).

A DLR can arrive before the carrier id is saved. The handler stores the receipt and commits, then tries to
apply it. The worker commits `SUBMITTED + carrier_id`, then reads the stored receipts. These are two separate
transactions on purpose: whichever commits second is guaranteed to see the other's row, so the DLR is never lost.

## 3. Fairness

Each tenant has its own Redis list. A ring of tenants that have waiting messages is served round-robin: one
Lua call takes the tenant at the head, pops **one** message, and puts the tenant at the back if it still has
more. A worker holds one message at a time and takes a slot from the global limiter (a sliding-window log in
Redis, shared by all workers) as the last step before the HTTP call.

An Acme OTP that arrives during a 10,000-message campaign waits at most one turn. In the load run
its p50 was 413 ms and p95 619 ms from `ACCEPTED` to `SUBMITTED`. That includes the carrier's 20-300 ms
answer time, because `SUBMITTED` is recorded when the carrier replies. No OTP that succeeded on its first
attempt took longer than 2 s (checked in the database). The slow ones (up to 4.4 s) are the 12 OTPs that
needed a retry after a carrier 503 or a timeout.

**With 50 tenants:** every backlogged tenant gets 1/50 of the carrier capacity (0.8 messages/s at 40/s). A
tenant that sends one message waits at most one round, about 50 slots = 1.25 s. Limits of this design:
equal shares regardless of plan, and no priority inside a tenant: an OTP sent by ShopFast would queue behind
ShopFast's own campaign. Next step would be weighted round-robin and a priority lane per tenant.

## 4. What breaks first at 1,000 messages per second?

The carrier limit (50/s) breaks first by definition: we would need many carriers and routing. Inside our
system, in this order:

1. **The dispatch loop.** One loop per process does about four round trips per message (dequeue, claim,
   limiter, and the DB writes after). It tops out at a few hundred messages/s. Change: claim and dequeue in
   batches, one loop per tenant shard, more worker processes (already supported).
2. **The tenant balance row.** Every accept and refund locks the same row, so one big tenant serialises. Change:
   reserve credit in chunks (allocate e.g. 1,000 messages worth of balance per worker, reconcile to the
   ledger), or split a balance over several rows.
3. **PostgreSQL write volume.** Each message costs about 5 `UPDATE`s and 7 inserts, and DLRs add 1,000 more
   writes/s. Change: batch DLR inserts, partition `messages`, `message_events` and `ledger_entries` by
   time, and use PgBouncer.
4. **Redis.** One node, with Lua scripts and one hot limiter key, handles this, but it is a single point of
   failure. Change: one limiter key per carrier, Redis Sentinel or a managed replica.
5. **Dashboard counters.** `GROUP BY status` over all messages and SSE for every event will not scale.
   Change: maintained counters and sampled or aggregated events.
