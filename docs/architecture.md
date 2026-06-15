# Architecture

Rhombus follows a strict MVP architecture:

1. Application code opens a Postgres transaction.
2. Business tables are updated.
3. Rhombus enqueues an outbox event in the same transaction.
4. The transaction commits.
5. The worker claims eligible events.
6. The worker dispatches to Kafka.
7. On success, the row becomes DELIVERED.
8. On failure, the row is rescheduled or moved to DLQ.

## Core data flow

- `PENDING` → waiting to be claimed
- `PROCESSING` → leased by one worker
- `RETRY_WAIT` → scheduled for another attempt
- `DELIVERED` → successfully processed
- `DLQ` → exhausted or non-retryable failure

## Why this shape

This keeps the system simple enough to reason about and test:

- a single source of truth in Postgres
- explicit state transitions
- clear replay path from DLQ
- worker recovery through stale lease reset

## Ordering model

Ordering is per aggregate key.  
Events for the same aggregate should share the same ordering key so they are processed in sequence.

## Delivery model

Delivery is at-least-once.  
Consumers and downstream handlers must be idempotent.
