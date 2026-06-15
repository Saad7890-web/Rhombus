## `docs/guarantees.md`

```md
# Guarantees

Rhombus guarantees:

- atomic database write + event persistence
- at-least-once delivery
- per-aggregate ordering
- crash recovery
- replayability
- eventual downstream consistency

Rhombus does not guarantee:

- global distributed transactions
- true exactly-once semantics across every system
- atomicity across multiple databases

The practical guarantee is effectively-once processing when downstream consumers and handlers are idempotent.
```
