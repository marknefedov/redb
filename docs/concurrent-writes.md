# Concurrent write transactions

`Database::begin_concurrent_write()` starts an optimistic transaction from a pinned
database snapshot. Multiple threads can prepare independent transactions at the same
time. Reads see that snapshot plus the transaction's own staged insertions and removals.
Preparation does not acquire the database's writer lock or modify storage.

Commit acquires the existing writer lock, validates every read dependency, applies the
buffered changes, and publishes one atomic commit. Physical B-tree updates and publication
remain serialized. This API allows application work and transaction preparation to overlap;
it does not promise parallel disk writes or a fixed commit-latency bound.

The ordinary `begin_write()` API keeps its existing blocking behavior. Both APIs can be
used on the same database, and optimistic validation includes ordinary writers' commits.

## Basic use and conflicts

```rust
use redb::{ConcurrentWriteError, Database, TableDefinition};

const COUNTERS: TableDefinition<u64, u64> = TableDefinition::new("counters");

fn increment(database: &Database, key: u64) -> Result<u64, ConcurrentWriteError> {
    let mut remaining_attempts = 8;
    loop {
        remaining_attempts -= 1;
        let mut transaction = database.begin_concurrent_write()?;
        let next = {
            let mut table = transaction.open_table(COUNTERS)?;
            let current = table.get(key)?.map_or(0, |value| value.value());
            let next = current.checked_add(1).expect("counter overflow");
            table.insert(key, next)?;
            next
        };
        match transaction.commit() {
            Ok(()) => return Ok(next),
            Err(ConcurrentWriteError::Conflict { .. }) if remaining_attempts > 0 => {}
            Err(error) => return Err(error),
        }
    }
}
```

Each retry starts a fresh snapshot and reruns the transaction's reads and decisions.
Only retry `Conflict` automatically. External effects, such as sending a message, should
happen after a successful commit; they must not be repeated inside the retry body. The
attempt limit is application policy and keeps contention from causing an unbounded retry.

Point reads, including reads of missing keys, and keys being written are validated against
their original values. Transactions changing different keys in an existing table can both
commit without retrying. Conflicting changes return `ConcurrentWriteError::Conflict { table }`
without committing any of the losing transaction's changes. Validation checks every table
before replaying any buffered change, preventing partial replay after a conflict.

Calling `len()`, `is_empty()`, `iter()`, or `range()` establishes a whole-table dependency.
Any concurrent change to that table then conflicts, including changes outside the selected
range. This conservative rule detects phantom insertions and preserves serializable
transactions. Table removal, incompatible schema changes, and competing creation of a
missing table also conflict. Initialize shared tables once before launching workers if
they should immediately write disjoint keys without a table-creation conflict.

A handled table-type or schema error is still a catalog observation. If another writer
changes that schema before commit, the optimistic transaction conflicts even when the
failed table open did not stage a change.

Values are compared by their serialized bytes. A key's custom `Key::compare` ordering is
used throughout the overlay, ranges, and replay. As with ordinary tables, the key and value
implementations must honor the serialization and ordering contracts of their traits.

## Ownership, durability, and scope

A concurrent transaction borrows its `Database`, keeping the database alive until the
transaction ends. A table handle borrows the transaction mutably: close that handle before
opening another table or committing. Reopening a table retains its changes and dependencies.
`ConcurrentValue::value()` borrows an owned value, so returned values remain usable after
the transaction ends. Ranges are owned, double-ended iterators over the staged view as it
existed when the range was created.

`abort()` and dropping a transaction discard its private buffers without performing storage
writes. `Durability::Immediate` is the default. `set_durability()` follows the ordinary write
transaction's restrictions, including refusal of non-durable commits when sharing the file
with other processes.

`ConcurrentWriteError::Commit` has the same contract as `WriteTransaction::commit`: after
an I/O failure, changes may already be durable, and the database may require reopening for
recovery. It is not a retryable conflict. Storage, table, transaction-start, and durability
errors are reported separately.

This API provides concurrent preparation in one process. It does not change the file-lock
protocol or enable concurrent physical writers across processes. Existing multi-process
concurrency modes keep their documented behavior. Normal key-value tables are supported;
multimap operations, renaming and deleting tables, and savepoint operations continue to use
the ordinary `WriteTransaction` API.

## Work and memory

The overlay retains each touched key's original value and its final staged change. Memory
usage therefore grows with the point reads and writes in the transaction. A range is
materialized in memory, proportional to the selected entries. Commit validation and replay
grow with the transaction's dependencies and changes. Keep transaction batches bounded when
predictable memory use and latency matter. The storage engine's bounded reclamation and
hysteresis policies remain in effect; this API does not run a separate compaction pass.

An optional in-memory validation history holds at most 64 optimistic commits and 64 KiB of
key footprints and their headers. It skips redundant point reads only when the complete
intervening history proves that none of the transaction's keys changed. Missing or evicted
history, oversized footprints, and ordinary writers' commits fall back to comparing actual
values. The ordinary write path does not maintain this cache.
