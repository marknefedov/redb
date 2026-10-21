# Bounded group commit

Group commit lets independent optimistic transactions share one durable publication.
It is an opt-in, in-process facility for builds with the standard library. It is disabled
by default, so databases continue to publish each transaction separately unless configured.

```rust
use redb::{Database, GroupCommitConfig};

let config = GroupCommitConfig::default()
    .with_max_batch_transactions(16)
    .with_max_batch_bytes(1024 * 1024);
let mut builder = Database::builder();
builder.set_group_commit(config);
let database = builder.create("data.redb")?;
# Ok::<(), redb::Error>(())
```

Use `begin_concurrent_write()` and `commit()` as usual. Only transactions using
`Durability::Immediate` participate. Ordinary write transactions and non-durable
optimistic transactions continue through the existing commit path.

## Scheduling and bounds

The committing threads run the coordinator themselves; there is no background worker.
Key comparators and value codecs for a queued transaction can execute on another
committing thread, rather than the thread that prepared that transaction.
A leader takes a bounded batch of requests that are already waiting. It does not wait
for more requests to fill the batch. A lone writer therefore pays no collection delay.
Requests that arrive while a batch is executing can share the next publication.

Configuration bounds both transaction count and accounted bytes for the waiting queue
and for each batch. `with_max_queue_transactions()` and `with_max_queue_bytes()` set
the waiting limits. A full queue returns `ConcurrentWriteError::QueueFull` without
applying that request. Applications can reject work or retry from a fresh transaction
with their own bounded backoff; repeatedly spinning on overload can worsen contention.

The byte accounting includes prepared keys, original and staged values, schemas, and
structural allowances. It does not bound exact process memory, allocator overhead, the
page cache, or pages retained by a snapshot. Transaction preparation still uses memory
proportional to the application's reads and writes.

A request larger than either the batch or queue byte limit uses the ordinary optimistic commit path.
Its work remains proportional to that transaction, and it does not fit within the batch
budget. FIFO scheduling applies to admitted queued requests; ordinary writes and these
oversized requests also compete for the existing writer lock.

Each leader runs one batch and hands off to the next queued caller. A caller does not
have to drain an ongoing stream of other requests before its own commit returns.
These scheduling bounds do not impose a wall-clock deadline on storage synchronization.

## Isolation, publication, and errors

All transactions are validated against the current committed database before any
buffered changes are replayed. Later transactions are also checked against writes from
earlier accepted members of the batch. Point reads, missing keys, written keys, table-wide
reads, and handled schema errors retain their usual dependencies. Intersections are
checked with the key type's custom comparator. Conservative conflicts require a fresh
transaction, just as for an ungrouped optimistic commit.

Accepted transactions are replayed in order into one physical write transaction. Readers
observe the state before the group or after the group; no intermediate root is published.
Successful callers are acknowledged only after the shared durable commit completes.
Recovery therefore preserves or rolls back the group atomically. Each logical transaction
still retains atomicity and serializable isolation.

An individual validation conflict returns `ConcurrentWriteError::Conflict`. A failure
affecting the shared operation is returned as `BatchAborted`, retaining the original error
in a shared value. A commit I/O error may already have published durable data, so it must
not be retried as a conflict. In builds that unwind on panic, a panicking leader wakes
affected callers with `BatchPanicked` before resuming its panic; those callers must treat
the result as uncertain and inspect or reopen the database as appropriate. Builds using
`panic = "abort"` terminate the process. External side effects belong after successful commit.

The validation-history cache records a bounded union of accepted writes for a grouped
publication. Rejected requests never contribute keys. Oversized footprints clear the
cache, and missing or evicted history falls back to exact preimages; missing cache history
never proves that a key is unchanged.

## Maintenance and observations

The existing relocation, reclamation, and resize budgets apply once per physical write
transaction. Grouping does not multiply a batch's fixed maintenance allowance by its number
of logical transactions or drain accumulated debt in one pass. Consequently, convergence
may take more logical commits when each durable publication serves many transactions.
Long-lived readers and savepoints continue to retain their original pages.

`Database::group_commit_stats()` exposes current waiting and active counts and accounted
bytes, queue high-water marks, selected batch peaks, successful physical batches, and
successful logical transactions. A physical batch can include additional synchronization
for existing cleanup work; batch count and backend synchronization count are different.

Commit-phase metrics are disabled by default. Enable them explicitly with
`Database::set_commit_metrics_enabled(true)` and read `Database::commit_metrics()`.
The measurements separate waiting, validation, replay, physical commit, backend writes,
and synchronization. Phase times can overlap or nest; summing them does not give elapsed
wall time. Snapshots contain cumulative counters and maxima, rather than a latency
histogram. Use benchmark samples for p99, p99.9, and maximum logical transaction latency.

See [performance measurements](performance.md) for the repeatable comparison of classic,
ungrouped optimistic, and grouped optimistic transactions. Throughput measurements run
with metrics disabled; a separate diagnostic pass measures commit phases.
