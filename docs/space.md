# Space usage and predictable maintenance

Upstream redb's space overhead comes from partially filled B+tree nodes, power-of-two buddy
allocation, retained copy-on-write versions, and a file-growth reserve. A deletion can leave
holes throughout the file even when many pages are free. RocksDB stores densely packed sorted
runs and can compress their contents, at the cost of compaction work. This fork targets the
allocation and reuse overhead while retaining copy-on-write transactions; it does not add
compression, so compressible data can still be much smaller in RocksDB.

New databases use this fork's v4 format. The default B+tree node target remains 4096 bytes;
allocation uses 256-byte units. Ordinary nodes occupy exact contiguous extents, including
extents crossing buddy boundaries. Extents up to 32767 allocation units (just under 8 MiB)
use exact lengths; larger allocations keep the buddy encoding. The database
header occupies at least 512 bytes, so the smaller allocation unit cannot overlap it.

The allocator maintains coalesced free extents in an address-ordered AVL tree, with each
subtree recording its largest extent. A second augmented tree indexes regions. Finding the
lowest fitting address takes logarithmic work even for large extents in fragmented files;
freeing an extent considers its two neighbors. The existing buddy bitmap remains the durable allocator representation and
supports large allocations, recovery, rollback, and savepoints. Run indices are reconstructed
from it on open or allocator reload. This trades additional allocator RAM for tighter disk use.

Durability retains the existing [underlying-media assumptions](design.md#assumptions-about-underlying-media),
including powersafe overwrite.

## Work during normal writes

Normal writes do not invoke `Database::compact()`, scan the database for relocation candidates,
or launch a background compaction thread. They reuse lower offsets and consider a sibling on
each already visited branch of the mutation path. All tables in one write transaction share a
budget of 256 relocation probes and 1 MiB of copied bytes. A candidate is at most 8192 bytes,
so probe reads are bounded by 2 MiB even if none can move. Larger values are excluded before
reading the candidate.
Moves are copy-on-write and use the normal pending-free machinery; readers and savepoints
keep their original pages until their snapshots are released. There is no extra commit or
fsync for a move, and relocation never grows the file.

Hysteresis separates the thresholds:

* Leaves and branches split when full and merge below one third of the node target.
  Dirty leaves are rebuilt after deletion when content falls below half their allocation,
  releasing unused extent units without requiring a tree merge.
* A region starts evacuating nodes at 25% free and stops below 12.5% free.
* File resizing keeps a reserve of 1/64 of capacity, clamped to 64 KiB..1 MiB. Shrinking starts
  when twice that reserve is free at the tail and preserves one reserve. Each normal shrink
  resizes at most 1 MiB of allocator metadata. Growth adds at most 1 MiB for ordinary nodes;
  a large user allocation can require more, proportional to that allocation.
* Durable garbage reclamation gets twice the pages retired by current writes plus 256 pages
  per commit. It finishes its final pending-free record, allowing at most 199 extra pages.
  Empty records also consume budget. Dropping an old reader therefore leaves a backlog for
  later commits instead of freeing its entire durable history at once.

These bounds cover additional maintenance, not a hard wall-clock deadline for a transaction.
Tree changes still take work proportional to the modified paths and input size; the storage
device controls fsync latency. Large user transactions, non-durable-to-durable transitions,
savepoint restoration, open/recovery, and multi-process allocator snapshot reloads can perform
more work. The multi-writer mode still serializes the allocator snapshot on each commit.

Space is reclaimed during subsequent writes. After a large deletion, a file can temporarily
exceed the steady-state size target. An untouched table or an idle database does not promise
immediate file truncation, and a long-lived reader/savepoint necessarily retains its snapshot.

## Reusable benchmark

Run `just bench_space` (or `just bench_space 100000 /tmp/space.json` for a smaller run).
It requires the same native libraries as the other comparison benchmarks, including libclang.
`REDB_SPACE_ENTRIES` and `REDB_SPACE_OUTPUT` also configure the executable directly:

```sh
REDB_SPACE_ENTRIES=512000 REDB_SPACE_OUTPUT=/tmp/space.json \
    cargo bench -p redb-bench-compare --bench space_benchmark
python3 scripts/space-report.py /tmp/space.json --output /tmp/space.md --max-ratio 1.5
```

The seeded generator produces random 24-byte keys and incompressible 32-, 150-, and 1024-byte
values, plus 4096-byte values that cross the default node-size boundary. Each workload loads
records, deletes half, and churns the remaining population twice.
Transactions process 256 records; churn removes and inserts 256 records. Both engines use
synchronous commits. The default size gives 2000 load and 1000 deletion/churn samples, so
p99.9 is meaningful. The benchmark reopens redb, verifies every surviving key/value, checks
the record count and database integrity, and never invokes redb compaction.

JSON includes file/directory sizes, logical payload, redb metadata and fragmentation, and
p50/p99/p99.9/max transaction latency for both engines. RocksDB uses LZ4 compression and is flushed before measuring;
both its online size and its fully compacted reference size are recorded. Its reference
flush/compaction time is excluded from transaction latency. The strict 1.5x size target applies
after load and after the second churn cycle; all transient measurements remain in the report.
Small populations also expose the fixed working space needed by a batch of copy-on-write
updates, so results at another population/batch size may differ. Timing on shared CI runners
is diagnostic; deterministic work bounds are tested separately.

CI saves JSON artifacts and a job summary, then posts or updates one GitHub PR comment.
Same-repository PRs have a separate reporting job that parses numeric JSON without checking
out or executing PR code, so the first PR introducing this workflow is reported too.
The privileged `workflow_run` reporter reads only JSON from the unprivileged benchmark job;
it checks out default-branch reporting code and never executes PR artifacts. GitHub activates
that reporter after its workflow is on the default branch. Superseded commits are ignored.

## Existing databases

v3 files remain readable and writable using their original geometry and buddy pointers.
Opening one does not upgrade it in place, and the v4 space improvements require a new file.
Upstream redb cannot open v4 files. Older upstream versions may also reject v3 files after
this fork writes allocator snapshots introduced by the fork; retaining v3 geometry does not
promise backwards compatibility with every older upstream release.

To migrate, open the v3 source using this fork, create a new destination, and stream each table
into it using its original typed `TableDefinition` (and `MultimapTableDefinition` for multimaps).
Commit in bounded batches; compare table counts and values, close/reopen the destination,
and run `check_integrity()` before switching the application to it. Keep the source until
validation completes. Custom key ordering and type identities must be preserved; copying
everything through a byte-slice table is not a general migration for typed databases.
