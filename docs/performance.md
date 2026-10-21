# Performance measurements

`performance_benchmark` measures the engine without compiling other storage engines:

```sh
just bench_performance
# Optional record count and output path:
just bench_performance 65536 'performance results.json'

REDB_PERF_OUTPUT="$PWD/performance.json" cargo bench -p redb-bench --bench performance_benchmark
```

It uses the same deterministic random keys and incompressible values for two backends:
an in-memory backend and a file backend, both with `Immediate` commits. Memory measurements
isolate engine work from filesystem synchronization; file measurements include synchronous
commits. Neither workload calls redb compaction. Choose a disk-backed temporary directory
for file measurements: on Unix, set `TMPDIR` to a directory on the filesystem being measured.
A RAM-backed temporary directory measures file operations without persistent-device latency.
CI uses the runner's temporary directory for both the base and head measurements.

The workload loads a database, replaces its records with new keys, then reads the live keys
in random order. Write batch timing includes transaction creation, mutations, and commit.
Read batch timing measures lookups while holding one read transaction and table for the phase.
Data generation and content/integrity checks run outside the timed sections. Churn counts
each deletion and insertion as one operation, so its operation count is twice the record count.

The following environment variables select the workload:

| Variable | Default | Meaning |
|---|---|---|
| `REDB_PERF_ENTRIES` | `65536` | Initial live records |
| `REDB_PERF_BATCH` | `64` | Records per measured batch |
| `REDB_PERF_REPEATS` | `3` | Independent database runs per workload |
| `REDB_PERF_MODES` | `memory,durable` | Comma-separated backends |
| `REDB_PERF_VALUES` | `150,4096` | Comma-separated value sizes in bytes |
| `REDB_PERF_OUTPUT` | unset | Optional JSON output path; use an absolute path because Cargo changes the working directory |

JSON includes elapsed time, operation and transaction counts, throughput, and batch latency
p50, p99, p99.9, and maximum. Read and write batch latencies have different meanings and
should be compared only with the same workload and batch size. The default workload produces
1,024 latency samples per phase per run; these are empirical percentiles, not latency guarantees.
The recipe writes a single-revision report. CI wraps four such reports into the comparison
bundle consumed by `scripts/performance-report.py`.

For each PR, the Performance benchmarks workflow builds the exact base and head commits
with the same benchmark source and Rust toolchain. It runs them sequentially in base, head,
head, base order on one runner. The report includes median throughput and each revision's
range, plus the worst per-run p99, p99.9, and maximum latency. Raw runs are preserved in the
`performance-results` artifact. PR comments identify both measured commits and update in place;
reports from superseded heads are ignored. A manual workflow run accepts a `base_ref` for
comparisons outside a PR.

Shared runners vary in CPU scheduling and filesystem synchronization time, so performance
comparisons are diagnostic rather than a CI gate. Use several paired repetitions on an idle
machine before interpreting small differences. The separate Space benchmark continues to
check the 1.5x RocksDB size target and publish transient space and latency measurements.

## Concurrent writers

`concurrent_write_benchmark` compares classic writes, optimistic writes, and optimistic writes
with [bounded group commit](group-commit.md) on the current code. It runs independent per-writer counters and a shared hot counter,
using both memory and durable file backends with Immediate commits:

```sh
just bench_concurrent
# Optional committed transactions per writer and output path:
just bench_concurrent 1024 'concurrent results.json'

REDB_CONCURRENT_OUTPUT="$PWD/concurrent.json" cargo bench -p redb-bench --bench concurrent_write_benchmark
```

Each transaction reads a fixed amount of reference data, then updates a counter. Every worker
must complete its fixed transaction count. Conflicting concurrent transactions retry with a
fresh snapshot up to a configured limit. Reported logical transaction latency includes all
those retries; attempts, conflicts, and backend bytes are published alongside throughput.
The final counters and database integrity are checked outside the measured interval.
Queue admission failures are counted separately from validation conflicts; both use the
configured retry limit, and queue retries yield before starting a fresh transaction.

Headline throughput and latency run with commit metrics disabled. A separate fresh-database
diagnostic pass repeats the same workload with metrics enabled and reports writer wait,
group queue wait, validation, replay, physical commit, backend write and synchronization
timings, successful written bytes, actual synchronization calls, and coordinator statistics.
Instrumentation changes execution cost and scheduling, so diagnostic timing is not used
as headline throughput. Phase times can overlap or nest and must not be summed into wall time.

| Variable | Default | Meaning |
|---|---|---|
| `REDB_CONCURRENT_MODES` | `memory,durable` | Comma-separated backends |
| `REDB_CONCURRENT_WRITERS` | `1,2,4` | Comma-separated writer counts; `8` and `16` are also supported |
| `REDB_CONCURRENT_TRANSACTIONS` | `1024` | Committed transactions per writer |
| `REDB_CONCURRENT_PREPARATION_READS` | `64` | Reference reads per transaction attempt |
| `REDB_CONCURRENT_MAX_RETRIES` | `16384` | Retry limit per logical transaction |
| `REDB_CONCURRENT_OUTPUT` | unset | Optional JSON output path; use an absolute path |
| `REDB_GROUP_MAX_BATCH_TRANSACTIONS` | `16` | Maximum selected requests per grouped publication |
| `REDB_GROUP_MAX_BATCH_BYTES` | `1048576` | Maximum accounted prepared bytes per selected group |
| `REDB_GROUP_MAX_QUEUE_TRANSACTIONS` | `64` | Maximum waiting requests, excluding the active group |
| `REDB_GROUP_MAX_QUEUE_BYTES` | `4194304` | Maximum accounted bytes in the waiting queue |

CI runs this benchmark on the head revision after the base/head comparison, using
1/2/4/8/16 writers. It adds head-only contention and diagnostic tables to the PR comment.
This separates API scaling from the exact base/head engine comparison. Raw JSON preserves
both passes and their independent correctness, retry, latency, and coordinator measurements.
The shared-counter workload exposes conflicts and their tail latency cost; concurrent
preparation and group commit are not expected to improve every contention pattern.
With one outstanding transaction per thread, two writers often leave only one waiting
request, so higher writer counts are needed to demonstrate opportunities for grouping.
