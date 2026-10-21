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
commits. Neither workload calls redb compaction.

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

For each PR, the Performance benchmark workflow builds the exact base and head commits
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

`concurrent_write_benchmark` compares the classic write API with the concurrent preparation
API on the current code. It runs independent per-writer counters and a shared hot counter,
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

| Variable | Default | Meaning |
|---|---|---|
| `REDB_CONCURRENT_MODES` | `memory,durable` | Comma-separated backends |
| `REDB_CONCURRENT_WRITERS` | `1,2,4` | Comma-separated writer counts; `8` is also supported |
| `REDB_CONCURRENT_TRANSACTIONS` | `1024` | Committed transactions per writer |
| `REDB_CONCURRENT_PREPARATION_READS` | `64` | Reference reads per transaction attempt |
| `REDB_CONCURRENT_MAX_RETRIES` | `16384` | Retry limit per logical transaction |
| `REDB_CONCURRENT_OUTPUT` | unset | Optional JSON output path; use an absolute path |

CI runs this benchmark on the head revision after the base/head comparison and adds a
head-only contention table to the PR comment. This separates API scaling from the exact
base/head engine comparison. The shared-counter workload exposes conflicts and their tail
latency cost; concurrent preparation is not expected to improve every contention pattern.
