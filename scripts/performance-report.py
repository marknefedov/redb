#!/usr/bin/env python3
"""Validate numeric benchmark artifacts and compare counterbalanced runs."""
import argparse
import json
from pathlib import Path
import re
from statistics import median


ORDER = ("base", "head", "head", "base")
MODES = ("memory", "durable")
SIZES = (150, 4096)
PHASES = ("load", "churn", "random_read")
APIS = ("classic", "concurrent")
GROUP_APIS = (*APIS, "grouped")
WRITER_WORKLOADS = ("independent", "hot_key")
METRIC_PHASES = ("writer_wait", "queue_wait", "validation", "replay", "commit", "write", "sync")
METRIC_COUNTERS = ("physical_commits", "durable_commits", "commit_errors", "written_bytes",
                   "write_errors", "sync_errors", "group_batches", "grouped_transactions",
                   "max_group_size", "conflicts", "queue_rejections")
GROUP_LIMITS = ("max_batch_transactions", "max_batch_bytes", "max_queue_transactions", "max_queue_bytes")
MAX_BYTES = 256 * 1024


def number(value, minimum=0, maximum=2**53 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("Expected a bounded safe integer")
    return value


def revision(value):
    if not isinstance(value, str) or re.fullmatch("[0-9a-f]{40}", value) is None:
        raise ValueError("Expected a full commit SHA")
    return value


def choices(values, allowed):
    if (not isinstance(values, list) or not values or len(values) > len(allowed)
            or any(type(value) is not type(allowed[0]) or value not in allowed for value in values)
            or len(set(values)) != len(values)):
        raise ValueError("Unexpected workload configuration")
    return tuple(values)


def validate_report(report):
    if number(report["schema"]) != 1:
        raise ValueError("Unsupported benchmark schema")
    entries = number(report["entries"], 1)
    batch = number(report["batch_size"], 1)
    repeats = number(report["repeats"], 1, 10)
    modes = choices(report["modes"], MODES)
    sizes = choices(report["value_bytes"], SIZES)
    rows = report["results"]
    if not isinstance(rows, list) or len(rows) != repeats * len(modes) * len(sizes) * len(PHASES):
        raise ValueError("Incomplete benchmark results")
    samples = (entries + batch - 1) // batch
    seen = set()
    validated = []
    for row in rows:
        round_number = number(row["round"], 1, repeats)
        mode = row["mode"]
        size = number(row["value_bytes"])
        phase = row["phase"]
        if mode not in modes or size not in sizes or phase not in PHASES:
            raise ValueError("Unsupported benchmark workload")
        key = (round_number, mode, size, phase)
        if key in seen:
            raise ValueError("Duplicate benchmark workload")
        seen.add(key)
        if (number(row["operations"]) != entries * (2 if phase == "churn" else 1)
                or number(row["transactions"]) != (1 if phase == "random_read" else samples)
                or number(row["samples"]) != samples):
            raise ValueError("Inconsistent operation or sample counts")
        elapsed = number(row["elapsed_ns"], 1)
        throughput = number(row["ops_per_second"], 1)
        transaction_rate = number(row["transactions_per_second"])
        if (throughput != row["operations"] * 1_000_000_000 // elapsed
                or transaction_rate != row["transactions"] * 1_000_000_000 // elapsed):
            raise ValueError("Inconsistent throughput")
        latency = tuple(number(row["latency_ns"][name]) for name in ("p50", "p99", "p999", "max"))
        if list(latency) != sorted(latency):
            raise ValueError("Non-monotonic latency percentiles")
        validated.append(((mode, size, phase), throughput, latency))
    return (entries, batch, repeats, modes, sizes), validated


def render_concurrent(report):
    if number(report["schema"]) == 2:
        return render_grouped_concurrent(report)
    if number(report["schema"]) != 1 or number(report["value_bytes"]) != 150:
        raise ValueError("Unsupported concurrent benchmark schema")
    transactions = number(report["transactions_per_writer"], 1)
    reference_keys = number(report["reference_keys"], 1)
    preparation = number(report["preparation_reads"], 0, reference_keys)
    retry_limit = number(report["max_retries"])
    modes = choices(report["modes"], MODES)
    writers = choices(report["writers"], (1, 2, 4, 8))
    rows = report["results"]
    if not isinstance(rows, list) or len(rows) != len(modes) * len(writers) * len(APIS) * len(WRITER_WORKLOADS):
        raise ValueError("Incomplete concurrent benchmark results")
    grouped = {}
    for row in rows:
        mode, api, workload = row["mode"], row["api"], row["workload"]
        count = number(row["writers"], 1)
        if mode not in modes or api not in APIS or workload not in WRITER_WORKLOADS or count not in writers:
            raise ValueError("Unsupported writer workload")
        key = (mode, workload, count, api)
        if key in grouped:
            raise ValueError("Duplicate writer workload")
        total = number(row["transactions"], 1)
        attempts = number(row["attempts"], 1)
        conflicts = number(row["conflicts"])
        retries = number(row["retries"])
        elapsed = number(row["elapsed_ns"], 1)
        rate = number(row["transactions_per_second"], 1)
        size = number(row["backend_bytes"], 1)
        latency = tuple(number(row["latency_ns"][name]) for name in ("p50", "p99", "p999", "max"))
        if (total != transactions * count or attempts != total + conflicts or retries != conflicts
                or retries > total * retry_limit or (api == "classic" and conflicts != 0)
                or rate != total * 1_000_000_000 // elapsed or list(latency) != sorted(latency)):
            raise ValueError("Inconsistent writer result")
        grouped[key] = (rate, attempts, conflicts, size, latency)
    lines = ["", "### Head-only writer contention", "",
             f"{transactions:,} committed transactions per writer; {preparation} preparation reads from {reference_keys:,} "
             f"reference keys per attempt; 150-byte values; at most {retry_limit:,} retries per logical transaction.",
             "Both APIs use Immediate commits. Independent writers update separate counters; hot-key writers share a counter. "
             "Latency includes all retries for each committed logical transaction. This compares APIs on the head commit, not against the base commit.", "",
             "| Backend | Workload | Writers | Classic tx/s | Concurrent tx/s | Concurrent/classic | Conflicts / attempts | Classic p99 / p99.9 / max ms | Concurrent p99 / p99.9 / max ms | Classic / concurrent KiB |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for mode in modes:
        for workload in WRITER_WORKLOADS:
            for count in writers:
                classic = grouped[(mode, workload, count, "classic")]
                concurrent = grouped[(mode, workload, count, "concurrent")]
                tails = [" / ".join(f"{value / 1_000_000:.3f}" for value in stats[4][1:])
                         for stats in (classic, concurrent)]
                lines.append(f"| {mode} | {workload} | {count} | {classic[0]:,} | {concurrent[0]:,} | "
                             f"{concurrent[0] / classic[0]:.3f}x | {concurrent[2]:,} / {concurrent[1]:,} | "
                             f"{tails[0]} | {tails[1]} | {classic[3] / 1024:.1f} / {concurrent[3] / 1024:.1f} |")
    return lines


def validate_group_stats(stats, api, limits):
    if api != "grouped":
        if stats is not None:
            raise ValueError("Unexpected group statistics for an ungrouped API")
        return None
    if not isinstance(stats, dict):
        raise ValueError("Missing group statistics")
    validated = {name: number(stats[name]) for name in (
        "queued_transactions", "queued_bytes", "active_transactions", "active_bytes",
        "max_queued_transactions", "max_queued_bytes", "physical_batches", "committed_transactions",
        "max_batch_transactions", "max_batch_bytes")}
    if (any(validated[name] for name in ("queued_transactions", "queued_bytes", "active_transactions", "active_bytes"))
            or validated["max_queued_transactions"] > limits["max_queue_transactions"]
            or validated["max_queued_bytes"] > limits["max_queue_bytes"]
            or validated["max_batch_transactions"] > limits["max_batch_transactions"]
            or validated["max_batch_bytes"] > limits["max_batch_bytes"]
            or validated["committed_transactions"] < validated["physical_batches"]
            or validated["committed_transactions"] > validated["physical_batches"] * validated["max_batch_transactions"]
            or ((validated["physical_batches"] == 0) != (validated["max_batch_transactions"] == 0))):
        raise ValueError("Nonempty or over-budget group queue")
    return validated


def validate_metrics(metrics, api, limits, conflicts, rejections):
    if not isinstance(metrics, dict) or metrics.get("enabled") is not True:
        raise ValueError("Diagnostic metrics must be enabled")
    phases = {}
    for name in METRIC_PHASES:
        phase = {field: number(metrics[name][field]) for field in ("calls", "total_ns", "max_ns")}
        if phase["max_ns"] > phase["total_ns"] or (phase["calls"] == 0 and (phase["total_ns"] or phase["max_ns"])):
            raise ValueError("Inconsistent phase metrics")
        phases[name] = phase
    counters = {name: number(metrics[name]) for name in METRIC_COUNTERS}
    if (counters["durable_commits"] > counters["physical_commits"]
            or counters["commit_errors"] > phases["commit"]["calls"]
            or counters["write_errors"] > phases["write"]["calls"]
            or counters["sync_errors"] > phases["sync"]["calls"]
            or counters["conflicts"] != conflicts or counters["queue_rejections"] != rejections
            or counters["grouped_transactions"] < counters["group_batches"]
            or counters["grouped_transactions"] > counters["group_batches"] * counters["max_group_size"]
            or counters["max_group_size"] > limits["max_batch_transactions"]):
        raise ValueError("Inconsistent diagnostic counters")
    if api != "grouped" and any(counters[name] for name in ("group_batches", "grouped_transactions", "max_group_size")):
        raise ValueError("Unexpected grouped commits")
    if (counters["group_batches"] == 0) != (counters["max_group_size"] == 0):
        raise ValueError("Inconsistent group size")
    return {**counters, **phases}


def validate_writer_sample(row, transactions, count, api, retry_limit, limits, diagnostic=False):
    total = number(row["transactions"], 1)
    attempts = number(row["attempts"], 1)
    conflicts = number(row["conflicts"])
    rejections = number(row["queue_rejections"])
    retries = number(row["retries"])
    elapsed = number(row["elapsed_ns"], 1)
    rate = number(row["transactions_per_second"], 1)
    size = number(row["backend_bytes"], 1)
    latency = tuple(number(row["latency_ns"][name]) for name in ("p50", "p99", "p999", "max"))
    if (total != transactions * count or attempts != total + conflicts + rejections
            or retries != conflicts + rejections or retries > total * retry_limit
            or (api == "classic" and conflicts != 0) or (api != "grouped" and rejections != 0)
            or rate != total * 1_000_000_000 // elapsed or list(latency) != sorted(latency)):
        raise ValueError("Inconsistent writer result")
    result = {"rate": rate, "attempts": attempts, "conflicts": conflicts, "rejections": rejections,
              "bytes": size, "latency": latency, "transactions": total,
              "group_stats": validate_group_stats(row["group_stats"], api, limits)}
    stats = result["group_stats"]
    if stats and (stats["committed_transactions"] > total or stats["max_batch_transactions"] > count
                  or stats["max_queued_transactions"] > count):
        raise ValueError("Impossible grouped transaction counts")
    if diagnostic:
        result["metrics"] = validate_metrics(row["commit_metrics"], api, limits, conflicts, rejections)
        metrics = result["metrics"]
        if stats and (metrics["group_batches"] != stats["physical_batches"]
                      or metrics["grouped_transactions"] != stats["committed_transactions"]
                      or metrics["max_group_size"] > stats["max_batch_transactions"]):
            raise ValueError("Group diagnostics do not match coordinator statistics")
    elif "commit_metrics" in row:
        raise ValueError("Primary throughput must not include instrumentation")
    return result


def render_grouped_concurrent(report):
    if number(report["schema"]) != 2 or number(report["value_bytes"]) != 150:
        raise ValueError("Unsupported concurrent benchmark schema")
    transactions = number(report["transactions_per_writer"], 1)
    reference_keys = number(report["reference_keys"], 1)
    preparation = number(report["preparation_reads"], 0, reference_keys)
    retry_limit = number(report["max_retries"])
    modes = choices(report["modes"], MODES)
    writers = choices(report["writers"], (1, 2, 4, 8, 16))
    limits = {name: number(report["group_commit"][name], 1) for name in GROUP_LIMITS}
    rows = report["results"]
    if not isinstance(rows, list) or len(rows) != len(modes) * len(writers) * len(GROUP_APIS) * len(WRITER_WORKLOADS):
        raise ValueError("Incomplete concurrent benchmark results")
    grouped = {}
    for row in rows:
        mode, api, workload = row["mode"], row["api"], row["workload"]
        count = number(row["writers"], 1)
        if mode not in modes or api not in GROUP_APIS or workload not in WRITER_WORKLOADS or count not in writers:
            raise ValueError("Unsupported writer workload")
        key = (mode, workload, count, api)
        if key in grouped:
            raise ValueError("Duplicate writer workload")
        primary = validate_writer_sample(row, transactions, count, api, retry_limit, limits)
        diagnostic = validate_writer_sample(row["diagnostics"], transactions, count, api, retry_limit, limits, diagnostic=True)
        grouped[key] = (primary, diagnostic)
    lines = ["", "### Head-only writer contention", "",
             f"{transactions:,} committed transactions per writer; {preparation} preparation reads from {reference_keys:,} "
             f"reference keys per attempt; 150-byte values; at most {retry_limit:,} retries per logical transaction.",
             "All APIs use Immediate commits. Independent writers update separate counters; hot-key writers share a counter. "
             "Primary throughput and latency use metrics disabled. Latency includes conflict and queue-full retries. "
             "Each diagnostic measurement uses a separate fresh database with metrics enabled; its timing is not blended into primary results. "
             "These compare APIs on the head commit, not against the base commit.",
             f"Group limits: {limits['max_batch_transactions']:,} transactions / {limits['max_batch_bytes']:,} accounted prepared bytes per batch; "
             f"{limits['max_queue_transactions']:,} transactions / {limits['max_queue_bytes']:,} accounted prepared bytes queued. "
             "Grouping is opportunistic, with no added collection delay. Selected batch peaks include validation losers; accepted sizes count successful transactions.", "",
             "| Backend | Workload | Writers | API | tx/s | vs classic | Conflicts / queue full / attempts | p99 / p99.9 / max ms | KiB | Commits / accepted tx / max selected |",
             "|---|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    for mode in modes:
        for workload in WRITER_WORKLOADS:
            for count in writers:
                classic = grouped[(mode, workload, count, "classic")][0]
                for api in GROUP_APIS:
                    sample = grouped[(mode, workload, count, api)][0]
                    tails = " / ".join(f"{value / 1_000_000:.3f}" for value in sample["latency"][1:])
                    stats = sample["group_stats"]
                    groups = f"{stats['physical_batches']:,} / {stats['committed_transactions']:,} / {stats['max_batch_transactions']:,}" if stats else "-"
                    lines.append(f"| {mode} | {workload} | {count} | {api} | {sample['rate']:,} | "
                                 f"{sample['rate'] / classic['rate']:.3f}x | {sample['conflicts']:,} / {sample['rejections']:,} / "
                                 f"{sample['attempts']:,} | {tails} | {sample['bytes'] / 1024:.1f} | {groups} |")
    lines.extend(["", "### Separate instrumented run: I/O and grouping", "",
                  "Backend sync counts include synchronization during growth and maintenance. Memory backends implement these calls without disk I/O. "
                  "Written bytes include page and metadata traffic; they are not logical payload bytes.", "",
                  "| Backend | Workload | Writers | API | Diagnostic tx/s | Physical commits | Sync calls | tx/sync | Written KiB | Groups / accepted tx / max accepted | Peak queued tx / KiB |",
                  "|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"])
    phase_lines = ["", "### Separate instrumented run: phase mean / max us", "",
                   "Phase totals overlap: commit includes writes and synchronization, and waits from different threads overlap. "
                   "Do not add phase times as an elapsed-time breakdown. Phase maxima are observed timings, not latency guarantees. "
                   "Raw JSON preserves every phase's call count and total time.", "",
                   "| Backend | Workload | Writers | API | Writer wait | Queue wait | Validation | Replay | Commit | Write | Sync |",
                   "|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"]
    for mode in modes:
        for workload in WRITER_WORKLOADS:
            for count in writers:
                for api in GROUP_APIS:
                    sample = grouped[(mode, workload, count, api)][1]
                    metrics, stats = sample["metrics"], sample["group_stats"]
                    syncs = metrics["sync"]["calls"]
                    ratio = f"{sample['transactions'] / syncs:.3f}" if syncs else "n/a"
                    peak = f"{stats['max_queued_transactions']:,} / {stats['max_queued_bytes'] / 1024:.1f}" if stats else "-"
                    lines.append(f"| {mode} | {workload} | {count} | {api} | {sample['rate']:,} | "
                                 f"{metrics['physical_commits']:,} | {syncs:,} | {ratio} | {metrics['written_bytes'] / 1024:.1f} | "
                                 f"{metrics['group_batches']:,} / {metrics['grouped_transactions']:,} / {metrics['max_group_size']:,} | {peak} |")
                    phases = []
                    for name in METRIC_PHASES:
                        phase = metrics[name]
                        mean = phase["total_ns"] / phase["calls"] / 1000 if phase["calls"] else 0
                        phases.append(f"{mean:.3f} / {phase['max_ns'] / 1000:.3f}")
                    phase_lines.append(f"| {mode} | {workload} | {count} | {api} | " + " | ".join(phases) + " |")
    lines.extend(phase_lines)
    return lines


def render(data):
    if number(data["schema"]) != 1 or data["order"] != list(ORDER):
        raise ValueError("Unsupported comparison schema or execution order")
    base = revision(data["base_sha"])
    head = revision(data["head_sha"])
    runs = data["runs"]
    if not isinstance(runs, list) or len(runs) != len(ORDER):
        raise ValueError("Expected four counterbalanced runs")
    configuration = None
    grouped = {label: {} for label in ORDER}
    for label, run in zip(ORDER, runs):
        if run["revision"] != label:
            raise ValueError("Unexpected execution order")
        config, rows = validate_report(run["report"])
        if configuration is not None and config != configuration:
            raise ValueError("Mismatched benchmark configurations")
        configuration = config
        for key, throughput, latency in rows:
            grouped[label].setdefault(key, []).append((throughput, latency))
    entries, batch, repeats, modes, sizes = configuration
    lines = [
        f"Base `{base}`; head `{head}`.",
        f"{entries:,} initial entries; {batch:,} records per batch. "
        f"Execution order: base, head, head, base; {2 * repeats} measurements per revision.",
        "Throughput is median [min, max]. Latency columns show the worst reported p99 / p99.9 / max across runs.",
        "",
        "| Backend | Value bytes | Workload | Base ops/s [range] | Head ops/s [range] | Head/base | Base p99 / p99.9 / max ms | Head p99 / p99.9 / max ms |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for mode in modes:
        for size in sizes:
            for phase in PHASES:
                key = (mode, size, phase)
                statistics = []
                for label in ("base", "head"):
                    rows = grouped[label][key]
                    throughputs = [row[0] for row in rows]
                    tails = [max(row[1][index] for row in rows) for index in (1, 2, 3)]
                    statistics.append((median(throughputs), min(throughputs), max(throughputs), tails))
                base_stats, head_stats = statistics
                rates = [f"{stats[0]:,.0f} [{stats[1]:,}, {stats[2]:,}]" for stats in statistics]
                tails = [" / ".join(f"{time / 1_000_000:.3f}" for time in stats[3]) for stats in statistics]
                lines.append(f"| {mode} | {size} | {phase} | {rates[0]} | {rates[1]} | "
                             f"{head_stats[0] / base_stats[0]:.3f}x | {tails[0]} | {tails[1]} |")
    lines.extend(["", "Both backends use Immediate commits. Memory isolates engine CPU work; durable uses a file with synchronous commits. "
                  "Write batch latency includes begin, mutations, and commit; read batch latency covers lookups in one held snapshot. "
                  "Churn counts each removal and insertion as an operation. Dataset generation and verification are outside timing. "
                  "Shared-runner timing is diagnostic; there is no performance gate. The separate space benchmark keeps its 1.5x size gate."])
    if "concurrent" in data:
        lines.extend(render_concurrent(data["concurrent"]))
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.input.stat().st_size > MAX_BYTES:
        raise ValueError("Report exceeds 256 KiB")
    args.output.write_text(render(json.loads(args.input.read_text())))


if __name__ == "__main__":
    main()
