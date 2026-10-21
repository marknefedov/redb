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
WRITER_WORKLOADS = ("independent", "hot_key")
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
