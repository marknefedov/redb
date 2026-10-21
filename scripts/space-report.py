#!/usr/bin/env python3
"""Validate and render benchmark data; safe for artifacts from untrusted PRs."""
import argparse
import json
from pathlib import Path


def number(value):
    if type(value) is not int or not 0 <= value <= 2**53 - 1:
        raise ValueError("Expected a nonnegative safe integer")
    return value


def render(data):
    if (number(data["schema"]) != 1 or data["rocksdb_compression"] != "lz4"
            or len(data["results"]) != 16):
        raise ValueError("Unsupported benchmark report")
    count = number(data["entries"])
    batch = number(data["batch_size"])
    lines = [
        f"{count:,.0f} initial entries; {batch:,.0f} records per durable transaction. "
        "Random, incompressible keys and values. RocksDB uses LZ4. No redb compaction.",
        "",
        "| Value bytes | Phase | redb MiB | RocksDB online MiB | RocksDB compacted MiB | Size ratio | redb p99 / p99.9 / max ms |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    ratios = []
    seen = set()
    for row in data["results"]:
        phase = row["phase"]
        if phase not in ("load", "delete_half", "churn_1", "churn_2"):
            raise ValueError("Unsupported phase")
        size = number(row["value_bytes"])
        if size not in (32, 150, 1024, 4096) or (size, phase) in seen:
            raise ValueError("Unexpected or duplicate workload")
        seen.add((size, phase))
        redb = number(row["redb_bytes"])
        rocks_online = number(row["rocksdb_online_bytes"])
        rocks = number(row["rocksdb_compacted_bytes"])
        if rocks == 0:
            raise ValueError("Empty RocksDB reference")
        ratio = redb / rocks
        if phase in ("load", "churn_2"):
            ratios.append(ratio)
        latency = row["redb_latency"]
        times = " / ".join(f"{number(latency[k]) / 1000:.3f}" for k in ("p99_us", "p999_us", "max_us"))
        lines.append(f"| {size:.0f} | {phase} | {redb / 2**20:.2f} | {rocks_online / 2**20:.2f} | {rocks / 2**20:.2f} | {ratio:.2f}x | {times} |")
    lines.extend(["", "The 1.5x target is checked after load and after the second churn cycle. "
                  "Deletion and the first churn cycle expose transient reclamation debt; bounded maintenance converges during subsequent writes. "
                  "Churn removes and inserts each record. "
                  "Size ratio compares normal-operation redb file size with compacted RocksDB directory size, including its metadata and WAL. "
                  "Latency includes begin, mutations, and synchronous commit; it excludes reference flush/compaction and verification. "
                  "Shared-runner timing is diagnostic, not a real-time guarantee."])
    return "\n".join(lines) + "\n", max(ratios)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-ratio", type=float)
    args = parser.parse_args()
    if args.input.stat().st_size > 256 * 1024:
        raise ValueError("Report exceeds 256 KiB")
    report, ratio = render(json.loads(args.input.read_text()))
    args.output.write_text(report)
    if args.max_ratio is not None and ratio > args.max_ratio:
        raise SystemExit(f"Size ratio {ratio:.2f} exceeds target {args.max_ratio:.2f}")


if __name__ == "__main__":
    main()
