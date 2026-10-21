import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("space_report", Path(__file__).with_name("space-report.py"))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def fixture():
    return {"schema": 1, "entries": 512000, "batch_size": 256, "rocksdb_compression": "lz4", "results": [
        {"value_bytes": size, "phase": phase, "redb_bytes": 1400,
         "rocksdb_online_bytes": 1500, "rocksdb_compacted_bytes": 1000,
         "redb_latency": {"p99_us": 1000, "p999_us": 2000, "max_us": 3000}}
        for size in (32, 150, 1024, 4096)
        for phase in ("load", "delete_half", "churn_1", "churn_2")
    ]}


class SpaceReportTests(unittest.TestCase):
    def test_transient_debt_is_visible_but_not_the_steady_state_gate(self):
        data = fixture()
        data["results"][1]["redb_bytes"] = 4000
        markdown, ratio = report.render(data)
        self.assertEqual(ratio, 1.4)
        self.assertIn("4.00x", markdown)
        self.assertIn("deletion", markdown.lower())

    def test_rejects_incomplete_duplicate_or_unknown_workloads(self):
        data = fixture()
        data["results"].pop()
        with self.assertRaises(ValueError):
            report.render(data)
        data = fixture()
        data["results"][1] = copy.deepcopy(data["results"][0])
        with self.assertRaises(ValueError):
            report.render(data)
        data = fixture()
        data["results"][0]["phase"] = "@everyone"
        with self.assertRaises(ValueError):
            report.render(data)

    def test_rejects_non_numeric_non_finite_and_unsafe_integers(self):
        for value in ("[click](https://invalid)", True, -1, float("nan"), float("inf"), 2**53):
            with self.subTest(value=value), self.assertRaises(ValueError):
                report.number(value)

    def test_rejects_empty_reference(self):
        data = fixture()
        data["results"][0]["rocksdb_compacted_bytes"] = 0
        with self.assertRaises(ValueError):
            report.render(data)


if __name__ == "__main__":
    unittest.main()
