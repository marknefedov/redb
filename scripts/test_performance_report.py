import copy
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("performance_report", Path(__file__).with_name("performance-report.py"))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def fixture():
    data = {"schema": 1, "base_sha": "a" * 40, "head_sha": "b" * 40,
            "order": list(report.ORDER), "runs": []}
    for label in report.ORDER:
        rate = 200 if label == "head" else 100
        rows = [{"round": 1, "mode": mode, "value_bytes": size, "phase": phase,
                 "operations": 2048 if phase == "churn" else 1024,
                 "transactions": 1 if phase == "random_read" else 16,
                 "samples": 16, "elapsed_ns": (2048 if phase == "churn" else 1024) * 1_000_000_000 // rate,
                 "ops_per_second": rate,
                 "transactions_per_second": (1 if phase == "random_read" else 16) * rate // (2048 if phase == "churn" else 1024),
                 "latency_ns": {"p50": 1000, "p99": 2000, "p999": 3000, "max": 4000}}
                for mode in report.MODES for size in report.SIZES for phase in report.PHASES]
        data["runs"].append({"revision": label, "report": {
            "schema": 1, "entries": 1024, "batch_size": 64, "repeats": 1,
            "modes": list(report.MODES), "value_bytes": list(report.SIZES), "results": rows}})
    return data


def concurrent_fixture():
    return {"schema": 1, "transactions_per_writer": 1024, "preparation_reads": 64, "reference_keys": 1024,
            "value_bytes": 150, "max_retries": 16384, "modes": list(report.MODES), "writers": [1, 2, 4],
            "results": [
                {"mode": mode, "api": api, "workload": workload, "writers": writers,
                 "transactions": 1024 * writers, "attempts": 1024 * writers + (5 if api == "concurrent" else 0),
                 "conflicts": 5 if api == "concurrent" else 0, "retries": 5 if api == "concurrent" else 0,
                 "elapsed_ns": 1024 * writers * 1_000_000, "transactions_per_second": 1000,
                 "backend_bytes": 65536,
                 "latency_ns": {"p50": 1000, "p99": 2000, "p999": 3000, "max": 4000}}
                for mode in report.MODES for workload in report.WRITER_WORKLOADS
                for writers in (1, 2, 4) for api in report.APIS]}


class PerformanceReportTests(unittest.TestCase):
    def test_compares_counterbalanced_runs_and_publishes_worst_tails(self):
        data = fixture()
        data["runs"][2]["report"]["results"][0]["ops_per_second"] = 400
        data["runs"][2]["report"]["results"][0]["elapsed_ns"] //= 2
        data["runs"][2]["report"]["results"][0]["transactions_per_second"] *= 2
        data["runs"][2]["report"]["results"][0]["latency_ns"]["max"] = 9_000_000
        markdown = report.render(data)
        self.assertIn("300 [200, 400] | 3.000x", markdown)
        self.assertIn("0.002 / 0.003 / 9.000", markdown)
        self.assertIn("base, head, head, base", markdown)

    def test_rejects_incomplete_duplicate_or_unknown_workloads(self):
        for mutate in (
            lambda d: d["runs"][0]["report"]["results"].pop(),
            lambda d: d["runs"][0]["report"]["results"].__setitem__(1, copy.deepcopy(d["runs"][0]["report"]["results"][0])),
            lambda d: d["runs"][0]["report"]["results"][0].__setitem__("phase", "@everyone"),
            lambda d: d["runs"][0]["report"]["results"][0].__setitem__("mode", "[click](https://invalid)"),
        ):
            data = fixture()
            mutate(data)
            with self.assertRaises(ValueError):
                report.render(data)

    def test_rejects_wrong_order_or_mismatched_configurations(self):
        data = fixture()
        data["runs"][0]["revision"] = "head"
        with self.assertRaises(ValueError):
            report.render(data)
        data = fixture()
        data["runs"][0]["report"]["batch_size"] = 128
        with self.assertRaises(ValueError):
            report.render(data)

    def test_rejects_inconsistent_counts_and_latencies(self):
        for field, value in (("operations", 1), ("samples", 1), ("transactions", 1000), ("elapsed_ns", 0)):
            data = fixture()
            data["runs"][0]["report"]["results"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                report.render(data)
        data = fixture()
        data["runs"][0]["report"]["results"][0]["latency_ns"]["p99"] = 10_000_000
        with self.assertRaises(ValueError):
            report.render(data)

    def test_rejects_non_numeric_non_finite_and_unsafe_integers(self):
        for value in ("[click](https://invalid)", True, -1, float("nan"), float("inf"), 2**53):
            with self.subTest(value=value), self.assertRaises(ValueError):
                report.number(value)
        for value in ("@everyone", "b" * 39, "B" * 40, "b" * 40 + "\n"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                report.revision(value)

    def test_cli_rejects_oversized_artifact_before_reading_json(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "oversized.json"
            output = Path(temporary) / "report.md"
            source.write_text(" " * (report.MAX_BYTES + 1))
            result = subprocess.run([sys.executable, str(Path(report.__file__)), str(source), "--output", str(output)],
                                    text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("exceeds 256 KiB", result.stderr)
            self.assertFalse(output.exists())

    def test_head_only_writer_report_includes_conflicts_and_tail_latency(self):
        data = fixture()
        data["concurrent"] = concurrent_fixture()
        markdown = report.render(data)
        self.assertIn("Head-only writer contention", markdown)
        self.assertIn("independent | 1 | 1,000 | 1,000 | 1.000x | 5 / 1,029", markdown)
        self.assertIn("0.002 / 0.003 / 0.004 | 64.0 / 64.0", markdown)

    def test_rejects_inconsistent_or_unknown_writer_results(self):
        for field, value in (("workload", "@everyone"), ("attempts", 1), ("transactions", 1),
                             ("conflicts", 1), ("writers", 8), ("backend_bytes", 0)):
            data = fixture()
            data["concurrent"] = concurrent_fixture()
            data["concurrent"]["results"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                report.render(data)


@unittest.skipUnless(shutil.which("node"), "Node is needed to check the GitHub comment jobs")
class CommentWorkflowTests(unittest.TestCase):
    def execute(self, workflow, data=None, existing=False, stale=False, fork_fallback=False):
        data = fixture() if data is None else data
        workflow_path = Path(__file__).resolve().parents[1] / ".github" / "workflows" / workflow
        block = workflow_path.read_text().split("          script: |\n", 1)[1]
        script = "\n".join(line[12:] for line in block.splitlines())
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact = root / "performance-artifact"
            artifact.mkdir()
            (artifact / "performance-results.json").write_text(json.dumps(data))
            (root / "performance-results.md").write_text(report.render(fixture()))
            config = {"existing": existing, "stale": stale, "fork_fallback": fork_fallback}
            harness = """
const config = CONFIG;
const calls = [];
const head = 'b'.repeat(40), marker = '<!-- redb-performance-benchmark -->';
const context = {issue: {number: 7}, repo: {owner: 'owner', repo: 'repo'}, runId: 123,
  payload: {pull_request: {head: {sha: head}}, workflow_run: {
    head_sha: head, html_url: 'https://github.com/owner/repo/actions/runs/123',
    pull_requests: config.fork_fallback ? [] : [{number: 7}]}}};
const github = {rest: {
  pulls: {get: async () => ({data: {head: {sha: config.stale ? 'c'.repeat(40) : head}, state: 'open'}})},
  issues: {listComments: () => {},
    createComment: async args => calls.push({action: 'create', ...args}),
    updateComment: async args => calls.push({action: 'update', ...args})},
  repos: {listPullRequestsAssociatedWithCommit: async () => {
    calls.push({action: 'associated'});
    return {data: [{number: 7, head: {sha: head}, state: 'open'}]};
  }}}, paginate: async () => config.existing ? [{id: 42, user: {type: 'Bot'}, body: marker}] : []};
const core = {setFailed: message => {throw new Error(message)}};
(async () => {
await (async () => {
SCRIPT
})();
process.stdout.write(JSON.stringify(calls));
})().catch(error => {process.stderr.write(String(error)); process.exit(1)});
""".replace("CONFIG", json.dumps(config)).replace("SCRIPT", script)
            result = subprocess.run(["node", "-e", harness], cwd=root, text=True, capture_output=True, check=True)
            return json.loads(result.stdout)

    def test_same_repo_comment_create_update_and_stale_head(self):
        workflow = "performance-benchmark.yml"
        created = self.execute(workflow)
        self.assertEqual(created[0]["action"], "create")
        self.assertIn("2.000x", created[0]["body"])
        self.assertIn("0.002 / 0.003 / 0.004", created[0]["body"])
        updated = self.execute(workflow, existing=True)
        self.assertEqual(updated[0]["action"], "update")
        self.assertEqual(updated[0]["comment_id"], 42)
        self.assertEqual(self.execute(workflow, stale=True), [])

    def test_same_repo_rejects_invalid_or_unsafe_artifact_data(self):
        for mutate in (
            lambda d: d.__setitem__("head_sha", "b" * 40 + "\n"),
            lambda d: d["runs"][0]["report"]["results"][0].__setitem__("phase", "@everyone"),
            lambda d: d["runs"][0]["report"]["results"][0].__setitem__("operations", True),
            lambda d: d["runs"][0]["report"]["results"][0].__setitem__("ops_per_second", 2**53),
        ):
            data = fixture()
            mutate(data)
            with self.assertRaises(subprocess.CalledProcessError):
                self.execute("performance-benchmark.yml", data=data)

    def test_same_repo_writer_report_and_validation(self):
        data = fixture()
        data["concurrent"] = concurrent_fixture()
        created = self.execute("performance-benchmark.yml", data=data)
        self.assertIn("Head-only writer contention", created[0]["body"])
        self.assertIn("5 / 1029", created[0]["body"])
        data["concurrent"]["results"][0]["conflicts"] = 1
        with self.assertRaises(subprocess.CalledProcessError):
            self.execute("performance-benchmark.yml", data=data)

    def test_fork_reporter_fallback_update_and_stale_head(self):
        workflow = "performance-benchmark-comment.yml"
        created = self.execute(workflow, fork_fallback=True)
        self.assertEqual([call["action"] for call in created], ["associated", "create"])
        self.assertIn("2.000x", created[1]["body"])
        updated = self.execute(workflow, existing=True)
        self.assertEqual(updated[0]["action"], "update")
        self.assertEqual(self.execute(workflow, stale=True), [])
        data = fixture()
        data["head_sha"] = "c" * 40
        with self.assertRaises(subprocess.CalledProcessError):
            self.execute(workflow, data=data)


if __name__ == "__main__":
    unittest.main()
