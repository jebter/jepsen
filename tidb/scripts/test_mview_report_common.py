import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import mview_report_common as report_common


class MViewReportCommonTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="mview-report-common-test-")
        self.root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_manifest_only_store_is_not_recognized(self):
        store = self.root / "manifest-only"
        (store / "build").mkdir(parents=True)
        (store / "build" / "manifest.json").write_text(
            json.dumps({"workload": "mv-stateful", "nemesis": "kill-db"})
        )

        report = report_common.summarize_store(store)
        self.assertTrue(report["reportable_store"])
        self.assertFalse(report["recognized_store"])
        with self.assertRaises(ValueError):
            report_common.require_recognized_store(report)

    def test_summary_only_store_is_rejected_by_suite_aggregation(self):
        store = self.root / "summary-only"
        (store / "mv-lifecycle").mkdir(parents=True)
        (store / "mv-lifecycle" / "summary.json").write_text(
            json.dumps({"valid?": True, "anomaly-count": 0, "warning-count": 0})
        )

        report = report_common.summarize_store(store)
        self.assertTrue(report["recognized_store"])
        with self.assertRaisesRegex(ValueError, "missing manifest workload/nemesis metadata"):
            report_common.aggregate_suite("suite", [store])

    def test_complete_store_is_aggregated(self):
        store = self.root / "complete"
        (store / "build").mkdir(parents=True)
        (store / "mv-lifecycle").mkdir(parents=True)
        (store / "build" / "manifest.json").write_text(
            json.dumps({"workload": "mv-lifecycle", "nemesis": "kill-db", "branch": "feature/x"})
        )
        (store / "mv-lifecycle" / "summary.json").write_text(
            json.dumps({"valid?": True, "anomaly-count": 0, "warning-count": 0})
        )

        summary = report_common.aggregate_suite("suite", [store])
        self.assertEqual(1, summary["run_count"])
        self.assertEqual(1, summary["valid_run_count"])
        self.assertEqual({"mv-lifecycle": 1}, summary["workload_counts"])
        self.assertEqual({"kill-db": 1}, summary["nemesis_counts"])

    def test_summary_keys_are_normalized_from_artifact_shape(self):
        store = self.root / "normalized"
        (store / "build").mkdir(parents=True)
        (store / "mv-lifecycle").mkdir(parents=True)
        (store / "build" / "manifest.json").write_text(
            json.dumps({"workload": "mv-lifecycle", "nemesis": "none"})
        )
        (store / "mv-lifecycle" / "summary.json").write_text(
            json.dumps({"valid?": False, "experimental?": False, "anomaly-count": 2, "warning-count": 1})
        )

        report = report_common.summarize_store(store)
        payload = report["workloads"]["mv-lifecycle"]
        self.assertFalse(payload["valid"])
        self.assertFalse(payload["experimental"])
        self.assertEqual(2, payload["anomaly_count"])
        self.assertEqual(1, payload["warning_count"])

    def test_suite_aggregation_uses_converged_valid_flag_for_autosched_gate(self):
        store = self.root / "autosched-converged"
        (store / "build").mkdir(parents=True)
        (store / "mv-autosched").mkdir(parents=True)
        (store / "build" / "manifest.json").write_text(
            json.dumps({"workload": "mv-autosched", "nemesis": "kill-kv"})
        )
        (store / "mv-autosched" / "summary.json").write_text(
            json.dumps(
                {
                    "valid?": True,
                    "strict-valid?": False,
                    "autosched-converged?": True,
                    "write-resolution-valid?": False,
                    "warning-count": 0,
                    "anomaly-count": 0,
                }
            )
        )

        summary = report_common.aggregate_suite("suite", [store])
        self.assertEqual(0, summary["invalid_run_count"])
        self.assertEqual(1, summary["valid_run_count"])
        self.assertTrue(summary["runs"][0]["valid"])

    def test_failed_store_without_workload_summaries_is_still_reportable(self):
        store = self.root / "failed-before-summary"
        (store / "node-0.example").mkdir(parents=True)
        (store / "jepsen.log").write_text(
            "\n".join(
                [
                    "2026-03-21 22:07:33,726 INFO tidb.db: node-0 TiDB install {:stage :plan, :tarball-url https://fileserver.example.invalid/tidb.tar.gz, :binary-override-count 1}",
                    "2026-03-21 22:07:40,101 INFO tidb.db: node-0 TiDB install {:stage :binary-override-start, :url https://fileserver.example.invalid/tikv.tar.gz, :dest /opt/tidb/bin}",
                ]
            )
            + "\n"
        )
        (store / "node-0.example" / "db.log").write_text(
            '[2026/03/21 14:21:07.721 +00:00] [INFO] [printer.go:52] ["Welcome to TiDB."] '
            '["Release Version"=v8.5.4] [Edition=Enterprise] '
            '["Git Commit Hash"=3f274db0eddcbbe89ba32449bb49dac70cd7043d] '
            '["Git Branch"=heads/refs/tags/v8.5.4] '
            '["UTC Build Time"="2026-03-11 07:03:02"] '
            '["Enterprise Extension Commit Hash"=7d43ff65ebc145bd63fa84cb368f8775be906998]\n'
        )

        report = report_common.summarize_store(store)
        self.assertTrue(report["reportable_store"])
        self.assertFalse(report["recognized_store"])
        self.assertEqual(
            "https://fileserver.example.invalid/tidb.tar.gz",
            report["build_inputs"]["tarball_url"],
        )
        self.assertEqual(
            ["https://fileserver.example.invalid/tikv.tar.gz"],
            report["build_inputs"]["binary_urls"],
        )
        self.assertEqual(
            "v8.5.4",
            report["runtime"]["uniform_tidb_fingerprint"]["release_version"],
        )


if __name__ == "__main__":
    unittest.main()
