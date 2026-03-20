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

    def test_suite_aggregation_still_uses_valid_flag_for_autosched_gate(self):
        store = self.root / "autosched-invalid"
        (store / "build").mkdir(parents=True)
        (store / "mv-autosched").mkdir(parents=True)
        (store / "build" / "manifest.json").write_text(
            json.dumps({"workload": "mv-autosched", "nemesis": "kill-kv"})
        )
        (store / "mv-autosched" / "summary.json").write_text(
            json.dumps(
                {
                    "valid?": False,
                    "strict-valid?": False,
                    "autosched-converged?": True,
                    "write-resolution-valid?": False,
                    "warning-count": 0,
                    "anomaly-count": 0,
                }
            )
        )

        summary = report_common.aggregate_suite("suite", [store])
        self.assertEqual(1, summary["invalid_run_count"])
        self.assertEqual(0, summary["valid_run_count"])
        self.assertFalse(summary["runs"][0]["valid"])


if __name__ == "__main__":
    unittest.main()
