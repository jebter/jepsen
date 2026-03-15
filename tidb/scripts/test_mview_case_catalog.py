import re
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import mview_case_catalog


class MViewCaseCatalogTest(unittest.TestCase):
    def test_branch_validation_suite_matches_smoke_matrix(self):
        cases = [
            (case["workload"], case["nemesis"], case["time_limit"])
            for case in mview_case_catalog.cases_for_suite("branch-validation")
        ]
        self.assertEqual(
            [
                ("mv-stateful", "none", 300),
                ("mv-stateful", "kill-db", 600),
                ("mv-stateful", "pause-db", 600),
                ("mv-stateful", "partition", 600),
                ("mv-lifecycle", "none", 300),
                ("mv-lifecycle", "kill-db", 600),
                ("mv-lifecycle", "pause-db", 600),
                ("mv-lifecycle", "partition", 600),
                ("mv-autosched", "none", 900),
                ("mv-autosched", "kill-db", 900),
                ("mv-autosched", "partition", 900),
            ],
            cases,
        )

    def test_full_single_fault_suite_expands_to_42_cases_in_batch_order(self):
        cases = mview_case_catalog.cases_for_suite("full-single-fault")
        self.assertEqual(42, len(cases))
        expected_nemesis_order = [
            "none",
            "kill-pd",
            "kill-kv",
            "kill-db",
            "stop-pd",
            "stop-kv",
            "stop-db",
            "pause-pd",
            "pause-kv",
            "pause-db",
            "partition",
            "shuffle-leader",
            "shuffle-region",
            "random-merge",
        ]
        self.assertEqual(
            ["mv-stateful"] * 14 + ["mv-lifecycle"] * 14 + ["mv-autosched"] * 14,
            [case["workload"] for case in cases],
        )
        for offset, workload in enumerate(["mv-stateful", "mv-lifecycle", "mv-autosched"]):
            batch = cases[offset * 14:(offset + 1) * 14]
            self.assertEqual([workload] * 14, [case["workload"] for case in batch])
            self.assertEqual(expected_nemesis_order, [case["nemesis"] for case in batch])

    def test_suite_cases_can_be_filtered_to_a_single_workload(self):
        cases = mview_case_catalog.cases_for_suite("full-single-fault", workload="mv-lifecycle")
        self.assertEqual(14, len(cases))
        self.assertEqual({"mv-lifecycle"}, {case["workload"] for case in cases})
        self.assertEqual(
            [
                "none",
                "kill-pd",
                "kill-kv",
                "kill-db",
                "stop-pd",
                "stop-kv",
                "stop-db",
                "pause-pd",
                "pause-kv",
                "pause-db",
                "partition",
                "shuffle-leader",
                "shuffle-region",
                "random-merge",
            ],
            [case["nemesis"] for case in cases],
        )

    def test_manual_only_case_stays_out_of_default_suites(self):
        manual = [
            (case["workload"], case["nemesis"], case["status"], case["time_limit"])
            for case in mview_case_catalog.manual_only_cases()
        ]
        self.assertEqual(
            [("mv-autosched-time", "clock-skew", "manual_only", 900)],
            manual,
        )
        branch_pairs = {(case["workload"], case["nemesis"]) for case in mview_case_catalog.cases_for_suite("branch-validation")}
        full_pairs = {(case["workload"], case["nemesis"]) for case in mview_case_catalog.cases_for_suite("full-single-fault")}
        self.assertNotIn(("mv-autosched-time", "clock-skew"), branch_pairs)
        self.assertNotIn(("mv-autosched-time", "clock-skew"), full_pairs)

    def test_active_faults_match_core_single_fault_capability(self):
        core = (ROOT / "src" / "tidb" / "core.clj").read_text()
        process_faults = self._parse_keyword_vector(core, "process-faults")
        network_faults = self._parse_keyword_vector(core, "network-faults")
        schedule_faults = self._parse_keyword_vector(core, "schedule-faults")
        expected_faults = {"none", *process_faults, *network_faults, *schedule_faults}
        active_faults = {case["nemesis"] for case in mview_case_catalog.active_mview_cases()}
        self.assertEqual(expected_faults, active_faults)

    @staticmethod
    def _parse_keyword_vector(text, name):
        match = re.search(rf"\(def {name}\s+.*?\[(.*?)\]\)", text, re.S)
        if match is None:
            raise AssertionError(f"unable to parse {name} from core.clj")
        return re.findall(r":([a-z0-9-]+)", match.group(1))


if __name__ == "__main__":
    unittest.main()
