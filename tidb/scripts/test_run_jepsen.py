import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import mview_case_catalog
import run_jepsen


class RunJepsenWorkloadOptionsTest(unittest.TestCase):
    def test_default_workload_options_include_active_mview_workloads(self):
        workloads = run_jepsen.workload_options()
        self.assertEqual(
            mview_case_catalog.active_mview_workloads(),
            [name for name in workloads if name.startswith("mv-")],
        )
        self.assertNotIn("mv-autosched-time", workloads)

    def test_pessimistic_workload_options_include_active_mview_workloads(self):
        workloads = run_jepsen.workload_options_for_pessimistic_txn()
        self.assertEqual(
            mview_case_catalog.active_mview_workloads(),
            [name for name in workloads if name.startswith("mv-")],
        )

    def test_mixed_workload_options_inherit_active_mview_workloads(self):
        workloads = run_jepsen.workload_options_for_mixed_txn()
        self.assertEqual(
            mview_case_catalog.active_mview_workloads(),
            [name for name in workloads if name.startswith("mv-")],
        )

    def test_gen_tests_uses_catalog_cases_for_mview_workloads(self):
        tests = run_jepsen.gen_tests(
            version="nightly",
            tarball="http://example.invalid/tidb.tar.gz",
            time_limit=120,
            txn_mode="optimistic",
            follower_read=False,
        )
        self.assertTrue(any("--workload=mv-stateful" in test and "--nemesis=stop-db" in test and "--time-limit=600" in test for test in tests))
        self.assertTrue(any("--workload=mv-autosched" in test and "--nemesis=pause-pd" in test and "--time-limit=900" in test for test in tests))
        self.assertFalse(any("--workload=mv-autosched-time" in test for test in tests))

    def test_gen_tests_preserves_catalog_order_for_mview_cases(self):
        tests = run_jepsen.gen_tests(
            version="nightly",
            tarball="http://example.invalid/tidb.tar.gz",
            time_limit=120,
            txn_mode="optimistic",
            follower_read=False,
        )
        mview_tests = [test for test in tests if "--workload=mv-" in test]
        expected_pairs = [
            (case["workload"], case["nemesis"])
            for case in mview_case_catalog.active_mview_cases()
        ]
        self.assertEqual(
            expected_pairs,
            [self._extract_pair(test) for test in mview_tests],
        )

    @staticmethod
    def _extract_pair(test):
        workload = test.split("--workload=")[1].split(" ", 1)[0]
        nemesis = test.split("--nemesis=")[1].split(" ", 1)[0]
        return workload, nemesis


if __name__ == "__main__":
    unittest.main()
