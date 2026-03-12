import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import run_jepsen


class RunJepsenWorkloadOptionsTest(unittest.TestCase):
    def test_default_workload_options_include_mv_lifecycle(self):
        workloads = run_jepsen.workload_options()
        self.assertIn("mv-lifecycle", workloads)
        self.assertEqual([""], workloads["mv-lifecycle"])

    def test_pessimistic_workload_options_include_mv_lifecycle(self):
        workloads = run_jepsen.workload_options_for_pessimistic_txn()
        self.assertIn("mv-lifecycle", workloads)
        self.assertEqual([""], workloads["mv-lifecycle"])

    def test_mixed_workload_options_inherit_mv_lifecycle(self):
        workloads = run_jepsen.workload_options_for_mixed_txn()
        self.assertIn("mv-lifecycle", workloads)
        self.assertEqual([""], workloads["mv-lifecycle"])


if __name__ == "__main__":
    unittest.main()
