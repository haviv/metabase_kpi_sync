import unittest
from datetime import datetime

from export_ado import should_snapshot_work_items


class WorkItemSnapshotScheduleTests(unittest.TestCase):
    def test_runs_after_six_am_on_new_day(self):
        now = datetime(2026, 8, 24, 6, 10)
        last = datetime(2026, 8, 23, 14, 34)
        self.assertTrue(should_snapshot_work_items(last, now=now))

    def test_waits_before_six_am(self):
        now = datetime(2026, 8, 24, 5, 30)
        last = datetime(2026, 8, 23, 14, 34)
        self.assertFalse(should_snapshot_work_items(last, now=now))

    def test_skips_if_already_ran_today(self):
        now = datetime(2026, 8, 24, 7, 0)
        last = datetime(2026, 8, 24, 6, 5)
        self.assertFalse(should_snapshot_work_items(last, now=now))


if __name__ == '__main__':
    unittest.main()
