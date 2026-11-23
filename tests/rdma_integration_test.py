#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Device-free checks that an opt-in integration run cannot silently skip."""

from pathlib import Path
import tempfile
import unittest

from rdma_integration import require_passes


class ReportTests(unittest.TestCase):
    def check_report(self, content):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "junit.xml"
            report.write_text(content)
            return require_passes(report)

    def test_ctest_success(self):
        self.assertEqual(self.check_report(
            '<testsuite><testcase name="registration" status="run"/>'
            '<testcase name="transfer" status="run"/></testsuite>'), 2)

    def test_skipped_or_failed_cases_are_not_success(self):
        for child in ('<skipped message="no device"/>', '<failure/>', '<error/>'):
            with self.subTest(child=child), self.assertRaises(RuntimeError):
                self.check_report(f'<testsuite><testcase name="rdma">{child}'
                                  '</testcase></testsuite>')
        with self.assertRaises(RuntimeError):
            self.check_report('<testsuite><testcase status="notrun"/></testsuite>')

    def test_empty_selection_is_not_success(self):
        with self.assertRaises(RuntimeError):
            self.check_report('<testsuite tests="0"/>')


if __name__ == "__main__":
    unittest.main()
