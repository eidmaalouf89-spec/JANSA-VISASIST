"""
Module 7 — Integration Tests.

Runs M1→M2→M3 pipeline on real data, then tests full M7 batch workflow.
Skips if data/GrandFichier_1.xlsx is not present.
"""

import json
import os
import shutil
import tempfile
import unittest

# Skip if no real data
DATA_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "GrandFichier_1.xlsx",
)
M3_QUEUE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "output",
    "m3",
    "m3_priority_queue.json",
)

SKIP_REASON = "GrandFichier_1.xlsx not found — skipping integration tests"


def _has_data() -> bool:
    return os.path.exists(DATA_FILE)


def _has_m3_queue() -> bool:
    return os.path.exists(M3_QUEUE_FILE)


@unittest.skipUnless(_has_m3_queue(), SKIP_REASON)
class TestM7Integration(unittest.TestCase):
    """Integration tests using real M3 queue output."""

    @classmethod
    def setUpClass(cls):
        """Load M3 queue data once."""
        with open(M3_QUEUE_FILE, "r", encoding="utf-8") as f:
            cls.m3_queue = json.load(f)

        cls.m1_metadata = {
            "source_file": "GrandFichier_1.xlsx",
            "total_rows": len(cls.m3_queue),
            "total_sheets": len(set(r.get("source_sheet", "") for r in cls.m3_queue)),
            "sheet_names": sorted(set(r.get("source_sheet", "") for r in cls.m3_queue)),
            "row_ids": sorted(r["row_id"] for r in cls.m3_queue),
        }

    def setUp(self):
        """Set up temp working directory for each test."""
        self._orig_dir = os.path.abspath(os.curdir)
        self._tmp_dir = tempfile.mkdtemp()
        os.chdir(self._tmp_dir)

    def tearDown(self):
        """Clean up."""
        os.chdir(self._orig_dir)
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def _import_modules(self):
        """Import M7 modules (deferred to avoid import errors during collection)."""
        from jansa_visasist.config_m7 import SESSION_SCHEMA_VERSION, CATEGORY_SORT_ORDER
        from jansa_visasist.pipeline.m7 import lifecycle, session_store
        from jansa_visasist.pipeline.m7 import decisions as decisions_mod
        from jansa_visasist.pipeline.m7.signature import compute_dataset_signature
        return lifecycle, session_store, decisions_mod, compute_dataset_signature, SESSION_SCHEMA_VERSION, CATEGORY_SORT_ORDER

    def test_create_session_correct_item_count(self):
        """Create session from real M3 queue → correct item count."""
        lifecycle, _, _, _, _, _ = self._import_modules()
        result = lifecycle.create_session(
            self.m3_queue, self.m1_metadata, "integration-run-001"
        )
        self.assertEqual(result.status, "OK")
        self.assertEqual(result.data.total_items, len(self.m3_queue))

    def test_w2_ordering(self):
        """W2 ordering: first item is from highest-priority category present."""
        lifecycle, _, _, _, _, CATEGORY_SORT_ORDER = self._import_modules()
        result = lifecycle.create_session(
            self.m3_queue, self.m1_metadata, "integration-run-002"
        )
        self.assertEqual(result.status, "OK")
        first_item = result.data.items[0]
        # The first item should be from the highest-priority category present
        present_categories = set(r.get("category") for r in self.m3_queue)
        expected_first = None
        for cat in CATEGORY_SORT_ORDER:
            if cat in present_categories:
                expected_first = cat
                break
        if expected_first:
            self.assertEqual(first_item.category, expected_first)

    def test_record_decisions_counters(self):
        """Record 3 decisions → counters correct."""
        lifecycle, session_store, decisions_mod, _, SESSION_SCHEMA_VERSION, _ = self._import_modules()

        r1 = lifecycle.create_session(
            self.m3_queue, self.m1_metadata, "integration-run-003"
        )
        session = r1.data
        sig = session.dataset_signature

        r2 = lifecycle.open_session(
            session.session_id, sig, "integration-run-003", SESSION_SCHEMA_VERSION
        )
        session = r2.data

        # Record up to 3 decisions (or all if fewer items)
        count = min(3, len(session.items))
        for i in range(count):
            item = session.items[i]
            if i == 0:
                decisions_mod.record_decision(session, item.row_id, "VISA_ISSUED", "VSO")
            elif i == 1:
                decisions_mod.record_decision(session, item.row_id, "DEFERRED")
            else:
                decisions_mod.record_decision(session, item.row_id, "SKIPPED")
            session = session_store.load_session(session.session_id)

        self.assertEqual(session.decided_count, count)
        if count >= 2:
            self.assertEqual(session.deferred_count, 1)
        if count >= 3:
            self.assertEqual(session.skipped_count, 1)

    def test_duplicate_decision_already_decided(self):
        """Duplicate decision → ALREADY_DECIDED."""
        lifecycle, session_store, decisions_mod, _, SESSION_SCHEMA_VERSION, _ = self._import_modules()

        r1 = lifecycle.create_session(
            self.m3_queue, self.m1_metadata, "integration-run-004"
        )
        session = r1.data
        sig = session.dataset_signature

        r2 = lifecycle.open_session(
            session.session_id, sig, "integration-run-004", SESSION_SCHEMA_VERSION
        )
        session = r2.data

        item = session.items[0]
        r3 = decisions_mod.record_decision(session, item.row_id, "VISA_ISSUED", "VSO")
        self.assertEqual(r3.status, "OK")

        session = session_store.load_session(session.session_id)
        r4 = decisions_mod.record_decision(session, item.row_id, "VISA_ISSUED", "VAO")
        self.assertEqual(r4.status, "ALREADY_DECIDED")

    def test_complete_session_report(self):
        """Complete session → report generated with correct totals."""
        lifecycle, session_store, decisions_mod, _, SESSION_SCHEMA_VERSION, _ = self._import_modules()

        r1 = lifecycle.create_session(
            self.m3_queue, self.m1_metadata, "integration-run-005"
        )
        session = r1.data
        sig = session.dataset_signature

        r2 = lifecycle.open_session(
            session.session_id, sig, "integration-run-005", SESSION_SCHEMA_VERSION
        )
        session = r2.data

        # Decide first item
        if session.items:
            decisions_mod.record_decision(
                session, session.items[0].row_id, "VISA_ISSUED", "VSO"
            )
            session = session_store.load_session(session.session_id)

        r3 = lifecycle.complete_session(session)
        self.assertEqual(r3.status, "OK")
        report = r3.data
        self.assertEqual(report.total_items, len(self.m3_queue))
        self.assertEqual(report.decided_count, len(self.m3_queue))

    def test_open_completed_session_errors(self):
        """Open completed session → ERROR (terminal)."""
        lifecycle, session_store, _, _, SESSION_SCHEMA_VERSION, _ = self._import_modules()

        r1 = lifecycle.create_session(
            self.m3_queue, self.m1_metadata, "integration-run-006"
        )
        session = r1.data
        sig = session.dataset_signature

        lifecycle.open_session(
            session.session_id, sig, "integration-run-006", SESSION_SCHEMA_VERSION
        )
        session = session_store.load_session(session.session_id)
        lifecycle.complete_session(session)

        r3 = lifecycle.open_session(
            session.session_id, sig, "integration-run-006", SESSION_SCHEMA_VERSION
        )
        self.assertEqual(r3.status, "ERROR")
        self.assertEqual(r3.error_code, "session_terminal")

    def test_signature_stability(self):
        """Signature stability: compute twice → identical."""
        _, _, _, compute_dataset_signature, _, _ = self._import_modules()
        sig1 = compute_dataset_signature(
            self.m1_metadata["source_file"],
            self.m1_metadata["total_rows"],
            self.m1_metadata["total_sheets"],
            self.m1_metadata["sheet_names"],
            self.m1_metadata["row_ids"],
        )
        sig2 = compute_dataset_signature(
            self.m1_metadata["source_file"],
            self.m1_metadata["total_rows"],
            self.m1_metadata["total_sheets"],
            self.m1_metadata["sheet_names"],
            self.m1_metadata["row_ids"],
        )
        self.assertEqual(sig1, sig2)

    def test_full_lifecycle(self):
        """Full lifecycle: create → open → decide all → complete → report has all decisions."""
        lifecycle, session_store, decisions_mod, _, SESSION_SCHEMA_VERSION, _ = self._import_modules()

        r1 = lifecycle.create_session(
            self.m3_queue, self.m1_metadata, "integration-run-007"
        )
        session = r1.data
        sig = session.dataset_signature

        r2 = lifecycle.open_session(
            session.session_id, sig, "integration-run-007", SESSION_SCHEMA_VERSION
        )
        session = r2.data

        # Decide all items
        for item in session.items:
            decisions_mod.record_decision(
                session, item.row_id, "VISA_ISSUED", "VSO", "Integration test"
            )
            session = session_store.load_session(session.session_id)

        r3 = lifecycle.complete_session(session)
        self.assertEqual(r3.status, "OK")
        report = r3.data
        self.assertEqual(report.total_items, len(self.m3_queue))
        self.assertEqual(report.decided_count, len(self.m3_queue))
        self.assertEqual(report.skipped_count, 0)
        self.assertEqual(len(report.decisions), len(self.m3_queue))
        # All decisions should be VISA_ISSUED
        for d in report.decisions:
            self.assertEqual(d["decision_type"], "VISA_ISSUED")
            self.assertEqual(d["visa_value"], "VSO")


if __name__ == "__main__":
    unittest.main()
