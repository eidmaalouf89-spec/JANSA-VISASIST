"""
Module 7 — Unit Tests.

Tests for schemas, signature, validation, lifecycle, decisions, reporting,
session_store, and exporter.
"""

import csv
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from jansa_visasist.config_m7 import (
    CATEGORY_SORT_ORDER,
    SESSION_SCHEMA_VERSION,
    VALID_DECISIONS,
    VALID_VISA_VALUES,
)
from jansa_visasist.pipeline.m7.schemas import (
    BatchDecision,
    BatchItem,
    BatchSession,
    OperationResult,
    SessionReport,
)
from jansa_visasist.pipeline.m7.signature import compute_dataset_signature
from jansa_visasist.pipeline.m7 import validation
from jansa_visasist.pipeline.m7 import session_store
from jansa_visasist.pipeline.m7 import lifecycle
from jansa_visasist.pipeline.m7 import decisions as decisions_mod
from jansa_visasist.pipeline.m7 import reporting
from jansa_visasist.pipeline.m7 import exporter


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────


def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _make_item(
    row_id: str = "0_1",
    category: str = "EASY_WIN_APPROVE",
    priority_score: float = 50.0,
    order_index: int = 0,
    decision: BatchDecision = None,
) -> BatchItem:
    return BatchItem(
        row_id=row_id,
        document=f"DOC_{row_id}",
        titre=f"Title {row_id}",
        source_sheet="LOT_A",
        category=category,
        priority_score=priority_score,
        consensus_type="ALL_APPROVE",
        is_overdue=False,
        decision=decision,
        order_index=order_index,
    )


def _make_session(
    session_id: str = "test-session-001",
    status: str = "IN_PROGRESS",
    items: list = None,
    decided_count: int = 0,
    deferred_count: int = 0,
    skipped_count: int = 0,
) -> BatchSession:
    now = _utcnow_iso()
    if items is None:
        items = [_make_item("0_1", order_index=0), _make_item("0_2", order_index=1)]
    return BatchSession(
        session_id=session_id,
        batch_id="batch-001",
        status=status,
        session_schema_version=SESSION_SCHEMA_VERSION,
        dataset_signature="abc123" * 10 + "abcd",
        pipeline_run_id="run-001",
        user_id=None,
        created_at=now,
        updated_at=now,
        completed_at=None,
        invalidated_at=None,
        invalidated_reason=None,
        items=items,
        current_index=0,
        filter_params=None,
        total_items=len(items),
        decided_count=decided_count,
        deferred_count=deferred_count,
        skipped_count=skipped_count,
    )


def _make_decision(
    decision_type: str = "VISA_ISSUED",
    visa_value: str = "VSO",
    decision_source: str = "manual",
) -> BatchDecision:
    return BatchDecision(
        decision_type=decision_type,
        visa_value=visa_value,
        comment="Test",
        decided_at=_utcnow_iso(),
        suggested_action=None,
        proposed_visa=None,
        decision_source=decision_source,
    )


# ──────────────────────────────────────────────
# Schema Tests (5+)
# ──────────────────────────────────────────────


class TestSchemas(unittest.TestCase):
    """Tests for M7 dataclass schemas."""

    def test_batch_session_roundtrip(self):
        """BatchSession serialization roundtrip (to_dict -> from_dict)."""
        session = _make_session()
        d = session.to_dict()
        restored = BatchSession.from_dict(d)
        self.assertEqual(restored.session_id, session.session_id)
        self.assertEqual(restored.status, session.status)
        self.assertEqual(restored.total_items, session.total_items)
        self.assertEqual(len(restored.items), len(session.items))
        self.assertEqual(restored.items[0].row_id, session.items[0].row_id)

    def test_batch_decision_validates_decision_type(self):
        """BatchDecision validates decision_type enum."""
        with self.assertRaises(ValueError):
            BatchDecision(
                decision_type="INVALID",
                visa_value=None,
                comment=None,
                decided_at=_utcnow_iso(),
                suggested_action=None,
                proposed_visa=None,
                decision_source="manual",
            )

    def test_batch_decision_rejects_invalid_visa_value(self):
        """BatchDecision rejects invalid visa_value for VISA_ISSUED."""
        with self.assertRaises(ValueError):
            BatchDecision(
                decision_type="VISA_ISSUED",
                visa_value="INVALID",
                comment=None,
                decided_at=_utcnow_iso(),
                suggested_action=None,
                proposed_visa=None,
                decision_source="manual",
            )

    def test_batch_decision_rejects_visa_value_for_non_visa_issued(self):
        """BatchDecision rejects visa_value when not VISA_ISSUED."""
        with self.assertRaises(ValueError):
            BatchDecision(
                decision_type="DEFERRED",
                visa_value="VSO",
                comment=None,
                decided_at=_utcnow_iso(),
                suggested_action=None,
                proposed_visa=None,
                decision_source="manual",
            )

    def test_operation_result_structure(self):
        """OperationResult structure."""
        r = OperationResult(status="OK", error_code=None, message="Success", data=42)
        d = r.to_dict()
        self.assertEqual(d["status"], "OK")
        self.assertIsNone(d["error_code"])
        self.assertEqual(d["message"], "Success")
        self.assertEqual(d["data"], 42)

    def test_batch_item_snapshot_fields_preserved(self):
        """BatchItem snapshot fields preserved through serialization."""
        item = _make_item(category="BLOCKED", priority_score=75.5)
        d = item.to_dict()
        restored = BatchItem.from_dict(d)
        self.assertEqual(restored.category, "BLOCKED")
        self.assertAlmostEqual(restored.priority_score, 75.5)
        self.assertEqual(restored.consensus_type, "ALL_APPROVE")
        self.assertEqual(restored.is_overdue, False)

    def test_batch_session_with_decisions_roundtrip(self):
        """Session with decisions survives roundtrip."""
        decision = _make_decision("VISA_ISSUED", "VAO")
        item = _make_item(decision=decision)
        session = _make_session(items=[item], decided_count=1)
        d = session.to_dict()
        restored = BatchSession.from_dict(d)
        self.assertIsNotNone(restored.items[0].decision)
        self.assertEqual(restored.items[0].decision.decision_type, "VISA_ISSUED")
        self.assertEqual(restored.items[0].decision.visa_value, "VAO")


# ──────────────────────────────────────────────
# Signature Tests (3+)
# ──────────────────────────────────────────────


class TestSignature(unittest.TestCase):
    """Tests for dataset signature computation."""

    def test_same_input_same_signature(self):
        """Same input → same signature."""
        sig1 = compute_dataset_signature("f.xlsx", 100, 5, ["A", "B"], ["1", "2"])
        sig2 = compute_dataset_signature("f.xlsx", 100, 5, ["A", "B"], ["1", "2"])
        self.assertEqual(sig1, sig2)

    def test_different_input_different_signature(self):
        """Different input → different signature."""
        sig1 = compute_dataset_signature("f.xlsx", 100, 5, ["A", "B"], ["1", "2"])
        sig2 = compute_dataset_signature("g.xlsx", 100, 5, ["A", "B"], ["1", "2"])
        self.assertNotEqual(sig1, sig2)

    def test_sorted_row_ids_stable(self):
        """Sorted row_ids produce stable hash regardless of input order."""
        sig1 = compute_dataset_signature("f.xlsx", 10, 2, ["B", "A"], ["2", "1", "3"])
        sig2 = compute_dataset_signature("f.xlsx", 10, 2, ["A", "B"], ["3", "1", "2"])
        self.assertEqual(sig1, sig2)

    def test_signature_is_64_char_hex(self):
        """Signature is lowercase hex, 64 chars."""
        sig = compute_dataset_signature("f.xlsx", 1, 1, ["A"], ["1"])
        self.assertEqual(len(sig), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in sig))


# ──────────────────────────────────────────────
# Validation Tests (6+)
# ──────────────────────────────────────────────


class TestValidation(unittest.TestCase):
    """Tests for validation logic."""

    def test_vr1_schema_version_mismatch(self):
        """VR1: schema version mismatch → invalid."""
        session = _make_session()
        session.session_schema_version = "0.0.0"
        is_valid, reason = validation.validate_dataset_freshness(
            session, session.dataset_signature, session.pipeline_run_id, SESSION_SCHEMA_VERSION
        )
        self.assertFalse(is_valid)
        self.assertEqual(reason, "schema_version_mismatch")

    def test_vr2_dataset_signature_mismatch(self):
        """VR2: dataset signature mismatch → invalid."""
        session = _make_session()
        is_valid, reason = validation.validate_dataset_freshness(
            session, "different_sig", session.pipeline_run_id, SESSION_SCHEMA_VERSION
        )
        self.assertFalse(is_valid)
        self.assertEqual(reason, "dataset_signature_mismatch")

    def test_vr3_pipeline_run_id_mismatch(self):
        """VR3: pipeline_run_id mismatch → invalid."""
        session = _make_session()
        is_valid, reason = validation.validate_dataset_freshness(
            session, session.dataset_signature, "different-run", SESSION_SCHEMA_VERSION
        )
        self.assertFalse(is_valid)
        self.assertEqual(reason, "pipeline_run_id_mismatch")

    def test_vr4_all_match(self):
        """VR4: all match → valid."""
        session = _make_session()
        is_valid, reason = validation.validate_dataset_freshness(
            session, session.dataset_signature, session.pipeline_run_id, SESSION_SCHEMA_VERSION
        )
        self.assertTrue(is_valid)
        self.assertIsNone(reason)

    def test_recompute_current_index_finds_first_undecided(self):
        """recompute_current_index: finds first undecided."""
        items = [
            _make_item("0_1", order_index=0, decision=_make_decision()),
            _make_item("0_2", order_index=1),
            _make_item("0_3", order_index=2),
        ]
        idx = validation.recompute_current_index(items)
        self.assertEqual(idx, 1)

    def test_recompute_current_index_all_decided(self):
        """recompute_current_index: all decided → returns len(items)."""
        items = [
            _make_item("0_1", order_index=0, decision=_make_decision()),
            _make_item("0_2", order_index=1, decision=_make_decision()),
        ]
        idx = validation.recompute_current_index(items)
        self.assertEqual(idx, 2)

    def test_state_transition_allowed(self):
        """Allowed transitions pass."""
        self.assertTrue(validation.validate_state_transition("CREATED", "IN_PROGRESS"))
        self.assertTrue(validation.validate_state_transition("CREATED", "INVALIDATED"))
        self.assertTrue(validation.validate_state_transition("IN_PROGRESS", "COMPLETED"))
        self.assertTrue(validation.validate_state_transition("IN_PROGRESS", "INVALIDATED"))

    def test_state_transition_forbidden(self):
        """Forbidden transitions fail."""
        self.assertFalse(validation.validate_state_transition("COMPLETED", "IN_PROGRESS"))
        self.assertFalse(validation.validate_state_transition("INVALIDATED", "CREATED"))
        self.assertFalse(validation.validate_state_transition("CREATED", "COMPLETED"))

    def test_validate_decision_integrity(self):
        """Decision integrity check catches counter mismatch."""
        session = _make_session(decided_count=5)  # Incorrect
        errors = validation.validate_decision_integrity(session)
        self.assertTrue(len(errors) > 0)
        self.assertIn("decided_count mismatch", errors[0])


# ──────────────────────────────────────────────
# Lifecycle Tests (5+)
# ──────────────────────────────────────────────


class TestLifecycle(unittest.TestCase):
    """Tests for session lifecycle management."""

    def setUp(self):
        """Set up temp storage directory."""
        self._orig_dir = os.path.abspath(os.curdir)
        self._tmp_dir = tempfile.mkdtemp()
        os.chdir(self._tmp_dir)

    def tearDown(self):
        """Clean up."""
        os.chdir(self._orig_dir)
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def _make_queue_data(self, count: int = 3) -> list:
        """Create mock M3 queue data."""
        items = []
        categories = CATEGORY_SORT_ORDER
        for i in range(count):
            items.append({
                "row_id": f"0_{i}",
                "document": f"DOC_{i}",
                "titre": f"Title {i}",
                "source_sheet": "LOT_A",
                "category": categories[i % len(categories)],
                "priority_score": 100.0 - i * 10,
                "consensus_type": "ALL_APPROVE",
                "is_overdue": i % 2 == 0,
            })
        return items

    def _make_metadata(self, queue_data: list) -> dict:
        return {
            "source_file": "test.xlsx",
            "total_rows": len(queue_data),
            "total_sheets": 1,
            "sheet_names": ["LOT_A"],
            "row_ids": [r["row_id"] for r in queue_data],
        }

    def test_create_session_produces_created(self):
        """create_session: produces CREATED session with correct item count."""
        queue = self._make_queue_data(5)
        meta = self._make_metadata(queue)
        result = lifecycle.create_session(queue, meta, "run-001")
        self.assertEqual(result.status, "OK")
        self.assertEqual(result.data.status, "CREATED")
        self.assertEqual(result.data.total_items, 5)
        self.assertEqual(len(result.data.items), 5)

    def test_create_session_blocks_if_active(self):
        """create_session: blocks if active session exists."""
        queue = self._make_queue_data(2)
        meta = self._make_metadata(queue)
        r1 = lifecycle.create_session(queue, meta, "run-001")
        self.assertEqual(r1.status, "OK")

        r2 = lifecycle.create_session(queue, meta, "run-002")
        self.assertEqual(r2.status, "ERROR")
        self.assertEqual(r2.error_code, "active_session_exists")

    def test_open_session_transitions_to_in_progress(self):
        """open_session: CREATED → IN_PROGRESS."""
        queue = self._make_queue_data(2)
        meta = self._make_metadata(queue)
        r1 = lifecycle.create_session(queue, meta, "run-001")
        session = r1.data
        sig = session.dataset_signature

        r2 = lifecycle.open_session(
            session.session_id, sig, "run-001", SESSION_SCHEMA_VERSION
        )
        self.assertEqual(r2.status, "OK")
        self.assertEqual(r2.data.status, "IN_PROGRESS")

    def test_open_session_stale_signature_invalidates(self):
        """open_session: stale signature → INVALIDATED."""
        queue = self._make_queue_data(2)
        meta = self._make_metadata(queue)
        r1 = lifecycle.create_session(queue, meta, "run-001")
        session = r1.data

        r2 = lifecycle.open_session(
            session.session_id, "stale_signature", "run-001", SESSION_SCHEMA_VERSION
        )
        self.assertEqual(r2.status, "ERROR")
        self.assertEqual(r2.error_code, "session_invalidated")
        self.assertEqual(r2.data.status, "INVALIDATED")
        self.assertIsNotNone(r2.data.invalidated_at)

    def test_complete_session_auto_skips_undecided(self):
        """complete_session: undecided items auto-skipped [FIX C]."""
        queue = self._make_queue_data(3)
        meta = self._make_metadata(queue)
        r1 = lifecycle.create_session(queue, meta, "run-001")
        session = r1.data
        sig = session.dataset_signature

        r2 = lifecycle.open_session(
            session.session_id, sig, "run-001", SESSION_SCHEMA_VERSION
        )
        session = r2.data

        # Decide only the first item
        decisions_mod.record_decision(
            session, session.items[0].row_id, "VISA_ISSUED", "VSO"
        )
        # Reload after decision
        session = session_store.load_session(session.session_id)

        r3 = lifecycle.complete_session(session)
        self.assertEqual(r3.status, "OK")
        report = r3.data
        # 2 items should be auto-skipped
        self.assertEqual(report.skipped_count, 2)
        self.assertEqual(report.decided_count, 3)

    def test_create_session_w2_ordering(self):
        """create_session: items sorted by W2 ordering."""
        queue = [
            {"row_id": "a", "document": "D1", "titre": "T1", "source_sheet": "L",
             "category": "NOT_STARTED", "priority_score": 90.0,
             "consensus_type": "NOT_STARTED", "is_overdue": False},
            {"row_id": "b", "document": "D2", "titre": "T2", "source_sheet": "L",
             "category": "EASY_WIN_APPROVE", "priority_score": 50.0,
             "consensus_type": "ALL_APPROVE", "is_overdue": True},
        ]
        meta = self._make_metadata(queue)
        r = lifecycle.create_session(queue, meta, "run-001")
        self.assertEqual(r.status, "OK")
        # EASY_WIN_APPROVE should come first (index 0 in CATEGORY_SORT_ORDER)
        self.assertEqual(r.data.items[0].category, "EASY_WIN_APPROVE")

    def test_open_session_terminal_rejected(self):
        """open_session: terminal session → ERROR."""
        queue = self._make_queue_data(1)
        meta = self._make_metadata(queue)
        r1 = lifecycle.create_session(queue, meta, "run-001")
        session = r1.data
        sig = session.dataset_signature

        # Open and complete
        lifecycle.open_session(session.session_id, sig, "run-001", SESSION_SCHEMA_VERSION)
        session = session_store.load_session(session.session_id)
        lifecycle.complete_session(session)

        # Try to open again
        r3 = lifecycle.open_session(
            session.session_id, sig, "run-001", SESSION_SCHEMA_VERSION
        )
        self.assertEqual(r3.status, "ERROR")
        self.assertEqual(r3.error_code, "session_terminal")


# ──────────────────────────────────────────────
# Decision Tests (5+)
# ──────────────────────────────────────────────


class TestDecisions(unittest.TestCase):
    """Tests for decision recording."""

    def setUp(self):
        self._orig_dir = os.path.abspath(os.curdir)
        self._tmp_dir = tempfile.mkdtemp()
        os.chdir(self._tmp_dir)
        session_store.ensure_storage_dir()

    def tearDown(self):
        os.chdir(self._orig_dir)
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def _create_in_progress_session(self) -> BatchSession:
        """Create and open a session for testing decisions."""
        session = _make_session(status="IN_PROGRESS")
        session_store.save_session(session)
        return session

    def test_visa_issued_with_valid_value(self):
        """record_decision: VISA_ISSUED with valid visa_value → OK."""
        session = self._create_in_progress_session()
        r = decisions_mod.record_decision(session, "0_1", "VISA_ISSUED", "VSO")
        self.assertEqual(r.status, "OK")

    def test_visa_issued_without_value_errors(self):
        """record_decision: VISA_ISSUED without visa_value → ERROR."""
        session = self._create_in_progress_session()
        r = decisions_mod.record_decision(session, "0_1", "VISA_ISSUED", None)
        self.assertEqual(r.status, "ERROR")
        self.assertEqual(r.error_code, "invalid_visa_value")

    def test_duplicate_decision_returns_already_decided(self):
        """record_decision: duplicate row_id → ALREADY_DECIDED [FIX D]."""
        session = self._create_in_progress_session()
        r1 = decisions_mod.record_decision(session, "0_1", "VISA_ISSUED", "VSO")
        self.assertEqual(r1.status, "OK")
        # Reload
        session = session_store.load_session(session.session_id)
        r2 = decisions_mod.record_decision(session, "0_1", "VISA_ISSUED", "VAO")
        self.assertEqual(r2.status, "ALREADY_DECIDED")

    def test_decision_increments_counters(self):
        """record_decision: increments correct counters."""
        session = self._create_in_progress_session()
        decisions_mod.record_decision(session, "0_1", "VISA_ISSUED", "VSO")
        # Reload to check persisted state
        session = session_store.load_session(session.session_id)
        self.assertEqual(session.decided_count, 1)
        self.assertEqual(session.deferred_count, 0)

        decisions_mod.record_decision(session, "0_2", "DEFERRED")
        session = session_store.load_session(session.session_id)
        self.assertEqual(session.decided_count, 2)
        self.assertEqual(session.deferred_count, 1)

    def test_decision_on_non_in_progress_errors(self):
        """record_decision: session not IN_PROGRESS → ERROR."""
        session = _make_session(status="CREATED")
        session_store.save_session(session)
        r = decisions_mod.record_decision(session, "0_1", "VISA_ISSUED", "VSO")
        self.assertEqual(r.status, "ERROR")
        self.assertEqual(r.error_code, "invalid_status")

    def test_decision_item_not_found(self):
        """record_decision: item not found → ERROR."""
        session = self._create_in_progress_session()
        r = decisions_mod.record_decision(session, "nonexistent", "VISA_ISSUED", "VSO")
        self.assertEqual(r.status, "ERROR")
        self.assertEqual(r.error_code, "item_not_found")

    def test_visa_value_not_allowed_for_deferred(self):
        """record_decision: visa_value not allowed for DEFERRED."""
        session = self._create_in_progress_session()
        r = decisions_mod.record_decision(session, "0_1", "DEFERRED", "VSO")
        self.assertEqual(r.status, "ERROR")
        self.assertEqual(r.error_code, "visa_value_not_allowed")


# ──────────────────────────────────────────────
# Reporting Tests (3+)
# ──────────────────────────────────────────────


class TestReporting(unittest.TestCase):
    """Tests for report generation."""

    def _make_completed_session(self) -> BatchSession:
        """Create a completed session for reporting."""
        now = _utcnow_iso()
        items = [
            _make_item("0_1", category="EASY_WIN_APPROVE", order_index=0,
                       decision=_make_decision("VISA_ISSUED", "VSO")),
            _make_item("0_2", category="EASY_WIN_APPROVE", order_index=1,
                       decision=_make_decision("VISA_ISSUED", "VAO", "assisted")),
            _make_item("0_3", category="BLOCKED", order_index=2,
                       decision=BatchDecision(
                           decision_type="DEFERRED", visa_value=None, comment="Wait",
                           decided_at=now, suggested_action=None, proposed_visa=None,
                           decision_source="manual")),
        ]
        session = _make_session(
            status="COMPLETED",
            items=items,
            decided_count=3,
            deferred_count=1,
            skipped_count=0,
        )
        session.completed_at = now
        return session

    def test_correct_visa_breakdown(self):
        """generate_report: correct visa_breakdown."""
        session = self._make_completed_session()
        report = reporting.generate_report(session)
        self.assertEqual(report.visa_breakdown.get("VSO", 0), 1)
        self.assertEqual(report.visa_breakdown.get("VAO", 0), 1)
        self.assertNotIn("REF", report.visa_breakdown)

    def test_correct_category_breakdown(self):
        """generate_report: correct category_breakdown."""
        session = self._make_completed_session()
        report = reporting.generate_report(session)
        ewb = report.category_breakdown.get("EASY_WIN_APPROVE", {})
        self.assertEqual(ewb.get("total", 0), 2)
        self.assertEqual(ewb.get("decided", 0), 2)
        blk = report.category_breakdown.get("BLOCKED", {})
        self.assertEqual(blk.get("total", 0), 1)
        self.assertEqual(blk.get("deferred", 0), 1)

    def test_correct_decision_source_breakdown(self):
        """generate_report: correct decision_source_breakdown."""
        session = self._make_completed_session()
        report = reporting.generate_report(session)
        self.assertEqual(report.decision_source_breakdown.get("manual", 0), 2)
        self.assertEqual(report.decision_source_breakdown.get("assisted", 0), 1)

    def test_report_duration_computed(self):
        """generate_report: duration_seconds is non-negative."""
        session = self._make_completed_session()
        report = reporting.generate_report(session)
        self.assertGreaterEqual(report.duration_seconds, 0)


# ──────────────────────────────────────────────
# Exporter Tests
# ──────────────────────────────────────────────


class TestExporter(unittest.TestCase):
    """Tests for report export."""

    def setUp(self):
        self._tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def _make_report(self) -> SessionReport:
        now = _utcnow_iso()
        return SessionReport(
            session_id="sess-001",
            batch_id="batch-001",
            created_at=now,
            completed_at=now,
            duration_seconds=120,
            dataset_signature="abc",
            pipeline_run_id="run-001",
            invalidated_reason=None,
            total_items=2,
            decided_count=2,
            deferred_count=0,
            skipped_count=0,
            visa_breakdown={"VSO": 1, "VAO": 1},
            category_breakdown={"EASY_WIN_APPROVE": {"total": 2, "decided": 2, "deferred": 0, "skipped": 0}},
            decision_source_breakdown={"manual": 2, "assisted": 0},
            decisions=[
                {"row_id": "0_1", "document": "D1", "source_sheet": "L",
                 "category": "EASY_WIN_APPROVE", "decision_type": "VISA_ISSUED",
                 "visa_value": "VSO", "comment": None, "decided_at": now,
                 "decision_source": "manual", "suggested_action": None, "proposed_visa": None},
                {"row_id": "0_2", "document": "D2", "source_sheet": "L",
                 "category": "EASY_WIN_APPROVE", "decision_type": "VISA_ISSUED",
                 "visa_value": "VAO", "comment": None, "decided_at": now,
                 "decision_source": "manual", "suggested_action": None, "proposed_visa": None},
            ],
        )

    def test_export_csv_produces_valid_file(self):
        """export_csv: produces valid CSV file."""
        report = self._make_report()
        path = exporter.export_csv(report, self._tmp_dir)
        self.assertTrue(os.path.exists(path))
        with open(path, "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
        self.assertEqual(len(rows), 3)  # header + 2 data rows
        self.assertEqual(rows[0][0], "row_id")

    def test_export_json_produces_valid_file(self):
        """export_json: produces valid JSON file."""
        report = self._make_report()
        path = exporter.export_json(report, self._tmp_dir)
        self.assertTrue(os.path.exists(path))
        with open(path, "r") as f:
            data = json.load(f)
        self.assertEqual(data["session_id"], "sess-001")
        self.assertEqual(data["total_items"], 2)


# ──────────────────────────────────────────────
# Session Store Tests (3+)
# ──────────────────────────────────────────────


class TestSessionStore(unittest.TestCase):
    """Tests for session persistence."""

    def setUp(self):
        self._orig_dir = os.path.abspath(os.curdir)
        self._tmp_dir = tempfile.mkdtemp()
        os.chdir(self._tmp_dir)

    def tearDown(self):
        os.chdir(self._orig_dir)
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def test_save_load_roundtrip(self):
        """save_session + load_session roundtrip."""
        session = _make_session(session_id="store-test-001")
        session_store.save_session(session)
        loaded = session_store.load_session("store-test-001")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.session_id, "store-test-001")
        self.assertEqual(loaded.total_items, session.total_items)

    def test_find_active_returns_in_progress(self):
        """find_active_session: returns IN_PROGRESS session."""
        session = _make_session(session_id="active-001", status="IN_PROGRESS")
        session_store.save_session(session)
        found = session_store.find_active_session()
        self.assertIsNotNone(found)
        self.assertEqual(found.session_id, "active-001")

    def test_find_active_returns_none_all_completed(self):
        """find_active_session: returns None when all completed."""
        session = _make_session(session_id="done-001", status="COMPLETED")
        session.completed_at = _utcnow_iso()
        session_store.save_session(session)
        found = session_store.find_active_session()
        self.assertIsNone(found)

    def test_load_nonexistent_returns_none(self):
        """load_session: nonexistent → None."""
        loaded = session_store.load_session("nonexistent")
        self.assertIsNone(loaded)

    def test_list_sessions(self):
        """list_sessions: returns correct count."""
        s1 = _make_session(session_id="list-001", status="COMPLETED")
        s1.completed_at = _utcnow_iso()
        s2 = _make_session(session_id="list-002", status="IN_PROGRESS")
        session_store.save_session(s1)
        session_store.save_session(s2)
        sessions = session_store.list_sessions()
        self.assertEqual(len(sessions), 2)


if __name__ == "__main__":
    unittest.main()
