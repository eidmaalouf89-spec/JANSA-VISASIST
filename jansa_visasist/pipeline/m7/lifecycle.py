"""
Module 7 — Session Lifecycle Management.

Handles W1 (create), W4 (open/resume), and W5 (complete) workflows.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from jansa_visasist.config_m7 import (
    CATEGORY_SORT_ORDER,
    SESSION_SCHEMA_VERSION,
    TERMINAL_STATUSES,
    VALID_DECISIONS,
)
from jansa_visasist.pipeline.m7 import schemas
from jansa_visasist.pipeline.m7.schemas import (
    BatchDecision,
    BatchItem,
    BatchSession,
    OperationResult,
    SessionReport,
)
from jansa_visasist.pipeline.m7 import session_store
from jansa_visasist.pipeline.m7 import validation
from jansa_visasist.pipeline.m7.signature import compute_dataset_signature
from jansa_visasist.pipeline.m7 import reporting

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    """Return current UTC time as ISO string with Z suffix."""
    return datetime.utcnow().isoformat() + "Z"


def _generate_session_id(timestamp: str) -> str:
    """Generate deterministic session ID using uuid5 with namespace + timestamp."""
    namespace = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    return str(uuid.uuid5(namespace, timestamp))


def _generate_batch_id(timestamp: str, pipeline_run_id: str) -> str:
    """Generate deterministic batch ID."""
    namespace = uuid.UUID("b2c3d4e5-f6a7-8901-bcde-f12345678901")
    return str(uuid.uuid5(namespace, f"{timestamp}:{pipeline_run_id}"))


def _sort_items(items: List[BatchItem]) -> List[BatchItem]:
    """Sort items by W2 ordering: CATEGORY_SORT_ORDER index, then priority_score DESC."""

    def sort_key(item: BatchItem) -> tuple:
        try:
            cat_idx = CATEGORY_SORT_ORDER.index(item.category)
        except ValueError:
            cat_idx = len(CATEGORY_SORT_ORDER)
        return (cat_idx, -item.priority_score)

    sorted_items = sorted(items, key=sort_key)
    for i, item in enumerate(sorted_items):
        item.order_index = i
    return sorted_items


def create_session(
    m3_queue_data: List[Dict[str, Any]],
    m1_metadata: Dict[str, Any],
    pipeline_run_id: str,
    filter_params: Optional[Dict[str, Any]] = None,
) -> OperationResult:
    """
    W1: Create a new batch session from M3 queue data.

    Args:
        m3_queue_data: List of M3 priority queue records (dicts).
        m1_metadata: Dict with source_file, total_rows, total_sheets, sheet_names, row_ids.
        pipeline_run_id: Identifier for the current pipeline run.
        filter_params: Optional filters (lot, category, is_overdue). AND logic.

    Returns:
        OperationResult with BatchSession in data on success.
    """
    # 1. Check no active session exists
    active = session_store.find_active_session()
    if active is not None:
        return OperationResult(
            status="ERROR",
            error_code="active_session_exists",
            message=f"Active session already exists: {active.session_id}",
        )

    # 2. Compute dataset signature
    dataset_sig = compute_dataset_signature(
        source_file=m1_metadata.get("source_file", ""),
        total_rows=m1_metadata.get("total_rows", 0),
        total_sheets=m1_metadata.get("total_sheets", 0),
        sheet_names=m1_metadata.get("sheet_names", []),
        row_ids=m1_metadata.get("row_ids", []),
    )

    # 3. Apply filter_params if provided (AND logic)
    filtered_data = list(m3_queue_data)
    if filter_params:
        if "lot" in filter_params and filter_params["lot"] is not None:
            lot_filter = filter_params["lot"]
            filtered_data = [r for r in filtered_data if r.get("source_sheet") == lot_filter or r.get("lot") == lot_filter]
        if "category" in filter_params and filter_params["category"] is not None:
            cat_filter = filter_params["category"]
            filtered_data = [r for r in filtered_data if r.get("category") == cat_filter]
        if "is_overdue" in filter_params and filter_params["is_overdue"] is not None:
            overdue_filter = filter_params["is_overdue"]
            filtered_data = [r for r in filtered_data if r.get("is_overdue") == overdue_filter]

    # 4. Build BatchItem list with SNAPSHOT fields
    items: List[BatchItem] = []
    for row in filtered_data:
        item = BatchItem(
            row_id=row["row_id"],
            document=row.get("document"),
            titre=row.get("titre"),
            source_sheet=row.get("source_sheet", ""),
            category=row.get("category", "NOT_STARTED"),
            priority_score=float(row.get("priority_score", 0.0)),
            consensus_type=row.get("consensus_type", "NOT_STARTED"),
            is_overdue=bool(row.get("is_overdue", False)),
            decision=None,
            order_index=0,
        )
        items.append(item)

    # 5. Sort by W2 ordering
    items = _sort_items(items)

    # 6. Create BatchSession
    now = _utcnow_iso()
    session_id = _generate_session_id(now)
    batch_id = _generate_batch_id(now, pipeline_run_id)

    session = BatchSession(
        session_id=session_id,
        batch_id=batch_id,
        status="CREATED",
        session_schema_version=SESSION_SCHEMA_VERSION,
        dataset_signature=dataset_sig,
        pipeline_run_id=pipeline_run_id,
        user_id=None,
        created_at=now,
        updated_at=now,
        completed_at=None,
        invalidated_at=None,
        invalidated_reason=None,
        items=items,
        current_index=0,
        filter_params=filter_params,
        total_items=len(items),
        decided_count=0,
        deferred_count=0,
        skipped_count=0,
    )

    # 7. Persist
    session_store.save_session(session)
    logger.info(
        "Session created: %s with %d items", session.session_id, session.total_items
    )

    return OperationResult(
        status="OK",
        error_code=None,
        message=f"Session created with {session.total_items} items",
        data=session,
    )


def open_session(
    session_id: str,
    current_signature: str,
    current_pipeline_run_id: str,
    current_schema_version: str,
) -> OperationResult:
    """
    W4: Open/resume a batch session. Validates freshness (VR1-VR4).

    Returns:
        OperationResult with BatchSession in data on success.
    """
    # 1. Load session
    session = session_store.load_session(session_id)
    if session is None:
        return OperationResult(
            status="ERROR",
            error_code="session_not_found",
            message=f"Session not found: {session_id}",
        )

    # 2. Check terminal status
    if session.status in TERMINAL_STATUSES:
        return OperationResult(
            status="ERROR",
            error_code="session_terminal",
            message=f"Session is in terminal status: {session.status}",
        )

    # 3. Validate dataset freshness
    is_valid, reason = validation.validate_dataset_freshness(
        session, current_signature, current_pipeline_run_id, current_schema_version
    )
    if not is_valid:
        now = _utcnow_iso()
        session.status = "INVALIDATED"
        session.invalidated_at = now
        session.invalidated_reason = reason
        session.updated_at = now
        session_store.save_session(session)
        logger.warning("Session %s invalidated: %s", session_id, reason)
        return OperationResult(
            status="ERROR",
            error_code="session_invalidated",
            message=f"Session invalidated: {reason}",
            data=session,
        )

    # 4. Transition CREATED → IN_PROGRESS
    if session.status == "CREATED":
        session.status = "IN_PROGRESS"
        session.updated_at = _utcnow_iso()

    # 5. Recompute current_index
    session.current_index = validation.recompute_current_index(session.items)

    # 6. Persist and return
    session_store.save_session(session)
    logger.info("Session opened: %s (index=%d)", session_id, session.current_index)

    return OperationResult(
        status="OK",
        error_code=None,
        message=f"Session opened at index {session.current_index}",
        data=session,
    )


def get_current_item(session: BatchSession) -> Optional[BatchItem]:
    """Return the current item to review, or None if all decided."""
    if session.current_index >= session.total_items:
        return None
    # Items are sorted by order_index, so index i has order_index i
    for item in session.items:
        if item.order_index == session.current_index:
            return item
    return None


def complete_session(session: BatchSession) -> OperationResult:
    """
    W5: Complete a session and generate report.

    Auto-skips undecided items [FIX C].
    """
    # 1. Check status
    if session.status != "IN_PROGRESS":
        return OperationResult(
            status="ERROR",
            error_code="invalid_status",
            message=f"Cannot complete session in status '{session.status}'. Must be IN_PROGRESS.",
        )

    now = _utcnow_iso()

    # 2. [FIX C] Auto-skip undecided items
    for item in session.items:
        if item.decision is None:
            item.decision = BatchDecision(
                decision_type="SKIPPED",
                visa_value=None,
                comment="Auto-skipped at session completion",
                decided_at=now,
                suggested_action=None,
                proposed_visa=None,
                decision_source="manual",
            )
            session.decided_count += 1
            session.skipped_count += 1

    # 3. Set status COMPLETED
    session.status = "COMPLETED"
    session.completed_at = now
    session.updated_at = now
    session.current_index = session.total_items

    # 4. Generate report
    report = reporting.generate_report(session)

    # 5. Export (JSON + CSV)
    from jansa_visasist.pipeline.m7 import exporter

    output_dir = os.path.join(session_store._get_storage_dir(), "reports")
    os.makedirs(output_dir, exist_ok=True)
    try:
        exporter.export_json(report, output_dir)
        exporter.export_csv(report, output_dir)
    except Exception as exc:
        logger.error("Export failed for session %s: %s", session.session_id, exc)

    # 6. Persist
    session_store.save_session(session)
    logger.info("Session completed: %s", session.session_id)

    return OperationResult(
        status="OK",
        error_code=None,
        message="Session completed successfully",
        data=report,
    )
