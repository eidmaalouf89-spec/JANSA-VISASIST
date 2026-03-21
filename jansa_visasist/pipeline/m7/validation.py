"""
Module 7 — Validation Logic.

Pure functions for session validation. No side effects.
"""

from typing import List, Optional, Tuple

from jansa_visasist.config_m7 import (
    VALID_DECISIONS,
    VALID_VISA_VALUES,
    VALID_DECISION_SOURCES,
    VALID_SESSION_STATUSES,
    TERMINAL_STATUSES,
)
from jansa_visasist.pipeline.m7.schemas import BatchItem, BatchSession


# ──────────────────────────────────────────────
# State transition rules
# ──────────────────────────────────────────────

_ALLOWED_TRANSITIONS = {
    "CREATED": {"IN_PROGRESS", "INVALIDATED"},
    "IN_PROGRESS": {"COMPLETED", "INVALIDATED"},
    "COMPLETED": set(),
    "INVALIDATED": set(),
}


def validate_state_transition(current_status: str, target_status: str) -> bool:
    """Check if a state transition is allowed."""
    allowed = _ALLOWED_TRANSITIONS.get(current_status, set())
    return target_status in allowed


# ──────────────────────────────────────────────
# Schema validation
# ──────────────────────────────────────────────

def validate_session_schema(session: BatchSession) -> List[str]:
    """Validate required fields, enums, type correctness. Return error messages."""
    errors: List[str] = []

    if not session.session_id:
        errors.append("session_id is empty")
    if not session.batch_id:
        errors.append("batch_id is empty")
    if session.status not in VALID_SESSION_STATUSES:
        errors.append(f"Invalid status '{session.status}'")
    if not session.dataset_signature:
        errors.append("dataset_signature is empty")
    if not session.pipeline_run_id:
        errors.append("pipeline_run_id is empty")
    if not session.created_at:
        errors.append("created_at is empty")
    if not session.updated_at:
        errors.append("updated_at is empty")
    if session.total_items < 0:
        errors.append(f"total_items is negative: {session.total_items}")
    if session.decided_count < 0:
        errors.append(f"decided_count is negative: {session.decided_count}")
    if session.deferred_count < 0:
        errors.append(f"deferred_count is negative: {session.deferred_count}")
    if session.skipped_count < 0:
        errors.append(f"skipped_count is negative: {session.skipped_count}")

    for item in session.items:
        if item.decision is not None:
            d = item.decision
            if d.decision_type not in VALID_DECISIONS:
                errors.append(
                    f"Item {item.row_id}: invalid decision_type '{d.decision_type}'"
                )
            if d.decision_source not in VALID_DECISION_SOURCES:
                errors.append(
                    f"Item {item.row_id}: invalid decision_source '{d.decision_source}'"
                )

    return errors


# ──────────────────────────────────────────────
# Dataset freshness validation (VR1-VR4)
# ──────────────────────────────────────────────

def validate_dataset_freshness(
    session: BatchSession,
    current_signature: str,
    current_pipeline_run_id: str,
    current_schema_version: str,
) -> Tuple[bool, Optional[str]]:
    """
    Apply VR1-VR4 freshness rules.

    Returns:
        (is_valid, invalidation_reason_or_none)
    """
    # VR1: schema version mismatch
    if session.session_schema_version != current_schema_version:
        return (False, "schema_version_mismatch")

    # VR2: dataset signature mismatch
    if session.dataset_signature != current_signature:
        return (False, "dataset_signature_mismatch")

    # VR3: pipeline_run_id mismatch
    if session.pipeline_run_id != current_pipeline_run_id:
        return (False, "pipeline_run_id_mismatch")

    # VR4: all match
    return (True, None)


# ──────────────────────────────────────────────
# Decision integrity
# ──────────────────────────────────────────────

def validate_decision_integrity(session: BatchSession) -> List[str]:
    """Verify counters match actual decisions. Return error messages."""
    errors: List[str] = []

    actual_decided = sum(1 for item in session.items if item.decision is not None)
    actual_deferred = sum(
        1 for item in session.items
        if item.decision is not None and item.decision.decision_type == "DEFERRED"
    )
    actual_skipped = sum(
        1 for item in session.items
        if item.decision is not None and item.decision.decision_type == "SKIPPED"
    )

    if session.decided_count != actual_decided:
        errors.append(
            f"decided_count mismatch: stored={session.decided_count}, "
            f"actual={actual_decided}"
        )
    if session.deferred_count != actual_deferred:
        errors.append(
            f"deferred_count mismatch: stored={session.deferred_count}, "
            f"actual={actual_deferred}"
        )
    if session.skipped_count != actual_skipped:
        errors.append(
            f"skipped_count mismatch: stored={session.skipped_count}, "
            f"actual={actual_skipped}"
        )

    return errors


# ──────────────────────────────────────────────
# Cursor recomputation
# ──────────────────────────────────────────────

def recompute_current_index(items: List[BatchItem]) -> int:
    """
    Scan items by order_index, return index of first undecided item.
    If all decided, return len(items).
    This is the ONLY way to determine current position.
    """
    sorted_items = sorted(items, key=lambda item: item.order_index)
    for i, item in enumerate(sorted_items):
        if item.decision is None:
            return i
    return len(items)
