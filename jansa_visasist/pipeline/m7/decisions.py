"""
Module 7 — Decision Recording.

Handles W3: recording decisions on batch items.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from jansa_visasist.config_m7 import (
    VALID_DECISIONS,
    VALID_VISA_VALUES,
    VALID_DECISION_SOURCES,
)
from jansa_visasist.pipeline.m7.schemas import (
    BatchDecision,
    BatchSession,
    OperationResult,
)
from jansa_visasist.pipeline.m7 import session_store
from jansa_visasist.pipeline.m7 import validation

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    """Return current UTC time as ISO string with Z suffix."""
    return datetime.utcnow().isoformat() + "Z"


def record_decision(
    session: BatchSession,
    row_id: str,
    decision_type: str,
    visa_value: Optional[str] = None,
    comment: Optional[str] = None,
    decision_source: str = "manual",
    m5_suggestion: Optional[Dict[str, Any]] = None,
) -> OperationResult:
    """
    W3: Record a decision on a batch item.

    Args:
        session: The active BatchSession (must be IN_PROGRESS).
        row_id: The row_id of the item to decide.
        decision_type: One of VALID_DECISIONS.
        visa_value: Required if VISA_ISSUED, must be None otherwise.
        comment: Optional comment.
        decision_source: "manual" or "assisted".
        m5_suggestion: Optional M5 suggestion dict for audit trail.

    Returns:
        OperationResult.
    """
    # 1. Check session status
    if session.status != "IN_PROGRESS":
        return OperationResult(
            status="ERROR",
            error_code="invalid_status",
            message=f"Session status is '{session.status}', must be IN_PROGRESS",
        )

    # 2. Validate decision_type
    if decision_type not in VALID_DECISIONS:
        return OperationResult(
            status="ERROR",
            error_code="invalid_decision_type",
            message=f"Invalid decision_type '{decision_type}'. Must be one of {VALID_DECISIONS}",
        )

    # 3. Validate visa_value for VISA_ISSUED
    if decision_type == "VISA_ISSUED":
        if visa_value not in VALID_VISA_VALUES:
            return OperationResult(
                status="ERROR",
                error_code="invalid_visa_value",
                message=f"Invalid visa_value '{visa_value}' for VISA_ISSUED. Must be one of {VALID_VISA_VALUES}",
            )

    # 4. visa_value must be None for non-VISA_ISSUED
    if decision_type != "VISA_ISSUED" and visa_value is not None:
        return OperationResult(
            status="ERROR",
            error_code="visa_value_not_allowed",
            message=f"visa_value must be None for decision_type '{decision_type}'",
        )

    # 5. Validate decision_source
    if decision_source not in VALID_DECISION_SOURCES:
        return OperationResult(
            status="ERROR",
            error_code="invalid_decision_source",
            message=f"Invalid decision_source '{decision_source}'. Must be one of {VALID_DECISION_SOURCES}",
        )

    # 6. Find item by row_id
    target_item = None
    for item in session.items:
        if item.row_id == row_id:
            target_item = item
            break

    if target_item is None:
        return OperationResult(
            status="ERROR",
            error_code="item_not_found",
            message=f"Item with row_id '{row_id}' not found in session",
        )

    # 7. [FIX D] Check idempotency — already decided
    if target_item.decision is not None:
        return OperationResult(
            status="ALREADY_DECIDED",
            error_code=None,
            message=f"Decision already recorded for {row_id}",
        )

    # 8. Create BatchDecision
    decision = BatchDecision(
        decision_type=decision_type,
        visa_value=visa_value,
        comment=comment,
        decided_at=_utcnow_iso(),
        suggested_action=m5_suggestion.get("suggested_action") if m5_suggestion else None,
        proposed_visa=m5_suggestion.get("proposed_visa") if m5_suggestion else None,
        decision_source=decision_source,
    )

    # 9. Attach to item
    target_item.decision = decision

    # 10. Update counters
    session.decided_count += 1
    if decision_type == "DEFERRED":
        session.deferred_count += 1
    elif decision_type == "SKIPPED":
        session.skipped_count += 1

    # 11. Recompute current_index
    session.current_index = validation.recompute_current_index(session.items)

    # 12. Update timestamp
    session.updated_at = _utcnow_iso()

    # 13. Persist
    session_store.save_session(session)
    logger.info(
        "Decision recorded: %s → %s (visa=%s)",
        row_id,
        decision_type,
        visa_value,
    )

    # 14. Return OK
    return OperationResult(
        status="OK",
        error_code=None,
        message=f"Decision recorded: {row_id} → {decision_type}",
    )
