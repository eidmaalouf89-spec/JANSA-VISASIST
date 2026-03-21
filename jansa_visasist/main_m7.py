"""
JANSA VISASIST — Module 7: Batch Workflow Engine — Public API.

All functions return OperationResult.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from jansa_visasist.config_m7 import SESSION_SCHEMA_VERSION
from jansa_visasist.pipeline.m7.schemas import OperationResult
from jansa_visasist.pipeline.m7 import session_store
from jansa_visasist.pipeline.m7 import lifecycle
from jansa_visasist.pipeline.m7 import decisions as decisions_mod
from jansa_visasist.pipeline.m7 import validation

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# M5 Cache [FIX D]
# ──────────────────────────────────────────────

_m5_cache: Dict[Tuple[str, str], Dict] = {}


def get_m5_suggestion(
    row_id: str,
    pipeline_run_id: str,
    m3_row: Dict[str, Any],
    m4_result: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Get M5 suggestion, using cache or computing on demand.

    Returns None if M5 is not available or computation fails.
    """
    key = (row_id, pipeline_run_id)
    if key in _m5_cache:
        return _m5_cache[key]

    suggestion = _compute_m5_single(row_id, m3_row, m4_result)
    if suggestion:
        _m5_cache[key] = suggestion
    return suggestion


def _compute_m5_single(
    row_id: str,
    m3_row: Dict[str, Any],
    m4_result: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Try to compute M5 suggestion for a single item.
    Returns None if M5 module is not available or fails.
    """
    try:
        from jansa_visasist.pipeline.module5.engine import compute_suggestion

        result = compute_suggestion(m3_row, m4_result)
        if result:
            return result
    except (ImportError, Exception) as exc:
        logger.debug("M5 computation failed for %s: %s", row_id, exc)
    return None


def clear_m5_cache() -> None:
    """Clear the M5 suggestion cache. Called when session ends."""
    _m5_cache.clear()


# ──────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────


def create_batch(
    m3_queue_path: str,
    m1_metadata: Dict[str, Any],
    pipeline_run_id: str,
    filter_params: Optional[Dict[str, Any]] = None,
) -> OperationResult:
    """W1: Create a new batch session from M3 queue."""
    try:
        with open(m3_queue_path, "r", encoding="utf-8") as f:
            m3_queue_data = json.load(f)

        if not isinstance(m3_queue_data, list):
            return OperationResult(
                status="ERROR",
                error_code="invalid_queue_data",
                message=f"M3 queue data must be a list, got {type(m3_queue_data).__name__}",
            )

        return lifecycle.create_session(
            m3_queue_data=m3_queue_data,
            m1_metadata=m1_metadata,
            pipeline_run_id=pipeline_run_id,
            filter_params=filter_params,
        )
    except FileNotFoundError:
        return OperationResult(
            status="ERROR",
            error_code="queue_file_not_found",
            message=f"M3 queue file not found: {m3_queue_path}",
        )
    except json.JSONDecodeError as exc:
        return OperationResult(
            status="ERROR",
            error_code="queue_file_invalid",
            message=f"M3 queue file is not valid JSON: {exc}",
        )
    except Exception as exc:
        logger.error("create_batch failed: %s", exc)
        return OperationResult(
            status="ERROR",
            error_code="unexpected_error",
            message=f"Unexpected error: {exc}",
        )


def open_batch(
    session_id: str,
    current_signature: str,
    current_pipeline_run_id: str,
) -> OperationResult:
    """W4: Open/resume a batch session. Validates freshness."""
    try:
        return lifecycle.open_session(
            session_id=session_id,
            current_signature=current_signature,
            current_pipeline_run_id=current_pipeline_run_id,
            current_schema_version=SESSION_SCHEMA_VERSION,
        )
    except Exception as exc:
        logger.error("open_batch failed: %s", exc)
        return OperationResult(
            status="ERROR",
            error_code="unexpected_error",
            message=f"Unexpected error: {exc}",
        )


def decide(
    session_id: str,
    row_id: str,
    decision_type: str,
    visa_value: Optional[str] = None,
    comment: Optional[str] = None,
    decision_source: str = "manual",
    m5_suggestion: Optional[Dict[str, Any]] = None,
) -> OperationResult:
    """W3: Record a decision on an item."""
    try:
        session = session_store.load_session(session_id)
        if session is None:
            return OperationResult(
                status="ERROR",
                error_code="session_not_found",
                message=f"Session not found: {session_id}",
            )

        return decisions_mod.record_decision(
            session=session,
            row_id=row_id,
            decision_type=decision_type,
            visa_value=visa_value,
            comment=comment,
            decision_source=decision_source,
            m5_suggestion=m5_suggestion,
        )
    except Exception as exc:
        logger.error("decide failed: %s", exc)
        return OperationResult(
            status="ERROR",
            error_code="unexpected_error",
            message=f"Unexpected error: {exc}",
        )


def complete_batch(session_id: str) -> OperationResult:
    """W5: Complete session and generate report."""
    try:
        session = session_store.load_session(session_id)
        if session is None:
            return OperationResult(
                status="ERROR",
                error_code="session_not_found",
                message=f"Session not found: {session_id}",
            )

        result = lifecycle.complete_session(session)
        clear_m5_cache()
        return result
    except Exception as exc:
        logger.error("complete_batch failed: %s", exc)
        return OperationResult(
            status="ERROR",
            error_code="unexpected_error",
            message=f"Unexpected error: {exc}",
        )


def get_session_status(session_id: str) -> OperationResult:
    """Get current session state."""
    try:
        session = session_store.load_session(session_id)
        if session is None:
            return OperationResult(
                status="ERROR",
                error_code="session_not_found",
                message=f"Session not found: {session_id}",
            )

        return OperationResult(
            status="OK",
            error_code=None,
            message=f"Session {session_id} is {session.status}",
            data=session,
        )
    except Exception as exc:
        logger.error("get_session_status failed: %s", exc)
        return OperationResult(
            status="ERROR",
            error_code="unexpected_error",
            message=f"Unexpected error: {exc}",
        )


def list_all_sessions() -> OperationResult:
    """List all sessions with status."""
    try:
        sessions = session_store.list_sessions()
        return OperationResult(
            status="OK",
            error_code=None,
            message=f"Found {len(sessions)} sessions",
            data=sessions,
        )
    except Exception as exc:
        logger.error("list_all_sessions failed: %s", exc)
        return OperationResult(
            status="ERROR",
            error_code="unexpected_error",
            message=f"Unexpected error: {exc}",
        )
