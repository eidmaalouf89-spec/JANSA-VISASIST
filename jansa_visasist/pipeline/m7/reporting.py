"""
Module 7 — Report Generation.

Generates session reports at completion (W5).
"""

from datetime import datetime
from typing import Any, Dict, List

from jansa_visasist.config_m7 import CATEGORY_SORT_ORDER
from jansa_visasist.pipeline.m7.schemas import BatchSession, SessionReport


def generate_report(session: BatchSession) -> SessionReport:
    """
    Generate a SessionReport from a completed BatchSession.

    Args:
        session: A completed BatchSession.

    Returns:
        SessionReport with all breakdowns computed.
    """
    # 1. Compute duration
    created_dt = datetime.fromisoformat(session.created_at.rstrip("Z"))
    completed_at_str = session.completed_at or session.updated_at
    completed_dt = datetime.fromisoformat(completed_at_str.rstrip("Z"))
    duration_seconds = int((completed_dt - created_dt).total_seconds())

    # 2. Build visa_breakdown
    visa_breakdown: Dict[str, int] = {}
    for item in session.items:
        if item.decision and item.decision.decision_type == "VISA_ISSUED":
            vv = item.decision.visa_value or "UNKNOWN"
            visa_breakdown[vv] = visa_breakdown.get(vv, 0) + 1

    # 3. Build category_breakdown
    category_breakdown: Dict[str, Dict[str, int]] = {}
    for cat in CATEGORY_SORT_ORDER:
        category_breakdown[cat] = {
            "total": 0,
            "decided": 0,
            "deferred": 0,
            "skipped": 0,
        }
    for item in session.items:
        cat = item.category
        if cat not in category_breakdown:
            category_breakdown[cat] = {
                "total": 0,
                "decided": 0,
                "deferred": 0,
                "skipped": 0,
            }
        category_breakdown[cat]["total"] += 1
        if item.decision:
            category_breakdown[cat]["decided"] += 1
            if item.decision.decision_type == "DEFERRED":
                category_breakdown[cat]["deferred"] += 1
            elif item.decision.decision_type == "SKIPPED":
                category_breakdown[cat]["skipped"] += 1

    # 4. Build decision_source_breakdown
    decision_source_breakdown: Dict[str, int] = {"manual": 0, "assisted": 0}
    for item in session.items:
        if item.decision:
            src = item.decision.decision_source
            decision_source_breakdown[src] = decision_source_breakdown.get(src, 0) + 1

    # 5. Build decisions list
    decisions: List[Dict[str, Any]] = []
    for item in session.items:
        entry: Dict[str, Any] = {
            "row_id": item.row_id,
            "document": item.document,
            "source_sheet": item.source_sheet,
            "category": item.category,
            "decision_type": item.decision.decision_type if item.decision else None,
            "visa_value": item.decision.visa_value if item.decision else None,
            "comment": item.decision.comment if item.decision else None,
            "decided_at": item.decision.decided_at if item.decision else None,
            "decision_source": item.decision.decision_source if item.decision else None,
            "suggested_action": item.decision.suggested_action if item.decision else None,
            "proposed_visa": item.decision.proposed_visa if item.decision else None,
        }
        decisions.append(entry)

    return SessionReport(
        session_id=session.session_id,
        batch_id=session.batch_id,
        created_at=session.created_at,
        completed_at=completed_at_str,
        duration_seconds=duration_seconds,
        dataset_signature=session.dataset_signature,
        pipeline_run_id=session.pipeline_run_id,
        invalidated_reason=session.invalidated_reason,
        total_items=session.total_items,
        decided_count=session.decided_count,
        deferred_count=session.deferred_count,
        skipped_count=session.skipped_count,
        visa_breakdown=visa_breakdown,
        category_breakdown=category_breakdown,
        decision_source_breakdown=decision_source_breakdown,
        decisions=decisions,
    )
