"""
Module 5 — Output Schemas.

Defines the SuggestionResult schema and S1/S2/S3 report schemas.
All schemas are contract-locked: downstream consumers depend on these
exact column names and types.

[Plan §3] Per-item output schema.
[Plan §Phase 3] Report schemas (S1, S2, S3).
"""

from typing import Any


# ============================================================================
# SuggestionResult — Per-Item Output Schema [Plan §3]
# ============================================================================

# Fixed key set for reason_details dict (always present, alphabetically sorted)
# [PATCH 6, OBS-2]
REASON_DETAILS_KEYS: list[str] = sorted([
    "consensus_strength",
    "days_overdue",
    "missing_count",
    "priority_score",
    "rejection_depth",
    "staleness_days",
    "systemic_blocker_detected",
])
"""Fixed 7-key set for reason_details. Always present, always sorted alphabetically."""


# All fields of the SuggestionResult, in declaration order.
SUGGESTION_RESULT_FIELDS: list[str] = [
    "row_id",
    "suggested_action",
    "action_priority",
    "proposed_visa",
    "confidence",
    "reason_code",
    "reason_details",
    "blocking_approvers",
    "missing_approvers",
    "relance_required",
    "relance_targets",
    "relance_template_id",
    "relance_message",
    "escalation_required",
    "escalation_level",
    "based_on_lifecycle",
    "analysis_degraded",
    "pipeline_run_id",
]
"""Ordered list of all SuggestionResult dict keys."""


def build_safe_default_result(
    row_id: str,
    lifecycle_state: str,
    pipeline_run_id: str,
    blocking_approvers: list[str] | None = None,
    missing_approvers: list[str] | None = None,
) -> dict[str, Any]:
    """Build a safe-default SuggestionResult when processing fails.

    [Plan §9.1, §9.2] All fields populated with conservative defaults.
    Used when an unhandled exception occurs during compute_suggestion.

    Args:
        row_id: Item primary key.
        lifecycle_state: M4 lifecycle_state if available, else "UNKNOWN".
        pipeline_run_id: Pipeline run identifier.
        blocking_approvers: Blocking approver list from M3 (or empty).
        missing_approvers: Missing approver list from M3 (or empty).

    Returns:
        Complete SuggestionResult dict with safe defaults.
    """
    return {
        "action_priority": 0,
        "analysis_degraded": True,
        "based_on_lifecycle": lifecycle_state,
        "blocking_approvers": sorted(blocking_approvers or []),
        "confidence": 0.0,
        "escalation_level": "NONE",
        "escalation_required": False,
        "missing_approvers": sorted(missing_approvers or []),
        "pipeline_run_id": pipeline_run_id,
        "proposed_visa": "NONE",
        "reason_code": "DEGRADED_ANALYSIS",
        "reason_details": _build_default_reason_details(),
        "relance_message": None,
        "relance_required": False,
        "relance_targets": [],
        "relance_template_id": None,
        "row_id": row_id,
        "suggested_action": "HOLD",
    }


def _build_default_reason_details() -> dict[str, Any]:
    """Build a reason_details dict with all 7 keys set to safe defaults.

    [PATCH 6] All keys always present, alphabetically sorted.
    """
    return {
        "consensus_strength": None,
        "days_overdue": 0,
        "missing_count": 0,
        "priority_score": 0,
        "rejection_depth": 0,
        "staleness_days": None,
        "systemic_blocker_detected": False,
    }


# ============================================================================
# S1 Report Schema — Action Distribution [Plan §Phase 3]
# ============================================================================

S1_COLUMNS: list[str] = [
    "suggested_action",
    "source_sheet",
    "priority_band",
    "item_count",
    "avg_confidence",
    "avg_action_priority",
    "escalated_count",
]
"""S1 report columns. GROUP BY (suggested_action, source_sheet, priority_band)."""


# ============================================================================
# S2 Report Schema — VISA Recommendation [Plan §Phase 3]
# ============================================================================

S2_COLUMNS: list[str] = [
    "proposed_visa",
    "source_sheet",
    "item_count",
    "avg_confidence",
    "pct_of_lot",
]
"""S2 report columns. GROUP BY (proposed_visa, source_sheet)."""


# ============================================================================
# S3 Report Schema — Communication / Relance [Plan §Phase 3]
# ============================================================================

S3_COLUMNS: list[str] = [
    "row_id",
    "document",
    "source_sheet",
    "suggested_action",
    "relance_required",
    "relance_targets",
    "relance_template_id",
    "relance_message",
    "escalation_level",
    "action_priority",
]
"""S3 report columns. FILTER WHERE relance_required=true."""
