"""
Module 5 — Core Decision Engine (Layers 0–2 + Assembly).

[Plan §2.2–§2.10, §7.3] Per-item suggestion computation.
Calls Layers 0→1→2→3→4→5→6 in strict order.
Produces one SuggestionResult dict per item.

6-layer sequential decision engine:
  Layer 0: Scope guard (EXCLUDED → SKIP)
  Layer 1: Hard overrides (ON_HOLD, degraded → HOLD)
  Layer 2: Primary action resolution (lifecycle_state → action)
  Layer 3: VISA recommendation (action × consensus → visa)
  Layer 4: Relance logic (communication targets and templates)
  Layer 5: Escalation logic (threshold-based escalation_level)
  Layer 6: Confidence scoring (deterministic formula)
"""

import logging
import traceback
from typing import Any, Optional

from .confidence import compute_confidence
from .escalation import resolve_escalation
from .enums import validate_enum
from .priority import compute_action_priority
from .relance import resolve_relance
from .schemas import build_safe_default_result
from .validation import (
    safe_get_m3_bool,
    safe_get_m3_int,
    safe_get_m3_list,
    safe_get_m4_agreement_ratio,
    safe_get_m4_analysis_degraded,
    safe_get_m4_consecutive_rejections,
    safe_get_m4_days_since_last_action,
    safe_get_m4_lifecycle_state,
    safe_get_m4_missing_count,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Layer 2: Primary Action Decision Table [Plan §2.4, PATCH 1]
# ============================================================================

def resolve_action(
    lifecycle_state: str,
    consensus_type: str,
    consecutive_rejections: int,
    is_overdue: bool,
    analysis_degraded: bool,
) -> tuple[str, str]:
    """Resolve suggested_action and reason_code from Layer 1 + Layer 2.

    [Plan §2.3, §2.4] First checks Layer 1 hard overrides, then Layer 2
    decision table. Every item matches exactly one row.

    Args:
        lifecycle_state: M4 lifecycle_state.
        consensus_type: M3 consensus_type.
        consecutive_rejections: M4 A4 consecutive rejection count.
        is_overdue: M3 is_overdue flag.
        analysis_degraded: M4 analysis_degraded flag.

    Returns:
        Tuple of (suggested_action, reason_code).
    """
    # --- Layer 1: Hard Overrides [Plan §2.3] ---
    # Row 1a: ON_HOLD
    if lifecycle_state == "ON_HOLD":
        return ("HOLD", "DEGRADED_ANALYSIS")
    # Row 1b: analysis_degraded
    if analysis_degraded:
        return ("HOLD", "DEGRADED_ANALYSIS")

    # --- Layer 2: Primary Action Resolution [Plan §2.4] ---

    # Rows 2a–2b: NOT_STARTED
    if lifecycle_state == "NOT_STARTED" and consensus_type == "NOT_STARTED":
        return ("HOLD", "NOT_YET_STARTED")

    # Rows 2c–2d: WAITING_RESPONSES
    if lifecycle_state == "WAITING_RESPONSES" and consensus_type == "INCOMPLETE":
        if is_overdue:
            return ("CHASE_APPROVERS", "OVERDUE")  # Row 2d
        return ("CHASE_APPROVERS", "MISSING_RESPONSES")  # Row 2c

    # Rows 2e–2f: READY_TO_ISSUE
    if lifecycle_state == "READY_TO_ISSUE" and consensus_type == "ALL_APPROVE":
        return ("ISSUE_VISA", "CONSENSUS_APPROVAL")

    # Rows 2g–2h: READY_TO_REJECT
    if lifecycle_state == "READY_TO_REJECT" and consensus_type == "ALL_REJECT":
        return ("ISSUE_VISA", "CONSENSUS_REJECTION")

    # Rows 2i–2j: NEEDS_ARBITRATION
    if lifecycle_state == "NEEDS_ARBITRATION" and consensus_type == "MIXED":
        return ("ARBITRATE", "MIXED_CONFLICT")

    # Rows 2k–2n: CHRONIC_BLOCKED
    if lifecycle_state == "CHRONIC_BLOCKED" and consensus_type == "ALL_REJECT":
        return ("ESCALATE", "BLOCKING_LOOP")

    # Row 2o: Catch-all (lifecycle/consensus inconsistency) [PATCH 8]
    logger.error(
        "M5 Layer 2 catch-all: lifecycle_state=%s, consensus_type=%s — "
        "lifecycle contradiction detected. Defaulting to HOLD/DEGRADED_ANALYSIS.",
        lifecycle_state, consensus_type,
    )
    return ("HOLD", "DEGRADED_ANALYSIS")


# ============================================================================
# Layer 3: VISA Recommendation [Plan §2.5, PATCH 2]
# ============================================================================

# Pure lookup table: (suggested_action, consensus_type) → proposed_visa
_VISA_LOOKUP: dict[tuple[str, str], str] = {
    # Row 3a
    ("ISSUE_VISA", "ALL_APPROVE"): "APPROVE",
    # Row 3b
    ("ISSUE_VISA", "ALL_REJECT"): "REJECT",
    # Row 3c
    ("CHASE_APPROVERS", "INCOMPLETE"): "WAIT",
    # Row 3d
    ("CHASE_APPROVERS", "NOT_STARTED"): "WAIT",
    # Row 3e
    ("ARBITRATE", "MIXED"): "NONE",
    # Row 3f
    ("ESCALATE", "ALL_REJECT"): "NONE",
    # Row 3g
    ("ESCALATE", "MIXED"): "NONE",
    # Row 3h
    ("ESCALATE", "INCOMPLETE"): "NONE",
    # Row 3i
    ("HOLD", "NOT_STARTED"): "NONE",
}


def resolve_visa(suggested_action: str, consensus_type: str) -> str:
    """Resolve proposed_visa from suggested_action × consensus_type.

    [Plan §2.5, PATCH 2] Pure lookup table. No derived logic.
    Fallback: NONE for any unmatched pair (row 3l).

    Args:
        suggested_action: From Layer 2.
        consensus_type: From M3.

    Returns:
        proposed_visa enum value.
    """
    key = (suggested_action, consensus_type)
    result = _VISA_LOOKUP.get(key)

    if result is not None:
        return result

    # Rows 3j/3k: HOLD with any consensus → NONE
    if suggested_action == "HOLD":
        return "NONE"

    # Row 3l: fallback [PATCH 8]
    logger.warning(
        "M5 Layer 3 fallback: no visa mapping for action=%s, consensus=%s. "
        "Defaulting to NONE.",
        suggested_action, consensus_type,
    )
    return "NONE"


# ============================================================================
# Reason Details Builder [Plan §2.4, §3 field 7]
# ============================================================================

def build_reason_details(
    m3_item: dict[str, Any],
    m4_result: dict[str, Any],
    g1_blocker_index: dict[str, dict],
    blocking_approvers: list[str],
) -> dict[str, Any]:
    """Build the reason_details dict with all 7 mandatory keys.

    [Plan §3 field 7, PATCH 6] Fixed key set, always alphabetically sorted.
    All keys always present — irrelevant keys use safe defaults.

    Args:
        m3_item: M3 row as dict.
        m4_result: M4 analysis_result dict.
        g1_blocker_index: G1 blocker data index.
        blocking_approvers: Blocking approver keys from M3.

    Returns:
        Dict with 7 alphabetically-sorted keys.
    """
    # consensus_strength: A1.agreement_ratio (float or null)
    consensus_strength: Optional[float] = safe_get_m4_agreement_ratio(m4_result)

    # days_overdue: from M3 (int)
    days_overdue: int = safe_get_m3_int(m3_item, "days_overdue", 0)

    # missing_count: A3.total_missing (int)
    missing_count: int = safe_get_m4_missing_count(m4_result)

    # priority_score: from M3 (int)
    priority_score: int = safe_get_m3_int(m3_item, "priority_score", 0)

    # rejection_depth: A4.consecutive_rejections (int)
    rejection_depth: int = safe_get_m4_consecutive_rejections(m4_result)

    # staleness_days: A6.days_since_diffusion (int or null)
    staleness_days: Optional[int] = safe_get_m4_days_since_last_action(m4_result)

    # systemic_blocker_detected: check G1 data
    systemic_blocker_detected: bool = False
    if g1_blocker_index and blocking_approvers:
        for approver_key in blocking_approvers:
            blocker_data = g1_blocker_index.get(approver_key)
            if blocker_data and blocker_data.get("is_systemic_blocker", False):
                systemic_blocker_detected = True
                break

    # [PATCH 6] Alphabetically sorted keys
    return {
        "consensus_strength": consensus_strength,
        "days_overdue": days_overdue,
        "missing_count": missing_count,
        "priority_score": priority_score,
        "rejection_depth": rejection_depth,
        "staleness_days": staleness_days,
        "systemic_blocker_detected": systemic_blocker_detected,
    }


# ============================================================================
# Per-Item Suggestion Computation [Plan §7.3]
# ============================================================================

def compute_suggestion(
    m3_item: dict[str, Any],
    m4_result: dict[str, Any],
    g1_blocker_index: dict[str, dict],
    pipeline_run_id: str,
) -> Optional[dict[str, Any]]:
    """Compute complete SuggestionResult for one item.

    [Plan §7.3] Calls Layers 0→1→2→3→4→5→6 sequentially, then
    action_priority, then assembles output.

    Wrapped in try/except: on exception → safe defaults [Plan §9.2].
    Returns None only for EXCLUDED items (Layer 0).

    Args:
        m3_item: Single M3 row as dict.
        m4_result: Corresponding M4 analysis_result dict.
        g1_blocker_index: G1 blocker data index.
        pipeline_run_id: Pipeline run identifier.

    Returns:
        Complete SuggestionResult dict, or None for EXCLUDED items.
    """
    row_id = str(m3_item.get("row_id", "?"))

    try:
        return _compute_suggestion_inner(
            m3_item, m4_result, g1_blocker_index, pipeline_run_id, row_id,
        )
    except Exception:
        # [Plan §9.2] Catch ALL unhandled exceptions
        logger.error(
            "M5: Unhandled exception in compute_suggestion for row_id=%s:\n%s",
            row_id, traceback.format_exc(),
        )
        # Extract what we can for safe defaults
        lifecycle_state = m4_result.get("lifecycle_state", "UNKNOWN") if m4_result else "UNKNOWN"
        blocking_approvers = safe_get_m3_list(m3_item, "blocking_approvers")
        missing_approvers = safe_get_m3_list(m3_item, "missing_approvers")

        return build_safe_default_result(
            row_id=row_id,
            lifecycle_state=str(lifecycle_state),
            pipeline_run_id=pipeline_run_id,
            blocking_approvers=blocking_approvers,
            missing_approvers=missing_approvers,
        )


def _compute_suggestion_inner(
    m3_item: dict[str, Any],
    m4_result: dict[str, Any],
    g1_blocker_index: dict[str, dict],
    pipeline_run_id: str,
    row_id: str,
) -> Optional[dict[str, Any]]:
    """Inner suggestion computation — no try/except wrapping.

    Implements Layers 0–6, action_priority, and output assembly.
    """
    # --- Extract core fields ---
    lifecycle_state = safe_get_m4_lifecycle_state(m4_result)
    analysis_degraded = safe_get_m4_analysis_degraded(m4_result)
    consensus_type = str(m3_item.get("consensus_type", ""))
    is_overdue = safe_get_m3_bool(m3_item, "is_overdue", False)
    days_overdue = safe_get_m3_int(m3_item, "days_overdue", 0)
    priority_score = safe_get_m3_int(m3_item, "priority_score", 0)
    blocking_approvers = sorted(safe_get_m3_list(m3_item, "blocking_approvers"))
    missing_approvers = sorted(safe_get_m3_list(m3_item, "missing_approvers"))
    consecutive_rejections = safe_get_m4_consecutive_rejections(m4_result)

    # --- Validate upstream enums ---
    if not validate_enum(consensus_type, "consensus_type"):
        logger.error(
            "M5: Invalid consensus_type '%s' for row_id=%s. "
            "Treating as analysis_degraded.",
            consensus_type, row_id,
        )
        analysis_degraded = True

    # --- Layer 0: Scope Guard [Plan §2.2] ---
    if lifecycle_state == "EXCLUDED":
        # EXCLUDED items do not produce a SuggestionResult
        logger.warning(
            "M5: EXCLUDED item found in M3 queue — row_id=%s. "
            "Upstream filtering anomaly. Omitting.",
            row_id,
        )
        return None

    # --- Layer 1+2: Action Resolution ---
    suggested_action, reason_code = resolve_action(
        lifecycle_state=lifecycle_state,
        consensus_type=consensus_type,
        consecutive_rejections=consecutive_rejections,
        is_overdue=is_overdue,
        analysis_degraded=analysis_degraded,
    )

    # --- Layer 3: VISA Recommendation ---
    proposed_visa = resolve_visa(suggested_action, consensus_type)

    # --- Layer 5: Escalation Logic ---
    # (Computed before Layer 4 because relance may reference escalation context,
    #  and before action_priority which needs escalation_level)
    escalation_level, systemic_blockers = resolve_escalation(
        consecutive_rejections=consecutive_rejections,
        days_overdue=days_overdue,
        is_overdue=is_overdue,
        blocking_approvers=blocking_approvers,
        g1_blocker_index=g1_blocker_index,
        suggested_action=suggested_action,
        row_id=row_id,
    )
    escalation_required = escalation_level != "NONE"

    # --- Layer 4: Relance Logic ---
    relance_result = resolve_relance(
        m3_item=m3_item,
        m4_result=m4_result,
        suggested_action=suggested_action,
        is_overdue=is_overdue,
        lifecycle_state=lifecycle_state,
        consecutive_rejections=consecutive_rejections,
        g1_blocker_index=g1_blocker_index,
        row_id=row_id,
    )

    # --- Layer 6: Confidence Scoring ---
    confidence = compute_confidence(
        m3_item=m3_item,
        m4_result=m4_result,
        analysis_degraded=analysis_degraded,
        lifecycle_state=lifecycle_state,
    )

    # --- Action Priority [Plan §2.9] ---
    action_priority = compute_action_priority(
        priority_score=priority_score,
        escalation_level=escalation_level,
        is_overdue=is_overdue,
        days_overdue=days_overdue,
        blocking_approvers=blocking_approvers,
    )

    # --- Reason Details [Plan §2.4, §3 field 7] ---
    reason_details = build_reason_details(
        m3_item=m3_item,
        m4_result=m4_result,
        g1_blocker_index=g1_blocker_index,
        blocking_approvers=blocking_approvers,
    )

    # --- Output Assembly [Plan §2.10, §3] ---
    # [PATCH 6] All dict keys sorted alphabetically
    result: dict[str, Any] = {
        "action_priority": action_priority,
        "analysis_degraded": analysis_degraded,
        "based_on_lifecycle": lifecycle_state,
        "blocking_approvers": blocking_approvers,  # Already sorted
        "confidence": confidence,
        "escalation_level": escalation_level,
        "escalation_required": escalation_required,
        "missing_approvers": missing_approvers,  # Already sorted
        "pipeline_run_id": pipeline_run_id,
        "proposed_visa": proposed_visa,
        "reason_code": reason_code,
        "reason_details": reason_details,
        "relance_message": relance_result["relance_message"],
        "relance_required": relance_result["relance_required"],
        "relance_targets": relance_result["relance_targets"],  # Already sorted in relance.py
        "relance_template_id": relance_result["relance_template_id"],
        "row_id": row_id,
        "suggested_action": suggested_action,
    }

    return result


