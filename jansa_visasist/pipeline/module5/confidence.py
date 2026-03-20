"""
Module 5 — Layer 6: Confidence Scoring.

[Plan §2.8, PATCH 3] Deterministic formula. No ML. No heuristics. Reproducible.
Bounded [0, 1]. Rounded to 4 decimal places.

Hard override: If Layer 1 matched (HOLD from degraded/on_hold) → confidence = 0.0.

Formula:
    confidence_raw =
        BASE_CONFIDENCE
      + (W_CONSENSUS × score_consensus)
      + (W_COMPLETENESS × score_completeness)
      - (W_MISSING × missing_ratio)
      - (W_CONFLICT × conflict_penalty)
      - (W_OVERDUE × overdue_penalty)
      - (W_DEGRADED × degraded_flag)

    confidence = clamp(0.0, 1.0, round(confidence_raw, 4))
"""

import logging
from typing import Any

from .constants import (
    BASE_CONFIDENCE,
    OVERDUE_PENALTY_CAP,
    W_COMPLETENESS,
    W_CONFLICT,
    W_CONSENSUS,
    W_DEGRADED,
    W_MISSING,
    W_OVERDUE,
)
from .validation import (
    safe_get_m3_bool,
    safe_get_m3_int,
    safe_get_m3_response_rate,
    safe_get_m4_agreement_ratio,
    safe_get_m4_missing_count,
)

logger = logging.getLogger(__name__)


def compute_confidence(
    m3_item: dict[str, Any],
    m4_result: dict[str, Any],
    analysis_degraded: bool,
    lifecycle_state: str,
) -> float:
    """Compute deterministic confidence score for a suggestion.

    [Plan §2.8, PATCH 3] Layer 6 formula.
    Hard override: returns 0.0 if analysis_degraded or lifecycle_state = ON_HOLD.

    Args:
        m3_item: M3 row as dict (needs consensus_type, is_overdue, days_overdue,
                 relevant_approvers, replied, total_assigned).
        m4_result: M4 analysis_result dict (needs agreement block, missing block).
        analysis_degraded: Whether M4 analysis was degraded for this item.
        lifecycle_state: The lifecycle_state used for decision.

    Returns:
        Float in [0.0, 1.0], rounded to 4 decimal places.
    """
    # Hard override [Plan §2.8]
    if analysis_degraded or lifecycle_state == "ON_HOLD":
        return 0.0

    # --- Component computation ---

    # score_consensus: A1.agreement_ratio [0, 1]
    score_consensus: float = safe_get_m4_agreement_ratio(m4_result)

    # score_completeness: A3.response_rate [0, 1]
    score_completeness: float = safe_get_m3_response_rate(m3_item)

    # missing_ratio: A3.missing_count / max(relevant_approvers, 1)
    missing_count: int = safe_get_m4_missing_count(m4_result)
    relevant_approvers: int = safe_get_m3_int(m3_item, "relevant_approvers", 1)
    if relevant_approvers < 1:
        relevant_approvers = 1  # Avoid division by zero
    missing_ratio: float = min(missing_count / relevant_approvers, 1.0)

    # conflict_penalty: 1.0 if consensus_type = MIXED, else 0.0
    consensus_type = m3_item.get("consensus_type")
    conflict_penalty: float = 1.0 if consensus_type == "MIXED" else 0.0

    # overdue_penalty: min(days_overdue / OVERDUE_PENALTY_CAP, 1.0) if overdue
    is_overdue: bool = safe_get_m3_bool(m3_item, "is_overdue", False)
    days_overdue: int = safe_get_m3_int(m3_item, "days_overdue", 0)
    overdue_penalty: float = 0.0
    if is_overdue and days_overdue > 0:
        overdue_penalty = min(days_overdue / OVERDUE_PENALTY_CAP, 1.0)

    # degraded_flag: 1.0 if analysis_degraded = true, else 0.0
    # Already handled by hard override above, but included for formula completeness
    degraded_flag: float = 1.0 if analysis_degraded else 0.0

    # --- Formula ---
    confidence_raw: float = (
        BASE_CONFIDENCE
        + (W_CONSENSUS * score_consensus)
        + (W_COMPLETENESS * score_completeness)
        - (W_MISSING * missing_ratio)
        - (W_CONFLICT * conflict_penalty)
        - (W_OVERDUE * overdue_penalty)
        - (W_DEGRADED * degraded_flag)
    )

    # Clamp and round [Plan §2.8]
    confidence: float = max(0.0, min(1.0, confidence_raw))
    confidence = round(confidence, 4)

    return confidence
