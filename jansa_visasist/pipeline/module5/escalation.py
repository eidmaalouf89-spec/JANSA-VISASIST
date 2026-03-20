"""
Module 5 — Layer 5: Escalation Logic.

[Plan §2.7, PATCH 4] Determines escalation_level using explicit thresholds.
First matching rule wins (priority ordering ensures DIRECTION checked before MOEX).

8 rules, ordered by priority:
  1. consecutive_rejections >= 3 → DIRECTION
  2. days_overdue >= 60 → DIRECTION
  3. G1 systemic blocker >= 10 items → DIRECTION
  4. consecutive_rejections >= 2 → MOEX
  5. days_overdue >= 30 → MOEX
  6. G1 systemic blocker >= 5 items → MOEX
  7. suggested_action = ESCALATE (no threshold met) → MOEX
  8. ALL OTHER CASES → NONE
"""

import logging

from .constants import (
    ESCALATION_CONSEC_REJ_DIR,
    ESCALATION_CONSEC_REJ_MOEX,
    ESCALATION_OVERDUE_DIR,
    ESCALATION_OVERDUE_MOEX,
    ESCALATION_SYSTEMIC_DIR,
    ESCALATION_SYSTEMIC_MOEX,
)

logger = logging.getLogger(__name__)


def resolve_escalation(
    consecutive_rejections: int,
    days_overdue: int,
    is_overdue: bool,
    blocking_approvers: list[str],
    g1_blocker_index: dict[str, dict],
    suggested_action: str,
    row_id: str = "?",
) -> tuple[str, list[str]]:
    """Determine escalation_level using threshold rules.

    [Plan §2.7, PATCH 4] First matching rule wins.
    Returns (escalation_level, systemic_blockers_list).

    Args:
        consecutive_rejections: A4 consecutive rejection count.
        days_overdue: Days overdue from M3.
        is_overdue: Whether item is overdue from M3.
        blocking_approvers: List of blocking approver keys from M3.
        g1_blocker_index: G1 approver_key → blocker data dict.
        suggested_action: The suggested_action from Layer 2.
        row_id: Item row_id for logging.

    Returns:
        Tuple of (escalation_level, systemic_blockers_list).
        escalation_level: "NONE", "MOEX", or "DIRECTION".
        systemic_blockers_list: approver keys meeting systemic threshold.
    """
    # Track systemic blockers for reason_details
    systemic_blockers: list[str] = []

    # --- Rule 1: consecutive_rejections >= DIRECTION threshold ---
    if consecutive_rejections >= ESCALATION_CONSEC_REJ_DIR:
        return "DIRECTION", systemic_blockers

    # --- Rule 2: days_overdue >= DIRECTION threshold ---
    # [Plan §2.7] Condition is days_overdue alone — no is_overdue guard.
    if days_overdue >= ESCALATION_OVERDUE_DIR:
        return "DIRECTION", systemic_blockers

    # --- Rule 3: G1 systemic blocker >= DIRECTION threshold ---
    if g1_blocker_index:
        direction_systemic = _check_systemic_threshold(
            blocking_approvers, g1_blocker_index, ESCALATION_SYSTEMIC_DIR,
        )
        if direction_systemic:
            systemic_blockers = direction_systemic
            return "DIRECTION", systemic_blockers
    else:
        # [Edge Case #12] G1 data unavailable — skip systemic rules
        if blocking_approvers:
            logger.warning(
                "M5: G1 data unavailable, systemic escalation rules skipped "
                "for row_id=%s.",
                row_id,
            )

    # --- Rule 4: consecutive_rejections >= MOEX threshold ---
    if consecutive_rejections >= ESCALATION_CONSEC_REJ_MOEX:
        return "MOEX", systemic_blockers

    # --- Rule 5: days_overdue >= MOEX threshold ---
    # [Plan §2.7] Condition is days_overdue alone — no is_overdue guard.
    if days_overdue >= ESCALATION_OVERDUE_MOEX:
        return "MOEX", systemic_blockers

    # --- Rule 6: G1 systemic blocker >= MOEX threshold ---
    if g1_blocker_index:
        moex_systemic = _check_systemic_threshold(
            blocking_approvers, g1_blocker_index, ESCALATION_SYSTEMIC_MOEX,
        )
        if moex_systemic:
            systemic_blockers = moex_systemic
            return "MOEX", systemic_blockers

    # --- Rule 7: ESCALATE action always produces at least MOEX ---
    if suggested_action == "ESCALATE":
        return "MOEX", systemic_blockers

    # --- Rule 8: ALL OTHER CASES → NONE ---
    return "NONE", systemic_blockers


def _check_systemic_threshold(
    blocking_approvers: list[str],
    g1_blocker_index: dict[str, dict],
    threshold: int,
) -> list[str]:
    """Check if any blocking approver meets the systemic threshold.

    [Plan §2.7, Rules 3 & 6] For each approver_key in blocking_approvers,
    look up g1_blocker_index[approver_key].total_blocking.
    If ANY approver's total_blocking >= threshold → rule fires.

    Args:
        blocking_approvers: List of blocking approver keys.
        g1_blocker_index: G1 blocker data index.
        threshold: Minimum total_blocking count.

    Returns:
        List of approver keys meeting threshold (empty if none).
    """
    result: list[str] = []
    for approver_key in blocking_approvers:
        blocker_data = g1_blocker_index.get(approver_key)
        if blocker_data is None:
            continue
        total_blocking = blocker_data.get("total_blocking", 0)
        if total_blocking >= threshold:
            result.append(approver_key)
    return sorted(result)
