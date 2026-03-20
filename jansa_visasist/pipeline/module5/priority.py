"""
Module 5 — Action Priority Computation.

[Plan §2.9, PATCH 5] Composite priority formula for sorting/display.

Formula:
    action_priority = clamp(0, 100,
        base + escalation_boost + overdue_boost + blocking_boost)

Components:
    base: priority_score from M3 (0–100), direct pass-through
    escalation_boost: 10 for MOEX, 20 for DIRECTION, else 0
    overdue_boost: min(days_overdue, 30) × 0.5 if overdue, else 0
    blocking_boost: 5 if any blocking_approvers, else 0

Result: integer, clamped to [0, 100].
"""

import logging

from .constants import (
    BLOCKING_BOOST,
    ESCALATION_BOOST_DIR,
    ESCALATION_BOOST_MOEX,
    OVERDUE_CAP,
    OVERDUE_MULTIPLIER,
    SCORE_BAND_CRITICAL,
    SCORE_BAND_HIGH,
    SCORE_BAND_MEDIUM,
)

logger = logging.getLogger(__name__)


def compute_action_priority(
    priority_score: int,
    escalation_level: str,
    is_overdue: bool,
    days_overdue: int,
    blocking_approvers: list[str],
) -> int:
    """Compute action_priority using the PATCH 5 formula.

    [Plan §2.9] Integer result, clamped to [0, 100].

    Args:
        priority_score: M3 priority_score (0–100).
        escalation_level: NONE, MOEX, or DIRECTION.
        is_overdue: Whether item is overdue.
        days_overdue: Days overdue from M3.
        blocking_approvers: List of blocking approver keys.

    Returns:
        Integer action_priority in [0, 100].
    """
    # base: direct pass-through from M3
    base: int = max(0, min(100, priority_score))

    # escalation_boost
    escalation_boost: int = 0
    if escalation_level == "DIRECTION":
        escalation_boost = ESCALATION_BOOST_DIR
    elif escalation_level == "MOEX":
        escalation_boost = ESCALATION_BOOST_MOEX

    # overdue_boost: min(days_overdue, OVERDUE_CAP) × OVERDUE_MULTIPLIER
    overdue_boost: float = 0.0
    if is_overdue and days_overdue > 0:
        overdue_boost = min(days_overdue, OVERDUE_CAP) * OVERDUE_MULTIPLIER

    # blocking_boost: flat boost when blockers present
    blocking_boost_val: int = BLOCKING_BOOST if len(blocking_approvers) > 0 else 0

    # Sum and clamp
    raw: float = base + escalation_boost + overdue_boost + blocking_boost_val
    result: int = max(0, min(100, int(raw)))

    return result


def get_priority_band(action_priority: int) -> str:
    """Derive priority band from action_priority.

    [Plan §8.5, V2.2.2 §9.2] 4-band system applied to action_priority
    (the M5-boosted value, not raw M3 priority_score).

    Args:
        action_priority: Integer in [0, 100].

    Returns:
        Priority band string: CRITICAL, HIGH, MEDIUM, or LOW.
    """
    if action_priority >= SCORE_BAND_CRITICAL:
        return "CRITICAL"
    elif action_priority >= SCORE_BAND_HIGH:
        return "HIGH"
    elif action_priority >= SCORE_BAND_MEDIUM:
        return "MEDIUM"
    else:
        return "LOW"
