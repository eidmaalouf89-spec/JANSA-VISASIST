"""
Module 5 — Named Constants Registry.

Single source of truth for ALL thresholds, weights, limits, and band boundaries.
No magic numbers anywhere in M5 code — every value references this module.

[Plan §8] All constants consolidated here.
"""

# ============================================================================
# 8.1 — Confidence Formula Constants [PATCH 3, Layer 6]
# ============================================================================

BASE_CONFIDENCE: float = 0.30
"""Starting point: moderate confidence before adjustments."""

W_CONSENSUS: float = 0.35
"""Strong consensus is the strongest confidence signal."""

W_COMPLETENESS: float = 0.20
"""Full response coverage boosts confidence."""

W_MISSING: float = 0.15
"""Missing approvers reduce confidence proportionally."""

W_CONFLICT: float = 0.25
"""Conflict is a major confidence penalty."""

W_OVERDUE: float = 0.10
"""Overdue items have slightly less confident recommendations."""

W_DEGRADED: float = 0.50
"""Degraded analysis severely reduces confidence."""

OVERDUE_PENALTY_CAP: int = 60
"""Overdue penalty maxes out at 60 days."""


# ============================================================================
# 8.2 — Escalation Threshold Constants [PATCH 4, Layer 5]
# ============================================================================

ESCALATION_CONSEC_REJ_DIR: int = 3
"""Consecutive rejections threshold → DIRECTION escalation (Layer 5, rule 1)."""

ESCALATION_CONSEC_REJ_MOEX: int = 2
"""Consecutive rejections threshold → MOEX escalation (Layer 5, rule 4)."""

ESCALATION_OVERDUE_DIR: int = 60
"""Days overdue threshold → DIRECTION escalation (Layer 5, rule 2)."""

ESCALATION_OVERDUE_MOEX: int = 30
"""Days overdue threshold → MOEX escalation (Layer 5, rule 5)."""

ESCALATION_SYSTEMIC_DIR: int = 10
"""G1 blocked items count → DIRECTION escalation (Layer 5, rule 3)."""

ESCALATION_SYSTEMIC_MOEX: int = 5
"""G1 blocked items count → MOEX escalation (Layer 5, rule 6)."""


# ============================================================================
# 8.3 — Action Priority Constants [PATCH 5]
# ============================================================================

ESCALATION_BOOST_MOEX: int = 10
"""Moderate boost for MOEX-level escalation."""

ESCALATION_BOOST_DIR: int = 20
"""Strong boost for DIRECTION-level escalation."""

OVERDUE_CAP: int = 30
"""Cap overdue contribution at 30 days."""

OVERDUE_MULTIPLIER: float = 0.5
"""Each overdue day adds 0.5 pts (max 15)."""

BLOCKING_BOOST: int = 5
"""Flat boost when blockers present."""


# ============================================================================
# 8.4 — Relance Constants [PATCH 7, Layer 4]
# ============================================================================

RELANCE_NOT_STARTED_DAYS: int = 7
"""Wait 7 days before first contact on NOT_STARTED items (Layer 4, row 4f/4g)."""

RELANCE_MAX_LENGTH: int = 200
"""Maximum relance message length in characters (after parameter substitution)."""


# ============================================================================
# 8.5 — Priority Band Thresholds [V2.2.2 §9.2, S1 Report]
# ============================================================================

SCORE_BAND_CRITICAL: int = 80
"""action_priority >= 80 → CRITICAL band."""

SCORE_BAND_HIGH: int = 60
"""action_priority >= 60 → HIGH band."""

SCORE_BAND_MEDIUM: int = 40
"""action_priority >= 40 → MEDIUM band."""

SCORE_BAND_LOW: int = 0
"""action_priority >= 0 → LOW band (default/floor)."""
