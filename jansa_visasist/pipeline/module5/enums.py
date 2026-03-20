"""
Module 5 — Enum Definitions [GP8].

All enums used in M5. Each value is GP8-registered.
Out-of-enum values = ERROR.

[Plan §6] Enum ownership:
  - M5-OWNED: SuggestedAction, ProposedVisa, ReasonCode, EscalationLevel, RelanceTemplateId
  - UPSTREAM-CONSUMED: LifecycleState, ConsensusType, Category

validate_enum() enforces GP8 compliance on every enum assignment.
"""

import logging
from typing import FrozenSet

logger = logging.getLogger(__name__)


# ============================================================================
# 6.0 — Enum Value Sets (frozen sets for O(1) membership checks)
# ============================================================================

# --- M5-OWNED ENUMS ---

SUGGESTED_ACTION_VALUES: FrozenSet[str] = frozenset({
    "ISSUE_VISA",
    "CHASE_APPROVERS",
    "ARBITRATE",
    "ESCALATE",
    "HOLD",
})
"""[Plan §6.1] Primary MOEX action recommendation."""

PROPOSED_VISA_VALUES: FrozenSet[str] = frozenset({
    "APPROVE",
    "REJECT",
    "WAIT",
    "NONE",
})
"""[Plan §6.2] Recommended visa type to issue."""

REASON_CODE_VALUES: FrozenSet[str] = frozenset({
    "CONSENSUS_APPROVAL",
    "CONSENSUS_REJECTION",
    "MISSING_RESPONSES",
    "BLOCKING_LOOP",
    "MIXED_CONFLICT",
    "DEGRADED_ANALYSIS",
    "OVERDUE",
    "NOT_YET_STARTED",
})
"""[Plan §6.3] Deterministic category for recommendation."""

ESCALATION_LEVEL_VALUES: FrozenSet[str] = frozenset({
    "NONE",
    "MOEX",
    "DIRECTION",
})
"""[Plan §6.4] Escalation severity."""

RELANCE_TEMPLATE_ID_VALUES: FrozenSet[str] = frozenset({
    "T1", "T2", "T3", "T4", "T5", "T6",
})
"""[Plan §6.5] Template IDs for relance messages."""

PRIORITY_BAND_VALUES: FrozenSet[str] = frozenset({
    "CRITICAL", "HIGH", "MEDIUM", "LOW",
})
"""[Plan §8.5] 4-band priority system for S1 report."""


# --- UPSTREAM-CONSUMED ENUMS ---

LIFECYCLE_STATE_VALUES: FrozenSet[str] = frozenset({
    "NOT_STARTED",
    "WAITING_RESPONSES",
    "READY_TO_ISSUE",
    "READY_TO_REJECT",
    "NEEDS_ARBITRATION",
    "CHRONIC_BLOCKED",
    "ON_HOLD",
    "EXCLUDED",
})
"""[Plan §6.0] M4-defined lifecycle_state enum. Validated on M5 input."""

CONSENSUS_TYPE_VALUES: FrozenSet[str] = frozenset({
    "NOT_STARTED",
    "INCOMPLETE",
    "ALL_HM",
    "MIXED",
    "ALL_REJECT",
    "ALL_APPROVE",
})
"""[Plan §6.0] M3-defined consensus_type enum. Validated on M5 input."""

CATEGORY_VALUES: FrozenSet[str] = frozenset({
    "EASY_WIN_APPROVE",
    "BLOCKED",
    "FAST_REJECT",
    "CONFLICT",
    "WAITING",
    "NOT_STARTED",
})
"""[Plan §6.0] M3-defined category enum. Used for cross-check only."""


# ============================================================================
# 6.6 — Enum Registry (name → allowed values set)
# ============================================================================

_ENUM_REGISTRY: dict[str, FrozenSet[str]] = {
    "suggested_action": SUGGESTED_ACTION_VALUES,
    "proposed_visa": PROPOSED_VISA_VALUES,
    "reason_code": REASON_CODE_VALUES,
    "escalation_level": ESCALATION_LEVEL_VALUES,
    "relance_template_id": RELANCE_TEMPLATE_ID_VALUES,
    "priority_band": PRIORITY_BAND_VALUES,
    "lifecycle_state": LIFECYCLE_STATE_VALUES,
    "consensus_type": CONSENSUS_TYPE_VALUES,
    "category": CATEGORY_VALUES,
}


# ============================================================================
# GP8 Enforcement
# ============================================================================

def validate_enum(value: str, enum_name: str) -> bool:
    """Validate that ``value`` is a member of the named enum.

    [GP8] Out-of-enum = ERROR. Returns True if valid, False if invalid.
    Logs ERROR with the invalid value and enum name on failure.

    Args:
        value: The string value to validate.
        enum_name: Key into _ENUM_REGISTRY (e.g. "suggested_action").

    Returns:
        True if value is valid for the named enum, False otherwise.
    """
    allowed = _ENUM_REGISTRY.get(enum_name)
    if allowed is None:
        logger.error(
            "GP8 violation: unknown enum name '%s'. "
            "Value '%s' cannot be validated.",
            enum_name, value,
        )
        return False

    if value not in allowed:
        logger.error(
            "GP8 violation: value '%s' is not a member of enum '%s'. "
            "Allowed values: %s",
            value, enum_name, sorted(allowed),
        )
        return False

    return True
