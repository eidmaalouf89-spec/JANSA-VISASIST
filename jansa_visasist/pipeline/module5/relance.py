"""
Module 5 — Layer 4: Relance Logic + Templates T1–T6.

[Plan §2.6, §4, PATCH 7] Determines communication needs, targets, and messages.

Templates: French formal, vouvoiement, ≤200 chars, word-boundary truncation.
Placeholders: {document}, {approver}, {days}, {lot}, {deadline}.
No datetime stamps, no random values, no locale-dependent formatting.
"""

import logging
from typing import Any, Optional

from .constants import (
    RELANCE_MAX_LENGTH,
    RELANCE_NOT_STARTED_DAYS,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Template Definitions [Plan §4.2]
# ============================================================================

RELANCE_TEMPLATES: dict[str, str] = {
    "T1": (
        "Relance : merci de statuer sur le document {document} "
        "(lot {lot}), en attente depuis {days} jours."
    ),
    "T2": (
        "Relance urgente : le document {document} (lot {lot}) "
        "est en attente depuis {days} jours, délai dépassé. Merci de statuer."
    ),
    "T3": (
        "Escalade : le document {document} (lot {lot}) "
        "fait l'objet de {days} jours de blocage. Intervention requise."
    ),
    "T4": (
        "Arbitrage requis : avis divergents sur le document {document} "
        "(lot {lot}). Merci de vous coordonner."
    ),
    "T5": (
        "Premier contact : le document {document} (lot {lot}) "
        "est en attente de visa depuis {days} jours. Merci de statuer."
    ),
    "T6": (
        "Suivi : le document {document} (lot {lot}) "
        "attend encore votre retour depuis {days} jours. Merci de compléter."
    ),
}
"""[Plan §4.2] T1–T6 French formal relance templates."""

# Valid placeholders [Plan §4.1]
VALID_PLACEHOLDERS: frozenset[str] = frozenset({
    "document", "approver", "days", "lot", "deadline",
})


# ============================================================================
# Template Rendering [Plan §4.5]
# ============================================================================

def generate_relance_message(
    template_id: Optional[str],
    params: dict[str, str],
    row_id: str = "?",
) -> Optional[str]:
    """Render a relance message from a template and parameters.

    [Plan §4.5, PATCH 7, Fix 6] Word-boundary truncation at 200 chars.

    Args:
        template_id: Template ID (T1–T6) or None.
        params: Dict with keys from {document, approver, days, lot, deadline}.
        row_id: Item row_id for logging.

    Returns:
        Rendered message string (≤200 chars, UTF-8) or None if template_id is None.
    """
    if template_id is None:
        return None

    template_str = RELANCE_TEMPLATES.get(template_id)
    if template_str is None:
        logger.error(
            "M5: Unknown relance template_id '%s' for row_id=%s. "
            "Returning None.",
            template_id, row_id,
        )
        return None

    # Substitute placeholders [Plan §4.4]
    try:
        message = template_str.format(**params)
    except KeyError as e:
        logger.error(
            "M5: Missing placeholder %s in relance params for row_id=%s. "
            "Using fallback.",
            e, row_id,
        )
        # Fill missing keys with fallbacks
        safe_params = dict(params)
        for key in ("document", "approver", "days", "lot", "deadline"):
            if key not in safe_params:
                safe_params[key] = _placeholder_fallback(key)
        message = template_str.format(**safe_params)

    # Word-boundary truncation [Plan §4.5, Fix 6]
    if len(message) > RELANCE_MAX_LENGTH:
        original_len = len(message)
        message = _truncate_word_boundary(message, RELANCE_MAX_LENGTH)
        logger.warning(
            "M5: Relance message truncated from %d to %d chars for row_id=%s.",
            original_len, len(message), row_id,
        )

    return message


def _truncate_word_boundary(text: str, max_length: int) -> str:
    """Truncate text at word boundary, respecting max_length.

    [Plan §4.5, Fix 6]
    a. Find last whitespace at or before position max_length.
    b. If found: truncate there (no trailing whitespace).
    c. If no whitespace found: hard truncate at max_length.
    d. Result guaranteed ≤ max_length characters.

    Args:
        text: Input text.
        max_length: Maximum character count.

    Returns:
        Truncated text ≤ max_length chars.
    """
    if len(text) <= max_length:
        return text

    # Find last whitespace at or before position max_length
    truncated = text[:max_length]
    last_space = truncated.rfind(" ")

    if last_space > 0:
        return truncated[:last_space].rstrip()
    else:
        # No whitespace found — hard truncate [Plan §4.5c]
        return truncated


def _placeholder_fallback(key: str) -> str:
    """Return fallback value for a missing placeholder.

    [Plan §4.4]

    Args:
        key: Placeholder name.

    Returns:
        Fallback string.
    """
    fallbacks = {
        "document": "DOC_UNKNOWN",
        "lot": "LOT_UNKNOWN",
        "days": "0",
        "approver": "",
        "deadline": "",
    }
    return fallbacks.get(key, "")


# ============================================================================
# Relance Resolution [Plan §2.6]
# ============================================================================

def resolve_relance(
    m3_item: dict[str, Any],
    m4_result: dict[str, Any],
    suggested_action: str,
    is_overdue: bool,
    lifecycle_state: str,
    consecutive_rejections: int,
    g1_blocker_index: dict[str, dict],
    row_id: str = "?",
) -> dict[str, Any]:
    """Determine relance requirements for an item.

    [Plan §2.6] Layer 4 decision table.
    Returns dict with: relance_required, relance_targets, relance_template_id,
    relance_message.

    Args:
        m3_item: M3 row as dict.
        m4_result: M4 analysis_result dict.
        suggested_action: From Layer 2.
        is_overdue: From M3.
        lifecycle_state: The lifecycle_state driving the decision.
        consecutive_rejections: From M4 blocking block.
        g1_blocker_index: G1 blocker data index.
        row_id: Item row_id for logging.

    Returns:
        Dict with relance fields.
    """
    from .validation import safe_get_m3_list, safe_get_m3_int

    # Extract M3 fields
    missing_approvers = safe_get_m3_list(m3_item, "missing_approvers")
    blocking_approvers = safe_get_m3_list(m3_item, "blocking_approvers")
    assigned_approvers = safe_get_m3_list(m3_item, "assigned_approvers")
    days_since_diffusion = safe_get_m3_int(m3_item, "days_since_diffusion", 0)
    days_overdue = safe_get_m3_int(m3_item, "days_overdue", 0)
    document = m3_item.get("document") or "DOC_UNKNOWN"
    source_sheet = m3_item.get("source_sheet") or "LOT_UNKNOWN"

    # Default no-relance result
    no_relance: dict[str, Any] = {
        "relance_required": False,
        "relance_targets": [],
        "relance_template_id": None,
        "relance_message": None,
    }

    # --- Row 4a/4b: CHASE_APPROVERS ---
    if suggested_action == "CHASE_APPROVERS":
        # [Edge Case #11] Empty missing_approvers guard
        if not missing_approvers:
            logger.warning(
                "M5: CHASE_APPROVERS with empty missing_approvers for row_id=%s.",
                row_id,
            )
            return no_relance

        targets = sorted(missing_approvers)
        if is_overdue:
            # Row 4a: overdue → T2
            # [Plan §4.4] T2 {days} = days_since_diffusion (NOT days_overdue)
            template_id = "T2"
            days_param = str(days_since_diffusion)
        else:
            # Row 4b: not overdue → T1
            template_id = "T1"
            days_param = str(days_since_diffusion)

        params = {
            "document": str(document),
            "lot": str(source_sheet),
            "days": days_param,
        }
        message = generate_relance_message(template_id, params, row_id)

        return {
            "relance_required": True,
            "relance_targets": targets,
            "relance_template_id": template_id,
            "relance_message": message,
        }

    # --- Row 4c/4d: ESCALATE ---
    if suggested_action == "ESCALATE":
        from .constants import ESCALATION_CONSEC_REJ_MOEX

        targets: list[str] = []

        # Row 4c: consecutive rejections threshold
        if consecutive_rejections >= ESCALATION_CONSEC_REJ_MOEX:
            targets = sorted(blocking_approvers)

        # Row 4d: G1 systemic blocker detected
        if g1_blocker_index and assigned_approvers:
            systemic_targets = []
            for approver_key in assigned_approvers:
                blocker_data = g1_blocker_index.get(approver_key)
                if blocker_data and blocker_data.get("is_systemic_blocker", False):
                    systemic_targets.append(approver_key)
            if systemic_targets:
                targets = sorted(set(targets) | set(systemic_targets))

        if not targets:
            targets = sorted(blocking_approvers)

        template_id = "T3"
        params = {
            "document": str(document),
            "lot": str(source_sheet),
            "days": str(days_overdue) if days_overdue > 0 else str(days_since_diffusion),
        }
        message = generate_relance_message(template_id, params, row_id)

        return {
            "relance_required": True,
            "relance_targets": targets,
            "relance_template_id": template_id,
            "relance_message": message,
        }

    # --- Row 4e: ARBITRATE ---
    if suggested_action == "ARBITRATE":
        # [OBS-3] targets = responded approvers (assigned - missing)
        missing_set = set(missing_approvers)
        targets = sorted(
            appr for appr in assigned_approvers if appr not in missing_set
        )

        template_id = "T4"
        params = {
            "document": str(document),
            "lot": str(source_sheet),
            "days": str(days_since_diffusion),
        }
        message = generate_relance_message(template_id, params, row_id)

        return {
            "relance_required": True,
            "relance_targets": targets,
            "relance_template_id": template_id,
            "relance_message": message,
        }

    # --- Row 4f/4g: HOLD (NOT_STARTED) ---
    if suggested_action == "HOLD" and lifecycle_state == "NOT_STARTED":
        if days_since_diffusion >= RELANCE_NOT_STARTED_DAYS:
            # Row 4f: days >= 7 → T5
            targets = sorted(assigned_approvers)
            template_id = "T5"
            params = {
                "document": str(document),
                "lot": str(source_sheet),
                "days": str(days_since_diffusion),
            }
            message = generate_relance_message(template_id, params, row_id)

            return {
                "relance_required": True,
                "relance_targets": targets,
                "relance_template_id": template_id,
                "relance_message": message,
            }
        else:
            # Row 4g: days < 7 → no relance
            return no_relance

    # --- Row 4h: HOLD (degraded/on_hold) → no relance ---
    if suggested_action == "HOLD":
        return no_relance

    # --- Row 4i: ISSUE_VISA → no relance ---
    if suggested_action == "ISSUE_VISA":
        return no_relance

    # --- Row 4j: ANY (fallback) → no relance ---
    return no_relance
