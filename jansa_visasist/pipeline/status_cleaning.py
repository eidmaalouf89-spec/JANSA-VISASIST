"""
Steps 7c + 7d: VISA GLOBAL and Approver STATUT normalization.
"""

import re
from typing import Optional

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity
from ..config import VISA_GLOBAL_VALUES, APPROVER_STATUS_SYNONYMS, AMBIGUOUS_STATUSES


def normalize_visa_global(
    raw_value,
    sheet_name: str,
    source_row: int,
    ctx: PipelineContext,
) -> Optional[str]:
    """
    Step 7c: Normalize visa_global field.
    Returns one of {VSO, VAO, REF, HM, SUS, DEF, FAV} or None.
    """
    # Blank → None (GP2)
    if raw_value is None:
        return None

    val = str(raw_value).strip()
    if val == "":
        return None

    # Uppercase
    upper_val = val.upper()

    # Exact match
    if upper_val in VISA_GLOBAL_VALUES:
        return upper_val

    # Unknown value
    ctx.log(ImportLogEntry(
        log_id=ctx.next_log_id(),
        sheet=sheet_name,
        row=source_row,
        column="visa_global",
        severity=Severity.WARNING.value,
        category="unknown_status",
        raw_value=str(raw_value),
        action_taken=f"Unknown visa_global value '{raw_value}' — set to null",
    ))
    return None


def normalize_approver_statut(
    raw_value,
    approver_key: str,
    sheet_name: str,
    source_row: int,
    ctx: PipelineContext,
) -> Optional[str]:
    """
    Step 7d: Normalize an individual approver STATUT field.
    Returns normalized status or None.
    """
    # Blank → None (GP2)
    if raw_value is None:
        return None

    val = str(raw_value)

    # Step 1: Trim
    val = val.strip()
    if val == "":
        return None

    # Step 6 (check early): Multi-value (contains newline) → split, take last
    if '\n' in val or '\r' in val:
        parts = re.split(r'[\n\r]+', val)
        parts = [p.strip() for p in parts if p.strip()]
        if len(parts) > 1:
            ctx.log(ImportLogEntry(
                log_id=ctx.next_log_id(),
                sheet=sheet_name,
                row=source_row,
                column=f"{approver_key}_statut",
                severity=Severity.INFO.value,
                category="multi_value_status",
                raw_value=str(raw_value),
                action_taken=f"Multi-value status detected: {parts}. Taking last value: '{parts[-1]}'",
            ))
            val = parts[-1]
        elif parts:
            val = parts[0]
        else:
            return None

    # Step 2: Strip leading punctuation and parentheses
    val = re.sub(r'^[.,;\-\(\)]+', '', val)

    # Step 3: Remove internal spaces
    val = val.replace(' ', '')

    # Step 4: Uppercase
    val = val.upper()

    # Step 5: Map synonyms via canonical dictionary
    if val in APPROVER_STATUS_SYNONYMS:
        return APPROVER_STATUS_SYNONYMS[val]

    # Step 7: Ambiguous typos → null + WARNING
    if val in AMBIGUOUS_STATUSES:
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column=f"{approver_key}_statut",
            severity=Severity.WARNING.value,
            category="ambiguous_status",
            raw_value=str(raw_value),
            action_taken=f"Ambiguous approver status '{val}' — set to null",
        ))
        return None

    # Step 8: Validate against vocabulary (check if it's in the synonym values)
    valid_statuses = set(APPROVER_STATUS_SYNONYMS.values())
    if val in valid_statuses:
        return val

    # Unknown
    ctx.log(ImportLogEntry(
        log_id=ctx.next_log_id(),
        sheet=sheet_name,
        row=source_row,
        column=f"{approver_key}_statut",
        severity=Severity.WARNING.value,
        category="unknown_status",
        raw_value=str(raw_value),
        action_taken=f"Unknown approver status '{val}' — set to null",
    ))
    return None
