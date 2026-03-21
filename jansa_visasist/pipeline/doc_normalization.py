"""
Step 3: Document Reference Normalization
8-step normalization pipeline for the document field.
"""

import re
from typing import Optional

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity


def normalize_document(
    raw_value,
    sheet_name: str,
    source_row: int,
    ctx: PipelineContext,
) -> Optional[str]:
    """
    Normalize a raw document reference string.
    Returns normalized string or None.
    Emits log entries to ctx for any transformations.
    """
    if raw_value is None:
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column="document",
            severity=Severity.INFO.value,
            category="missing_field",
            raw_value=None,
            action_taken="Document reference is null — skipped (decoration row or empty cell)",
        ))
        return None

    val = str(raw_value)

    # Step 1: Strip leading/trailing whitespace
    val = val.strip()

    # Step 2: Uppercase
    val = val.upper()

    # Step 3: Replace spaces and hyphens with underscores
    val = re.sub(r'[\s\-]+', '_', val)

    # Step 4: Collapse repeated underscores
    val = re.sub(r'_+', '_', val)

    # Step 5: Strip trailing punctuation (dots, commas)
    original_before_strip = val
    val = re.sub(r'[.,]+$', '', val)
    if val != original_before_strip:
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column="document",
            severity=Severity.INFO.value,
            category="trailing_punctuation",
            raw_value=str(raw_value),
            action_taken=f"Stripped trailing punctuation: '{original_before_strip}' -> '{val}'",
        ))

    # Step 6: Strip leading/trailing underscores
    val = val.strip('_')

    # Step 7: If starts with '17_T2' or '17T2' (missing P): prepend 'P'
    if val.startswith('17_T2') or val.startswith('17T2'):
        val = 'P' + val
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column="document",
            severity=Severity.INFO.value,
            category="missing_p_prefix",
            raw_value=str(raw_value),
            action_taken=f"Prepended missing 'P' prefix: result = '{val}'",
        ))

    # Step 8: If result is empty → null
    if val == "":
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column="document",
            severity=Severity.ERROR.value,
            category="missing_field",
            raw_value=str(raw_value),
            action_taken="Document reference is empty after normalization — set to null",
        ))
        return None

    return val
