"""
Step 7a: Structural Document Validation
Validate normalized document references against structural rules.
Emits logs only — does NOT assign row_quality directly.
"""

import re
from typing import Optional

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity
from ..config import DOC_MIN_LENGTH, DOC_MAX_NOISE_RATIO


def validate_document(
    document: Optional[str],
    document_raw,
    sheet_name: str,
    source_row: int,
    ctx: PipelineContext,
) -> Optional[str]:
    """
    Validate a normalized document against structural rules R1-R4.
    Returns the document if valid, or None if invalid.
    Emits WARNING log entries for invalid documents.
    """
    # If document is already None (flagged by Step 3), skip validation
    if document is None:
        return None

    raw_str = str(document_raw) if document_raw is not None else ""

    # R1: Too short (< 10 characters)
    if len(document) < DOC_MIN_LENGTH:
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column="document",
            severity=Severity.WARNING.value,
            category="unparseable_document",
            raw_value=raw_str,
            action_taken=f"Set document to null — failed structural rule R1 (length {len(document)} < {DOC_MIN_LENGTH})",
        ))
        return None

    # R2: No numeric segment (no digit found)
    if not re.search(r'[0-9]', document):
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column="document",
            severity=Severity.WARNING.value,
            category="unparseable_document",
            raw_value=raw_str,
            action_taken="Set document to null — failed structural rule R2 (no numeric segment)",
        ))
        return None

    # R3: No alphabetic segment (no letter found)
    if not re.search(r'[A-Z]', document):
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column="document",
            severity=Severity.WARNING.value,
            category="unparseable_document",
            raw_value=raw_str,
            action_taken="Set document to null — failed structural rule R3 (no alphabetic segment)",
        ))
        return None

    # R4: High noise ratio (> 30% non-alphanumeric, excluding underscores)
    non_underscore_chars = document.replace('_', '')
    if len(non_underscore_chars) > 0:
        non_alnum_count = sum(1 for c in non_underscore_chars if not c.isalnum())
        noise_ratio = non_alnum_count / len(non_underscore_chars)
        if noise_ratio > DOC_MAX_NOISE_RATIO:
            ctx.log(ImportLogEntry(
                log_id=ctx.next_log_id(),
                sheet=sheet_name,
                row=source_row,
                column="document",
                severity=Severity.WARNING.value,
                category="unparseable_document",
                raw_value=raw_str,
                action_taken=f"Set document to null — failed structural rule R4 (noise ratio {noise_ratio:.2f} > {DOC_MAX_NOISE_RATIO})",
            ))
            return None

    return document
