"""
Step 5: Approver Block Detection
Detect approver blocks from row R+1, right of visa_global.
"""

import logging
from dataclasses import dataclass
from typing import List, Dict, Optional

from rapidfuzz import fuzz

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity
from ..config import (
    APPROVER_VARIANT_MAP,
    CANONICAL_APPROVERS,
    FUZZY_THRESHOLD,
)
from .column_mapping import ColumnMapping

logger = logging.getLogger(__name__)


@dataclass
class ApproverBlock:
    canonical_key: str
    raw_name: str
    date_col: int     # Source column index for DATE
    n_col: int        # Source column index for N°
    statut_col: int   # Source column index for STATUT


def _match_approver_name(raw_name: str) -> Optional[str]:
    """
    Match a raw approver name to a canonical key.
    First tries exact/variant lookup, then fuzzy.
    """
    cleaned = raw_name.strip()
    upper = cleaned.upper()

    # Exact/variant lookup
    if cleaned in APPROVER_VARIANT_MAP:
        return APPROVER_VARIANT_MAP[cleaned]
    if upper in APPROVER_VARIANT_MAP:
        return APPROVER_VARIANT_MAP[upper]

    # Try normalized form (replace hyphens, collapse spaces)
    normalized = " ".join(cleaned.upper().replace("-", " ").split())
    if normalized in APPROVER_VARIANT_MAP:
        return APPROVER_VARIANT_MAP[normalized]

    # Fuzzy match against canonical display names
    best_key = None
    best_score = 0.0
    for variant, canonical in APPROVER_VARIANT_MAP.items():
        score = fuzz.ratio(upper, variant.upper()) / 100.0
        if score >= FUZZY_THRESHOLD and score > best_score:
            best_score = score
            best_key = canonical

    return best_key


def detect_approver_blocks(
    ws,
    approver_row: int,
    col_mappings: Dict[int, ColumnMapping],
    sheet_name: str,
    ctx: PipelineContext,
) -> List[ApproverBlock]:
    """
    Scan row R+1 for approver names. Each approver owns 3 consecutive columns.
    Returns list of ApproverBlock.
    """
    # Find visa_global column index
    visa_col = None
    obs_col = None
    for col_idx, mapping in col_mappings.items():
        if mapping.canonical_key == "visa_global":
            visa_col = col_idx
        if mapping.canonical_key == "observations":
            obs_col = col_idx

    if visa_col is None:
        ctx.log_sheet_level(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=None,
            column=None,
            severity=Severity.WARNING.value,
            category="no_visa_global_column",
            raw_value=None,
            action_taken="Cannot detect approver blocks — visa_global column not found",
        ))
        return []

    # Read the approver row as a single tuple (one iter_rows call
    # instead of N individual ws.cell() calls).
    approver_row_values = None
    for row_tuple in ws.iter_rows(
        min_row=approver_row, max_row=approver_row, values_only=True
    ):
        approver_row_values = row_tuple
        break

    if approver_row_values is None:
        return []

    blocks: List[ApproverBlock] = []
    max_col = len(approver_row_values)
    col_idx = visa_col + 1  # 1-based

    while col_idx <= max_col:
        # Stop if we've reached observations column
        if obs_col is not None and col_idx >= obs_col:
            break

        # 0-based tuple access
        cell_value = approver_row_values[col_idx - 1] if col_idx - 1 < max_col else None
        if cell_value is not None and str(cell_value).strip():
            raw_name = str(cell_value).strip()
            canonical = _match_approver_name(raw_name)

            if canonical:
                block = ApproverBlock(
                    canonical_key=canonical,
                    raw_name=raw_name,
                    date_col=col_idx,
                    n_col=col_idx + 1,
                    statut_col=col_idx + 2,
                )
                blocks.append(block)
                logger.debug("Sheet '%s': approver '%s' -> %s at cols %d-%d",
                             sheet_name, raw_name, canonical, col_idx, col_idx + 2)
                # Skip past this approver's 3 columns
                col_idx += 3
                continue
            else:
                ctx.log_sheet_level(ImportLogEntry(
                    log_id=ctx.next_log_id(),
                    sheet=sheet_name,
                    row=None,
                    column=f"col_{col_idx}",
                    severity=Severity.WARNING.value,
                    category="unknown_approver",
                    raw_value=raw_name,
                    action_taken=f"Approver name '{raw_name}' not matched to any canonical key",
                ))

        col_idx += 1

    logger.info("Sheet '%s': detected %d approver blocks", sheet_name, len(blocks))
    return blocks
