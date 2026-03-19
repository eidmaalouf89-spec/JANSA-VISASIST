"""
Step 2: Header Row Detection
Find the anchor row R containing 'document' in column A.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity
from ..config import HEADER_SCAN_MAX_ROW, HEADER_ANCHOR_KEYWORD

logger = logging.getLogger(__name__)


@dataclass
class HeaderAnchor:
    header_row: int       # R: main column headers
    approver_row: int     # R+1: approver names
    sublabel_row: int     # R+2: sub-labels (DATE/N°/STATUT)
    data_start: int       # R+3: first data row


def detect_header_row(ws, sheet_name: str, ctx: PipelineContext) -> Optional[HeaderAnchor]:
    """
    Scan rows 1 to HEADER_SCAN_MAX_ROW in column A for the anchor keyword.
    Returns HeaderAnchor or None if not found.
    """
    # Use iter_rows for a single sequential scan instead of
    # HEADER_SCAN_MAX_ROW individual ws.cell() calls.
    for row_offset, row_tuple in enumerate(
        ws.iter_rows(min_row=1, max_row=HEADER_SCAN_MAX_ROW, values_only=True)
    ):
        cell_value = row_tuple[0] if row_tuple else None
        if cell_value is None:
            continue
        normalized = str(cell_value).strip().lower()
        if HEADER_ANCHOR_KEYWORD in normalized:
            row_idx = row_offset + 1  # 1-based
            logger.info("Sheet '%s': header anchor at row %d", sheet_name, row_idx)
            return HeaderAnchor(
                header_row=row_idx,
                approver_row=row_idx + 1,
                sublabel_row=row_idx + 2,
                data_start=row_idx + 3,
            )

    # No header found
    ctx.log_sheet_level(ImportLogEntry(
        log_id=ctx.next_log_id(),
        sheet=sheet_name,
        row=None,
        column=None,
        severity=Severity.ERROR.value,
        category="header_not_found",
        raw_value=None,
        action_taken="Sheet skipped — no header row containing 'document' found in rows 1-15",
    ))
    ctx.sheets_skipped += 1
    return None
