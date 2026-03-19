"""
Step 6: Data Row Extraction (Permissive) — OPTIMIZED

Performance changes vs. original:
1. ws.iter_rows(values_only=True) replaces ws.cell(row, col) in a double loop.
   In read_only mode openpyxl parses the sheet XML once, yielding tuples
   sequentially.  The old code did ws.cell() per column per row, forcing a
   full XML seek on every call → O(rows × cols) seeks.
2. Pre-computed 0-based index maps (built once per sheet, reused for every row).
3. Early stopping after EMPTY_ROW_THRESHOLD consecutive empty rows avoids
   scanning thousands of blank trailing rows that openpyxl may report.
"""

import logging
from typing import List, Dict, Tuple

from .column_mapping import ColumnMapping
from .approver_detection import ApproverBlock

logger = logging.getLogger(__name__)

# Stop scanning after this many consecutive empty rows
EMPTY_ROW_THRESHOLD = 30


def _build_index_maps(
    col_mappings: Dict[int, ColumnMapping],
    approver_blocks: List[ApproverBlock],
) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int, int, int]]]:
    """
    Pre-compute 0-based column index maps from 1-based mappings.
    Called ONCE per sheet, not per row.
    Returns:
        core_pairs: [(canonical_key, 0-based-index), ...]
        approver_tuples: [(canonical_key, date_0idx, n_0idx, statut_0idx), ...]
    """
    core_pairs: List[Tuple[str, int]] = []
    for col_idx, mapping in col_mappings.items():
        if mapping.canonical_key is not None:
            core_pairs.append((mapping.canonical_key, col_idx - 1))

    approver_tuples: List[Tuple[str, int, int, int]] = []
    for block in approver_blocks:
        approver_tuples.append((
            block.canonical_key,
            block.date_col - 1,
            block.n_col - 1,
            block.statut_col - 1,
        ))

    return core_pairs, approver_tuples


def _safe_get(row_tuple: tuple, idx: int):
    """Get value from tuple by index; None if out of range."""
    if 0 <= idx < len(row_tuple):
        return row_tuple[idx]
    return None


def extract_rows(
    ws,
    data_start: int,
    col_mappings: Dict[int, ColumnMapping],
    approver_blocks: List[ApproverBlock],
    sheet_name: str,
) -> List[Dict]:
    """
    Extract raw rows starting from data_start using iter_rows.
    A row is included if column A is non-empty (permissive extraction).
    Returns list of raw row dicts with source_row.

    Uses tuple-based access with pre-computed index maps for performance.
    Stops after EMPTY_ROW_THRESHOLD consecutive empty rows.
    """
    # Build index maps ONCE per sheet
    core_pairs, approver_tuples = _build_index_maps(col_mappings, approver_blocks)

    rows: List[Dict] = []
    consecutive_empty = 0

    # ── CRITICAL PERFORMANCE FIX ──
    # Stream rows as value tuples instead of ws.cell() per cell.
    for row_offset, row_tuple in enumerate(
        ws.iter_rows(min_row=data_start, values_only=True)
    ):
        # Check column A (index 0) for emptiness
        col_a = _safe_get(row_tuple, 0)
        is_empty = (col_a is None) or (isinstance(col_a, str) and col_a.strip() == "")

        if is_empty:
            consecutive_empty += 1
            if consecutive_empty >= EMPTY_ROW_THRESHOLD:
                logger.debug(
                    "Sheet '%s': early stop after %d consecutive empty rows at row %d",
                    sheet_name, EMPTY_ROW_THRESHOLD, data_start + row_offset,
                )
                break
            continue

        # Reset on non-empty row
        consecutive_empty = 0

        source_row = data_start + row_offset  # Excel row number (1-based)

        row_data: Dict = {
            "source_row": source_row,
            "source_sheet": sheet_name,
        }

        # Core columns — direct tuple indexing, zero ws.cell() calls
        for canonical_key, idx in core_pairs:
            row_data[canonical_key] = _safe_get(row_tuple, idx)

        # Approver columns — 3 per approver, direct tuple indexing
        for prefix, date_idx, n_idx, statut_idx in approver_tuples:
            row_data[f"{prefix}_date_src"] = _safe_get(row_tuple, date_idx)
            row_data[f"{prefix}_n_src"] = _safe_get(row_tuple, n_idx)
            row_data[f"{prefix}_statut_src"] = _safe_get(row_tuple, statut_idx)

        rows.append(row_data)

        # Batch progress (every 200 rows, not per-row)
        if len(rows) % 200 == 0:
            logger.debug("Sheet '%s': extracted %d rows so far...", sheet_name, len(rows))

    logger.info("Sheet '%s': extracted %d rows (from row %d)", sheet_name, len(rows), data_start)
    return rows
