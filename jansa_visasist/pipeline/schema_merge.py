"""
Step 9: Schema Union & Merge
Merge all per-sheet DataFrames into a single master dataset.
"""

import logging
from typing import List

import pandas as pd

from ..config import CORE_COLUMNS, CANONICAL_APPROVERS, APPROVER_OUTPUT_SUFFIXES

logger = logging.getLogger(__name__)


def build_full_schema() -> List[str]:
    """Build the complete output schema: core columns + all approver output columns."""
    schema = list(CORE_COLUMNS)
    for approver in CANONICAL_APPROVERS:
        for suffix in APPROVER_OUTPUT_SUFFIXES:
            col_name = f"{approver}{suffix}"
            if col_name not in schema:
                schema.append(col_name)
    return schema


def merge_sheets(sheet_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge all per-sheet DataFrames using schema union.
    All 14 approver keys × 5 output columns are always present.
    Missing approver columns → None (structural null).
    """
    if not sheet_dfs:
        full_schema = build_full_schema()
        return pd.DataFrame(columns=full_schema)

    # Concatenate all sheets
    master = pd.concat(sheet_dfs, ignore_index=True)

    # Ensure all schema columns exist
    full_schema = build_full_schema()
    for col in full_schema:
        if col not in master.columns:
            master[col] = None

    # GP2 enforcement: no empty strings in normalized fields
    # Replace empty strings with None across the entire DataFrame
    master = master.replace("", None)

    # Reorder columns: schema columns first, then any extras
    ordered_cols = [c for c in full_schema if c in master.columns]
    extra_cols = [c for c in master.columns if c not in full_schema]
    master = master[ordered_cols + extra_cols]

    logger.info("Merged master dataset: %d rows, %d columns", len(master), len(master.columns))
    return master
