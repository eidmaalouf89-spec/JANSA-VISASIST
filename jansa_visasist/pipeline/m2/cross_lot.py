"""
Step 4: Cross-Lot Detection
[SPEC] V2.2 S2.4 Step 4
"""

import logging

import pandas as pd
import numpy as np

from ...context_m2 import Module2Context

logger = logging.getLogger(__name__)


def detect_cross_lot(df: pd.DataFrame, ctx: Module2Context) -> pd.DataFrame:
    """
    Add is_cross_lot and cross_lot_sheets columns.
    [SPEC] GP2: cross_lot_sheets MUST be null when is_cross_lot = false.
    """
    # Build mapping: family_key -> sorted list of distinct sheets
    family_sheets = (
        df.groupby("doc_family_key")["source_sheet"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .to_dict()
    )

    # Vectorized: map family_key -> sheet count
    sheet_counts = {k: len(v) for k, v in family_sheets.items()}
    count_series = df["doc_family_key"].map(sheet_counts)

    df["is_cross_lot"] = count_series > 1

    # cross_lot_sheets: list for cross-lot, None for single-lot (GP2)
    df["cross_lot_sheets"] = df["doc_family_key"].map(
        lambda k: family_sheets[k] if sheet_counts[k] > 1 else None
    )

    # [SPEC] GP2 runtime assertion
    non_cross = df.loc[~df["is_cross_lot"], "cross_lot_sheets"]
    assert non_cross.isna().all(), \
        "GP2 violation: cross_lot_sheets is not null for some non-cross-lot rows"

    cross_count = sum(1 for c in sheet_counts.values() if c > 1)
    ctx.cross_lot_count = cross_count
    logger.info("Step 4: Cross-lot detection. %d families in multiple sheets.", cross_count)
    return df
