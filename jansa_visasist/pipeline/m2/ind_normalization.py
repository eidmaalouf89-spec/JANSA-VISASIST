"""
Step 2: Revision Index Normalization
[SPEC] V2.2 §2.4 Step 2
"""

import logging

import pandas as pd
import numpy as np

from ...config_m2 import ANOMALY_MISSING_IND
from ...context_m2 import Module2Context
from ...models.linking_anomaly import LinkingAnomalyEntry

logger = logging.getLogger(__name__)


def _alpha_to_sort_order(s: str) -> int:
    """[SPEC] A=1, B=2, ..., Z=26, AA=27, AB=28, ..."""
    result = 0
    for char in s.upper():
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result


def _compute_single(val):
    """Return (sort_order, is_missing) for one IND value."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0, True
    s = str(val).strip()
    if not s:
        return 0, True
    if s.isdigit():
        return int(s), False
    if s.isalpha():
        return _alpha_to_sort_order(s), False
    # [SAFEGUARD] Unexpected format
    return 0, False


def compute_sort_orders(df: pd.DataFrame, ctx: Module2Context) -> pd.DataFrame:
    """Add ind_sort_order column to every row. Vectorized with Python fallback for anomaly logging."""
    # Vectorized computation using apply on the Series (much faster than row-by-row df.at)
    ind_series = df["ind"]
    results = ind_series.map(_compute_single)  # Returns Series of (sort_order, is_missing) tuples

    df["ind_sort_order"] = results.map(lambda x: x[0]).astype(int)
    missing_mask = results.map(lambda x: x[1])

    # Log anomalies only for missing rows (typically ~13)
    missing_count = missing_mask.sum()
    if missing_count > 0:
        missing_df = df.loc[missing_mask]
        for idx in missing_df.index:
            doc_raw = df.at[idx, "document_raw"]
            ctx.log_anomaly(LinkingAnomalyEntry(
                anomaly_id=ctx.next_anomaly_id(),
                anomaly_type=ANOMALY_MISSING_IND,
                doc_family_key=str(df.at[idx, "doc_family_key"]),
                source_sheet=str(df.at[idx, "source_sheet"]),
                row_id=str(df.at[idx, "row_id"]),
                ind=None,
                severity="WARNING",
                details={
                    "source_row": int(df.at[idx, "source_row"]),
                    "document_raw": str(doc_raw) if pd.notna(doc_raw) else None,
                },
            ))

    logger.info("Step 2: Computed sort orders. %d MISSING_IND.", missing_count)
    return df
