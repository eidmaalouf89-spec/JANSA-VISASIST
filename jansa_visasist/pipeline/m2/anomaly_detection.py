"""
Step 6: Anomaly Detection
[SPEC] V2.2 S2.4 Step 6

Detects: REVISION_GAP, LATE_FIRST_APPEARANCE, DATE_REGRESSION.
(DUPLICATE_EXACT/SUSPECT logged in Step 5; MISSING_IND in Step 2;
UNPARSEABLE_DOCUMENT in Step 1.)
"""

import logging

import pandas as pd
import numpy as np

from ...config_m2 import (
    UNPARSEABLE_PREFIX,
    ANOMALY_REVISION_GAP,
    ANOMALY_LATE_FIRST,
    ANOMALY_DATE_REGRESSION,
)
from ...context_m2 import Module2Context
from ...models.linking_anomaly import LinkingAnomalyEntry

logger = logging.getLogger(__name__)


def detect_anomalies(df: pd.DataFrame, ctx: Module2Context) -> pd.DataFrame:
    """Detect REVISION_GAP, LATE_FIRST_APPEARANCE, DATE_REGRESSION.

    Assumes df is already sorted by (doc_family_key, source_sheet, ind_sort_order, source_row)
    from chain_linking step.
    """
    gap_count = 0
    late_count = 0
    regression_count = 0

    has_dates = "date_diffusion" in df.columns

    # Filter out UNPARSEABLE families upfront (they're single-row, skip entirely)
    valid_mask = ~df["doc_family_key"].str.startswith(UNPARSEABLE_PREFIX)
    valid_df = df.loc[valid_mask]

    if valid_df.empty:
        logger.info("Step 6: Anomaly detection. 0 REVISION_GAP, 0 LATE_FIRST, 0 DATE_REGRESSION.")
        return df

    # Already sorted from chain_linking; group on the filtered subset
    groups = valid_df.groupby(["doc_family_key", "source_sheet"], sort=False)

    # Pre-extract numpy arrays for fast access within loops
    sort_order_arr = valid_df["ind_sort_order"].values
    ind_arr = valid_df["ind"].values
    fam_arr = valid_df["doc_family_key"].values
    sheet_arr = valid_df["source_sheet"].values
    date_arr = valid_df["date_diffusion"].values if has_dates else None

    for (fam_key, sheet), group_idx in groups.groups.items():
        # Use positional indexing on the valid_df subset
        g_sort_orders = sort_order_arr[valid_df.index.get_indexer(group_idx)]
        g_inds = ind_arr[valid_df.index.get_indexer(group_idx)]

        # Distinct sort orders excluding 0 (null IND)
        distinct_orders = sorted(set(g_sort_orders) - {0})

        if not distinct_orders:
            continue

        # LATE_FIRST_APPEARANCE: first sort order > 1
        if distinct_orders[0] > 1:
            # Find the ind at the first sort order
            first_order = distinct_orders[0]
            first_ind_val = None
            for so, iv in zip(g_sort_orders, g_inds):
                if so == first_order:
                    first_ind_val = iv if pd.notna(iv) else None
                    break
            ctx.log_anomaly(LinkingAnomalyEntry(
                anomaly_id=ctx.next_anomaly_id(),
                anomaly_type=ANOMALY_LATE_FIRST,
                doc_family_key=str(fam_key),
                source_sheet=str(sheet),
                row_id=None,
                ind=str(first_ind_val) if first_ind_val is not None else None,
                severity="WARNING",
                details={
                    "first_ind": str(first_ind_val) if first_ind_val is not None else None,
                    "first_sort_order": int(first_order),
                },
            ))
            late_count += 1

        # REVISION_GAP: consecutive sort orders with jump > 1
        for i in range(1, len(distinct_orders)):
            prev_o = distinct_orders[i - 1]
            curr_o = distinct_orders[i]
            if curr_o - prev_o > 1:
                ctx.log_anomaly(LinkingAnomalyEntry(
                    anomaly_id=ctx.next_anomaly_id(),
                    anomaly_type=ANOMALY_REVISION_GAP,
                    doc_family_key=str(fam_key),
                    source_sheet=str(sheet),
                    row_id=None,
                    ind=None,
                    severity="WARNING",
                    details={
                        "prev_sort_order": int(prev_o),
                        "curr_sort_order": int(curr_o),
                        "jump_size": int(curr_o - prev_o),
                    },
                ))
                gap_count += 1

        # DATE_REGRESSION: later revision with earlier date
        if has_dates:
            g_dates = date_arr[valid_df.index.get_indexer(group_idx)]
            # Filter to non-null dates, already in sort order
            prev_date = None
            prev_ind = None
            for so, iv, dt in zip(g_sort_orders, g_inds, g_dates):
                if dt is None or (isinstance(dt, float) and pd.isna(dt)) or pd.isna(dt):
                    continue
                if prev_date is not None and dt < prev_date:
                    ctx.log_anomaly(LinkingAnomalyEntry(
                        anomaly_id=ctx.next_anomaly_id(),
                        anomaly_type=ANOMALY_DATE_REGRESSION,
                        doc_family_key=str(fam_key),
                        source_sheet=str(sheet),
                        row_id=None,
                        ind=None,
                        severity="WARNING",
                        details={
                            "earlier_ind": str(prev_ind) if prev_ind is not None else None,
                            "earlier_date": str(prev_date),
                            "later_ind": str(iv) if pd.notna(iv) else None,
                            "later_date": str(dt),
                        },
                    ))
                    regression_count += 1
                prev_date = dt
                prev_ind = iv if pd.notna(iv) else None

    logger.info(
        "Step 6: Anomaly detection. %d REVISION_GAP, %d LATE_FIRST, %d DATE_REGRESSION.",
        gap_count, late_count, regression_count,
    )
    return df
