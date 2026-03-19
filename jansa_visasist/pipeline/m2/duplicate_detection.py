"""
Step 5: Duplicate Detection (executed BEFORE chain linking)
[SPEC] V2.2 S2.4 Step 5

AUTHORITATIVE duplicate comparison rule:
Compare all columns present in the DataFrame at Step 5 execution time,
excluding row_id, source_row (unique by definition), and any M2-derived
columns already present (doc_family_key, ind_sort_order).
Everything else from M1 participates - including row_quality,
row_quality_details, all _raw fields, and all approver columns.
"""

import logging

import pandas as pd
import numpy as np

from ...config_m2 import (
    DUPLICATE_EXCLUDE_COLS,
    M2_DERIVED_COLS_STEP5,
    ANOMALY_DUPLICATE_EXACT,
    ANOMALY_DUPLICATE_SUSPECT,
)
from ...context_m2 import Module2Context
from ...models.linking_anomaly import LinkingAnomalyEntry

logger = logging.getLogger(__name__)


def _serialize_cell(val):
    """Convert a cell value to a hashable, null-normalized form for comparison."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, list):
        return tuple(val)
    if isinstance(val, np.ndarray):
        return tuple(val.tolist())
    try:
        if pd.isna(val):
            return None
    except (ValueError, TypeError):
        pass
    return val


def detect_duplicates(df: pd.DataFrame, ctx: Module2Context) -> pd.DataFrame:
    """Add duplicate_flag column: UNIQUE / DUPLICATE / SUSPECT."""
    # Build exclusion set at runtime
    exclude = DUPLICATE_EXCLUDE_COLS | M2_DERIVED_COLS_STEP5
    compare_cols = [c for c in df.columns if c not in exclude]

    df["duplicate_flag"] = "UNIQUE"  # Default

    # Use a string-based grouping key for ind to handle nulls
    ind_group = df["ind"].fillna("__NULL__")
    group_sizes = df.groupby(["doc_family_key", ind_group, "source_sheet"], sort=False).size()

    # Only process groups with 2+ rows (vast majority are size 1)
    multi_groups = group_sizes[group_sizes >= 2]
    if multi_groups.empty:
        logger.info("Step 5: Duplicate detection. 0 DUPLICATE, 0 SUSPECT rows.")
        return df

    # Pre-serialize comparison columns into tuples for fast hashing
    # Build a composite key per row: tuple of serialized comparison values
    serialized = {}
    # Only compute for rows in multi-row groups
    multi_keys = set(multi_groups.index)
    # Build a temporary group-key column
    group_key_series = list(zip(df["doc_family_key"], ind_group, df["source_sheet"]))

    # Identify row indices that belong to multi-row groups
    multi_row_mask = np.array([gk in multi_keys for gk in group_key_series])
    if not multi_row_mask.any():
        logger.info("Step 5: Duplicate detection. 0 DUPLICATE, 0 SUSPECT rows.")
        return df

    # Extract only the rows we need to compare
    multi_indices = df.index[multi_row_mask]

    # Pre-compute serialized comparison tuples for multi-row entries only
    compare_data = df.loc[multi_indices, compare_cols]
    row_tuples = {}
    for idx in multi_indices:
        row_tuples[idx] = tuple(_serialize_cell(compare_data.at[idx, c]) for c in compare_cols)

    exact_count = 0
    suspect_count = 0

    # Group only the multi-row subset
    for (fam_key, ind_val, sheet), size in multi_groups.items():
        # Get indices for this group
        mask = (df["doc_family_key"].values == fam_key) & \
               (ind_group.values == ind_val) & \
               (df["source_sheet"].values == sheet)
        group_indices = df.index[mask]

        # Sort by source_row
        group_source_rows = df.loc[group_indices, "source_row"]
        sorted_indices = group_indices[group_source_rows.argsort()]

        ref_idx = sorted_indices[0]
        ref_tuple = row_tuples[ref_idx]

        all_identical = True
        differing_columns = set()

        for other_idx in sorted_indices[1:]:
            other_tuple = row_tuples[other_idx]
            for ci, col in enumerate(compare_cols):
                if ref_tuple[ci] != other_tuple[ci]:
                    all_identical = False
                    differing_columns.add(col)

        row_ids = [str(df.at[i, "row_id"]) for i in sorted_indices]
        real_ind = None if ind_val == "__NULL__" else ind_val

        if all_identical:
            for i, idx in enumerate(sorted_indices):
                if i > 0:
                    df.at[idx, "duplicate_flag"] = "DUPLICATE"
            exact_count += len(sorted_indices) - 1
            ctx.log_anomaly(LinkingAnomalyEntry(
                anomaly_id=ctx.next_anomaly_id(),
                anomaly_type=ANOMALY_DUPLICATE_EXACT,
                doc_family_key=str(fam_key),
                source_sheet=str(sheet),
                row_id=None,
                ind=real_ind,
                severity="WARNING",
                details={"row_ids": row_ids, "duplicate_count": len(sorted_indices)},
            ))
        else:
            for idx in sorted_indices:
                df.at[idx, "duplicate_flag"] = "SUSPECT"
            suspect_count += len(sorted_indices)
            ctx.log_anomaly(LinkingAnomalyEntry(
                anomaly_id=ctx.next_anomaly_id(),
                anomaly_type=ANOMALY_DUPLICATE_SUSPECT,
                doc_family_key=str(fam_key),
                source_sheet=str(sheet),
                row_id=None,
                ind=real_ind,
                severity="WARNING",
                details={
                    "row_ids": row_ids,
                    "differing_columns": sorted(differing_columns),
                },
            ))

    ctx.duplicate_exact_count = exact_count
    ctx.duplicate_suspect_count = suspect_count
    logger.info(
        "Step 5: Duplicate detection. %d DUPLICATE, %d SUSPECT rows.",
        exact_count, suspect_count,
    )
    return df
