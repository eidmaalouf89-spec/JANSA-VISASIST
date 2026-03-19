"""
Step 3: Family Grouping & Chain Linking
[SPEC] V2.2 S2.4 Step 3
"""

import logging

import pandas as pd
import numpy as np

from ...config_m2 import NULL_IND_LABEL
from ...context_m2 import Module2Context

logger = logging.getLogger(__name__)


def link_chains(df: pd.DataFrame, ctx: Module2Context) -> pd.DataFrame:
    """
    Add doc_version_key, previous_version_key, is_latest, revision_count.
    Group by (doc_family_key, source_sheet).
    Fully vectorized where possible.
    """
    n = len(df)

    # ── doc_version_key: vectorized construction ──
    # [SAFEGUARD] Use NULL_IND_LABEL when ind is null
    ind_str = df["ind"].fillna(NULL_IND_LABEL).astype(str)
    df["doc_version_key"] = df["doc_family_key"] + "::" + ind_str + "::" + df["source_sheet"]

    # ── Pre-sort once, reuse everywhere ──
    df.sort_values(["doc_family_key", "source_sheet", "ind_sort_order", "source_row"],
                   inplace=True, kind="mergesort")
    df.reset_index(drop=True, inplace=True)

    # ── Grouped computations using vectorized pandas ──
    grp = df.groupby(["doc_family_key", "source_sheet"], sort=False)

    # revision_count: distinct ind values per group (null counts as one)
    df["revision_count"] = grp["ind"].transform(lambda x: x.nunique(dropna=False)).astype(int)

    # is_latest: rows at the max ind_sort_order per group
    max_order = grp["ind_sort_order"].transform("max")
    df["is_latest"] = df["ind_sort_order"] == max_order

    # ── previous_version_key: vectorized chain linking ──
    # Within each group (already sorted by ind_sort_order, source_row),
    # we need: for each row, the doc_version_key of the previous distinct ind_sort_order.
    # Strategy: build a shifted mapping of distinct sort_order -> version_key per group.

    prev_vkey = np.empty(n, dtype=object)
    prev_vkey[:] = None

    group_starts = grp.ngroup()  # integer group ID per row
    sort_orders = df["ind_sort_order"].values
    vkeys = df["doc_version_key"].values
    group_ids = group_starts.values

    # Walk once through the sorted data to build previous_version_key
    if n > 0:
        current_group = group_ids[0]
        current_order = sort_orders[0]
        prev_order_vkey = None  # vkey of previous distinct sort_order
        current_order_vkey = vkeys[0]  # vkey of current sort_order (first row)

        for i in range(n):
            g = group_ids[i]
            o = sort_orders[i]

            if g != current_group:
                # New group: reset
                current_group = g
                prev_order_vkey = None
                current_order = o
                current_order_vkey = vkeys[i]
            elif o != current_order:
                # Same group, new sort_order
                prev_order_vkey = current_order_vkey
                current_order = o
                current_order_vkey = vkeys[i]

            prev_vkey[i] = prev_order_vkey

    # Pandas coerces None -> NaN in mixed-type object columns on bulk assign.
    # Fix: cast to object dtype then use .where() to replace NaN with None.
    df["previous_version_key"] = list(prev_vkey)
    df["previous_version_key"] = df["previous_version_key"].astype(object)
    df["previous_version_key"] = df["previous_version_key"].where(
        pd.notna(df["previous_version_key"]),
        None
    )

    ctx.family_count = grp.ngroups
    logger.info("Step 3: Linked chains for %d family-sheet groups.", grp.ngroups)
    return df
