"""
Step 4: Consensus Decision Tree
[SPEC] V2.2 §3 Step 3 (Consensus)
"""

import logging

import pandas as pd
import numpy as np

from ...config_m3 import VALID_CONSENSUS_TYPES

logger = logging.getLogger(__name__)


def _determine_consensus(
    replied_among_relevant: int,
    pending_among_relevant: int,
    relevant_count: int,
    ref_count: int,
    vso_vao_count: int,
) -> str:
    """
    Pure function: apply first-match-wins decision tree.
    Returns consensus_type string.
    Uses replied_among_relevant (not global replied) for rule 1,
    and pending_among_relevant (not global pending) for rule 2.
    """
    # Rule 1: No relevant approver has replied
    if replied_among_relevant == 0:
        return "NOT_STARTED"

    # Rule 2: Some relevant approvers still pending
    if pending_among_relevant > 0:
        return "INCOMPLETE"

    # Rule 3: No relevant approvers (safety net — should be excluded earlier)
    if relevant_count == 0:
        return "ALL_HM"

    # Rule 4: Mixed — both reject and approve
    if ref_count > 0 and vso_vao_count > 0:
        return "MIXED"

    # Rule 5: All reject
    if ref_count > 0 and vso_vao_count == 0:
        return "ALL_REJECT"

    # Rule 6: All approve
    if ref_count == 0 and vso_vao_count > 0:
        return "ALL_APPROVE"

    # Rule 7: Fallback — all replied with non-driving statuses only
    return "INCOMPLETE"


def add_consensus_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply consensus decision tree to each row.
    Adds: consensus_type.
    Requires columns from Step 3: _replied_among_relevant, approvers_ref,
    approvers_vso, approvers_vao, relevant_approvers, _pending_among_relevant.
    """
    if df.empty:
        df["consensus_type"] = pd.Series(dtype=str)
        return df

    fallback_count = 0
    consensus_types = []

    for idx in df.index:
        replied_rel = int(df.at[idx, "_replied_among_relevant"])
        pending_rel = int(df.at[idx, "_pending_among_relevant"])
        relevant_count = int(df.at[idx, "relevant_approvers"])
        ref_count = int(df.at[idx, "approvers_ref"])
        vso_vao_count = int(df.at[idx, "approvers_vso"]) + int(df.at[idx, "approvers_vao"])

        ct = _determine_consensus(
            replied_rel, pending_rel, relevant_count, ref_count, vso_vao_count
        )

        # Log warnings for safety-net cases
        if ct == "ALL_HM":
            logger.warning(
                "Row %s reached ALL_HM in consensus — should have been excluded in Step 1.",
                df.at[idx, "row_id"] if "row_id" in df.columns else idx,
            )
        elif (ct == "INCOMPLETE" and replied_rel > 0
              and pending_rel == 0 and relevant_count > 0
              and ref_count == 0 and vso_vao_count == 0):
            logger.warning(
                "Row %s: all relevant approvers replied with non-driving statuses — "
                "defaulting to INCOMPLETE.",
                df.at[idx, "row_id"] if "row_id" in df.columns else idx,
            )
            fallback_count += 1

        consensus_types.append(ct)

    df["consensus_type"] = consensus_types

    # Validation: all values must be valid
    assert df["consensus_type"].notna().all(), "Some rows have null consensus_type"
    assert df["consensus_type"].isin(VALID_CONSENSUS_TYPES).all(), \
        f"Invalid consensus types found: {set(df['consensus_type']) - VALID_CONSENSUS_TYPES}"

    if fallback_count > 0:
        logger.warning("Step 4: %d rows used non-driving fallback to INCOMPLETE.", fallback_count)
    logger.info("Step 4: Consensus types assigned. Distribution: %s",
                df["consensus_type"].value_counts().to_dict())
    return df
