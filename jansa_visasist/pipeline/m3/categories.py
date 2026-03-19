"""
Step 5: Category Assignment
[SPEC] V2.2 §3 Step 4 (Categories)
"""

import logging

import pandas as pd

from ...config_m3 import VALID_CATEGORIES

logger = logging.getLogger(__name__)


def _determine_category(consensus_type: str, revision_count: int) -> str:
    """Pure function: apply first-match-wins category rules."""
    # 1. ALL_APPROVE → EASY_WIN_APPROVE
    if consensus_type == "ALL_APPROVE":
        return "EASY_WIN_APPROVE"

    # 2. ALL_REJECT AND revision_count > 1 → BLOCKED
    if consensus_type == "ALL_REJECT" and revision_count > 1:
        return "BLOCKED"

    # 3. ALL_REJECT AND revision_count <= 1 → FAST_REJECT
    if consensus_type == "ALL_REJECT" and revision_count <= 1:
        return "FAST_REJECT"

    # 4. MIXED → CONFLICT
    if consensus_type == "MIXED":
        return "CONFLICT"

    # 5. INCOMPLETE → WAITING
    if consensus_type == "INCOMPLETE":
        return "WAITING"

    # 6. NOT_STARTED → NOT_STARTED
    if consensus_type == "NOT_STARTED":
        return "NOT_STARTED"

    # 7. ALL_HM (safety net — should not happen)
    if consensus_type == "ALL_HM":
        return "NOT_STARTED"

    # Should never reach here
    return "NOT_STARTED"


def add_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply category rules based on consensus_type and revision_count.
    Adds: category.
    """
    if df.empty:
        df["category"] = pd.Series(dtype=str)
        return df

    categories = []
    for idx in df.index:
        ct = df.at[idx, "consensus_type"]
        rc = int(df.at[idx, "revision_count"])

        cat = _determine_category(ct, rc)

        # Log warning for revision_count == 0 edge case
        if ct == "ALL_REJECT" and rc == 0:
            logger.warning(
                "Row %s: revision_count=0 with ALL_REJECT — treated as FAST_REJECT.",
                df.at[idx, "row_id"] if "row_id" in df.columns else idx,
            )

        # Log warning for ALL_HM safety net
        if ct == "ALL_HM":
            logger.warning(
                "Row %s: ALL_HM reached category engine — assigned NOT_STARTED.",
                df.at[idx, "row_id"] if "row_id" in df.columns else idx,
            )

        categories.append(cat)

    df["category"] = categories

    # Validation
    assert df["category"].notna().all(), "Some rows have null category"
    assert df["category"].isin(VALID_CATEGORIES).all(), \
        f"Invalid categories found: {set(df['category']) - VALID_CATEGORIES}"

    # V6/V7: BLOCKED must have revision_count > 1 AND ALL_REJECT
    blocked = df[df["category"] == "BLOCKED"]
    if len(blocked) > 0:
        assert (blocked["consensus_type"] == "ALL_REJECT").all(), \
            "BLOCKED rows must have ALL_REJECT consensus"
        assert (blocked["revision_count"] > 1).all(), \
            "BLOCKED rows must have revision_count > 1"

    fast_reject = df[df["category"] == "FAST_REJECT"]
    if len(fast_reject) > 0:
        assert (fast_reject["consensus_type"] == "ALL_REJECT").all(), \
            "FAST_REJECT rows must have ALL_REJECT consensus"
        assert (fast_reject["revision_count"] <= 1).all(), \
            "FAST_REJECT rows must have revision_count <= 1"

    logger.info("Step 5: Categories assigned. Distribution: %s",
                df["category"].value_counts().to_dict())
    return df
