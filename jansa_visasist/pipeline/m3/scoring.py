"""
Step 6: Priority Score Computation
[SPEC] V2.2 §3 Step 5 (Scoring)
"""

import logging

import pandas as pd
import numpy as np

from ...config_m3 import (
    OVERDUE_MAX_POINTS,
    OVERDUE_CAP_DAYS,
    PROXIMITY_3D_POINTS,
    PROXIMITY_7D_POINTS,
    PROXIMITY_14D_POINTS,
    COMPLETENESS_ALL_APPROVE,
    COMPLETENESS_ALL_REJECT,
    COMPLETENESS_MIXED,
    REVISION_DEPTH_HIGH,
    REVISION_DEPTH_MED,
    MISSING_DEADLINE_PENALTY,
    SCORE_MIN,
    SCORE_MAX,
)

logger = logging.getLogger(__name__)

# Completeness lookup
_COMPLETENESS_MAP = {
    "ALL_APPROVE": COMPLETENESS_ALL_APPROVE,
    "ALL_REJECT": COMPLETENESS_ALL_REJECT,
    "MIXED": COMPLETENESS_MIXED,
}


def _compute_score(
    is_overdue: bool,
    days_overdue: int,
    has_deadline: bool,
    days_until_deadline,
    consensus_type: str,
    revision_count: int,
) -> float:
    """Pure function: compute 0-100 priority score from components."""
    # Component 1: Overdue (0–40)
    if is_overdue:
        capped = min(days_overdue, OVERDUE_CAP_DAYS)
        overdue_score = (capped / OVERDUE_CAP_DAYS) * OVERDUE_MAX_POINTS
    else:
        overdue_score = 0.0

    # Component 2: Proximity (0–25) — mutually exclusive with overdue
    proximity_score = 0.0
    if has_deadline and not is_overdue and days_until_deadline is not None:
        d = int(days_until_deadline)
        if d <= 3:
            proximity_score = PROXIMITY_3D_POINTS
        elif d <= 7:
            proximity_score = PROXIMITY_7D_POINTS
        elif d <= 14:
            proximity_score = PROXIMITY_14D_POINTS

    # Component 3: Completeness (0–20)
    completeness_score = _COMPLETENESS_MAP.get(consensus_type, 0)

    # Component 4: Revision depth (0–5)
    if revision_count > 2:
        revision_score = REVISION_DEPTH_HIGH
    elif revision_count == 2:
        revision_score = REVISION_DEPTH_MED
    else:
        revision_score = 0

    # Penalty: Missing deadline (-10)
    deadline_penalty = MISSING_DEADLINE_PENALTY if not has_deadline else 0

    # Final score clamped to [0, 100]
    raw = overdue_score + proximity_score + completeness_score + revision_score + deadline_penalty
    return max(SCORE_MIN, min(SCORE_MAX, raw))


def add_priority_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute priority_score for each row.
    Adds: priority_score.
    """
    if df.empty:
        df["priority_score"] = pd.Series(dtype=float)
        return df

    scores = []
    for idx in df.index:
        score = _compute_score(
            is_overdue=bool(df.at[idx, "is_overdue"]),
            days_overdue=int(df.at[idx, "days_overdue"]),
            has_deadline=bool(df.at[idx, "has_deadline"]),
            days_until_deadline=df.at[idx, "days_until_deadline"],
            consensus_type=str(df.at[idx, "consensus_type"]),
            revision_count=int(df.at[idx, "revision_count"]),
        )
        scores.append(round(score, 2))

    df["priority_score"] = scores

    # Validation: all scores in range
    assert df["priority_score"].between(SCORE_MIN, SCORE_MAX).all(), \
        "Some scores are out of range [0, 100]"

    logger.info(
        "Step 6: Scores computed. min=%.1f, max=%.1f, mean=%.1f, median=%.1f",
        df["priority_score"].min(),
        df["priority_score"].max(),
        df["priority_score"].mean(),
        df["priority_score"].median(),
    )
    return df
