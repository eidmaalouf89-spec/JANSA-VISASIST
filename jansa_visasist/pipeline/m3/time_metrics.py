"""
Step 2: Time Metric Computation
[SPEC] V2.2 §3 Step 2
"""

import datetime
import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def add_time_metrics(
    df: pd.DataFrame,
    reference_date: datetime.date,
) -> pd.DataFrame:
    """
    Add time metric columns to pending DataFrame.
    Adds: days_since_diffusion, days_until_deadline, is_overdue,
          days_overdue, has_deadline.
    """
    if df.empty:
        for col in ["days_since_diffusion", "days_until_deadline",
                     "is_overdue", "days_overdue", "has_deadline"]:
            df[col] = None if col not in ("is_overdue", "has_deadline") else False
        return df

    n = len(df)

    # has_deadline
    df["has_deadline"] = df["_date_visa_deadline_parsed"].notna()

    # days_since_diffusion
    days_diff = pd.array([None] * n, dtype=object)
    for i, idx in enumerate(df.index):
        d = df.at[idx, "_date_diffusion_parsed"]
        if d is not None and not pd.isna(d):
            # Convert pd.Timestamp to datetime.date if needed
            if isinstance(d, pd.Timestamp):
                d = d.date()
            days_diff[i] = (reference_date - d).days
    df["days_since_diffusion"] = days_diff

    # days_until_deadline
    days_until = pd.array([None] * n, dtype=object)
    for i, idx in enumerate(df.index):
        d = df.at[idx, "_date_visa_deadline_parsed"]
        if d is not None and not pd.isna(d):
            # Convert pd.Timestamp to datetime.date if needed
            if isinstance(d, pd.Timestamp):
                d = d.date()
            days_until[i] = (d - reference_date).days
    df["days_until_deadline"] = days_until

    # is_overdue: True only if has_deadline AND days_until_deadline < 0
    df["is_overdue"] = df.apply(
        lambda row: bool(row["has_deadline"] and row["days_until_deadline"] is not None
                         and row["days_until_deadline"] < 0),
        axis=1,
    )

    # days_overdue: abs(days_until_deadline) if overdue, else 0
    df["days_overdue"] = df.apply(
        lambda row: abs(row["days_until_deadline"]) if row["is_overdue"] else 0,
        axis=1,
    ).astype(int)

    overdue_count = df["is_overdue"].sum()
    no_deadline_count = (~df["has_deadline"]).sum()
    logger.info(
        "Step 2: Time metrics computed. %d overdue, %d without deadline.",
        overdue_count, no_deadline_count,
    )
    return df
