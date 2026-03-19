"""
Step 0+1: Input Validation & Filtering to Pending Scope
[SPEC] V2.2 §3 Steps 0–1
"""

import json
import logging
from typing import Tuple, List

import pandas as pd

from ...config import CANONICAL_APPROVERS
from ...config_m3 import (
    M3_REQUIRED_COLUMNS,
    M3_OPTIONAL_COLUMNS,
    CONSENSUS_HM_STATUSES,
)
from ...context_m3 import Module3Context
from ...models.exclusion_entry import ExclusionEntry

logger = logging.getLogger(__name__)


class Module3InputError(Exception):
    """Raised when M2 output is missing required columns."""
    pass


def _parse_assigned_approvers(val) -> list:
    """Parse assigned_approvers from JSON string, semicolon-separated, or list."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return val
    s = str(val).strip()
    if not s:
        return []
    # Try JSON first
    if s.startswith("["):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except (json.JSONDecodeError, ValueError):
            pass
    # Semicolon-separated (CSV format per GP4)
    if ";" in s:
        return [x.strip() for x in s.split(";") if x.strip()]
    # Comma-separated fallback
    if "," in s:
        return [x.strip() for x in s.split(",") if x.strip()]
    # Single value
    return [s]


def validate_and_prepare(df: pd.DataFrame, ctx: Module3Context) -> pd.DataFrame:
    """
    Step 0: Validate required columns, parse assigned_approvers and dates.
    Returns prepared DataFrame (copy of input).
    """
    if df.empty:
        logger.info("Empty DataFrame - will return empty outputs.")
        return df.copy()

    # 0a. Verify required columns
    missing_required = M3_REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        raise Module3InputError(
            f"Module 2 output is missing required columns: {sorted(missing_required)}. "
            f"Cannot proceed with Module 3."
        )

    # 0b. Add missing optional columns
    missing_optional = M3_OPTIONAL_COLUMNS - set(df.columns)
    for col in missing_optional:
        logger.warning("Optional column '%s' missing from M2 output.", col)
        df[col] = None

    df = df.copy()
    ctx.input_rows = len(df)

    # 0c. Parse assigned_approvers into Python lists
    df["_assigned_list"] = df["assigned_approvers"].apply(_parse_assigned_approvers)
    null_assigned = df["_assigned_list"].apply(len).eq(0).sum()
    if null_assigned > 0:
        logger.warning("%d rows have null or empty assigned_approvers.", null_assigned)

    # 0d. Parse date columns to datetime.date
    for date_col, internal_col in [
        ("date_diffusion", "_date_diffusion_parsed"),
        ("date_contractuelle_visa", "_date_visa_deadline_parsed"),
    ]:
        if date_col in df.columns:
            parsed = pd.to_datetime(df[date_col], errors="coerce")
            df[internal_col] = parsed.dt.date
            # Enforce None instead of NaT
            df[internal_col] = df[internal_col].where(pd.notna(parsed), None)
        else:
            df[internal_col] = None

    # 0e. Verify approver statut columns exist
    for key in CANONICAL_APPROVERS:
        col = f"{key}_statut"
        if col not in df.columns:
            logger.warning("Approver statut column '%s' missing — adding as all-None.", col)
            df[col] = None

    logger.info("Step 0: Input validated. %d rows, %d columns.", len(df), len(df.columns))
    return df


def _is_all_hm(row: pd.Series, assigned_list: list) -> bool:
    """Check if all assigned approvers have statut == HM."""
    if not assigned_list:
        return False
    for key in assigned_list:
        statut_col = f"{key}_statut"
        val = row.get(statut_col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return False
        if str(val) not in CONSENSUS_HM_STATUSES:
            return False
    return True


def filter_to_pending_scope(
    df: pd.DataFrame,
    ctx: Module3Context,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Step 1: Filter M2 dataset to pending VISA items.
    Returns: (pending_df, exclusion_df)
    All excluded rows are logged in ctx.exclusion_log.
    """
    if df.empty:
        return df.copy(), pd.DataFrame()

    n = len(df)
    # Track exclusion reason per row (first match wins)
    exclusion_reason = pd.Series([None] * n, index=df.index, dtype=object)

    # 1b. is_latest != True
    mask_not_latest = df["is_latest"] != True  # noqa: E712
    exclusion_reason = exclusion_reason.where(
        exclusion_reason.notna() | ~mask_not_latest, "NOT_LATEST"
    )

    # 1c. duplicate_flag == "DUPLICATE"
    mask_dup = df["duplicate_flag"] == "DUPLICATE"
    exclusion_reason = exclusion_reason.where(
        exclusion_reason.notna() | ~mask_dup, "DUPLICATE"
    )

    # 1d. visa_global is not null AND != "HM"
    mask_resolved = df["visa_global"].notna() & (df["visa_global"] != "HM")
    exclusion_reason = exclusion_reason.where(
        exclusion_reason.notna() | ~mask_resolved, "VISA_RESOLVED"
    )

    # 1e. visa_global == "HM"
    mask_hm = df["visa_global"] == "HM"
    exclusion_reason = exclusion_reason.where(
        exclusion_reason.notna() | ~mask_hm, "VISA_HM"
    )

    # 1f. All-HM rows (among remaining — visa_global is null, is_latest, not DUPLICATE)
    remaining_mask = exclusion_reason.isna()
    if remaining_mask.any():
        for idx in df.index[remaining_mask]:
            assigned_list = df.at[idx, "_assigned_list"]
            if assigned_list and _is_all_hm(df.loc[idx], assigned_list):
                exclusion_reason.at[idx] = "ALL_APPROVERS_HM"
                logger.info(
                    "Row %s excluded: all assigned approvers are HM.",
                    df.at[idx, "row_id"] if "row_id" in df.columns else idx,
                )

    # Build exclusion log
    excluded_mask = exclusion_reason.notna()
    for idx in df.index[excluded_mask]:
        row = df.loc[idx]
        visa_val = row.get("visa_global")
        if pd.isna(visa_val):
            visa_val = None
        ctx.log_exclusion(ExclusionEntry(
            row_id=str(row.get("row_id", idx)),
            doc_family_key=str(row.get("doc_family_key", "")),
            source_sheet=str(row.get("source_sheet", "")),
            exclusion_reason=exclusion_reason.at[idx],
            visa_global=visa_val,
        ))

    pending_df = df.loc[~excluded_mask].copy()
    exclusion_df = df.loc[excluded_mask].copy()
    exclusion_df["exclusion_reason"] = exclusion_reason[excluded_mask]

    ctx.pending_count = len(pending_df)
    ctx.excluded_count = len(exclusion_df)

    # V1: No rows lost
    assert len(pending_df) + len(exclusion_df) == n, \
        f"Row count mismatch: {len(pending_df)} + {len(exclusion_df)} != {n}"

    logger.info(
        "Step 1: Filtered %d → %d pending (%d excluded).",
        n, len(pending_df), len(exclusion_df),
    )
    return pending_df, exclusion_df
