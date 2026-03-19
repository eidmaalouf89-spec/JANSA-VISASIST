"""
Step 7: Category Summaries & Finalization
[SPEC] V2.2 §3 Output 2 — Category Summaries
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def sort_and_finalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort queue by priority_score DESC, then tie-breakers.
    Drop internal working columns.
    """
    if df.empty:
        return df

    # 7a. Sort by priority_score DESC, days_overdue DESC,
    # days_since_diffusion DESC (nulls last), doc_family_key ASC
    df = df.sort_values(
        by=["priority_score", "days_overdue", "days_since_diffusion", "doc_family_key"],
        ascending=[False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)

    # 7e. Drop internal working columns
    internal_cols = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal_cols, errors="ignore")

    logger.info("Step 7: Queue sorted and finalized. %d rows.", len(df))
    return df


def build_category_summaries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build spec-mandated frequency tables (by category, by lot, by source_sheet)
    as a single DataFrame with columns: group_type, group_value, count.
    """
    if df.empty:
        return pd.DataFrame(columns=["group_type", "group_value", "count"])

    rows = []

    # By category
    cat_counts = df["category"].value_counts()
    for cat, cnt in cat_counts.items():
        rows.append({"group_type": "category", "group_value": cat, "count": int(cnt)})

    # By lot
    lot_counts = df["lot"].value_counts(dropna=False)
    for lot, cnt in lot_counts.items():
        lot_str = str(lot) if lot is not None and not (isinstance(lot, float) and pd.isna(lot)) else "UNKNOWN"
        rows.append({"group_type": "lot", "group_value": lot_str, "count": int(cnt)})

    # By source_sheet
    sheet_counts = df["source_sheet"].value_counts()
    for sheet, cnt in sheet_counts.items():
        rows.append({"group_type": "sheet", "group_value": str(sheet), "count": int(cnt)})

    summary_df = pd.DataFrame(rows)

    # V8: Category totals sum to queue size
    cat_total = summary_df.loc[summary_df["group_type"] == "category", "count"].sum()
    assert cat_total == len(df), \
        f"Category totals ({cat_total}) != queue size ({len(df)})"

    logger.info("Step 7: Category summaries built. %d groups.", len(summary_df))
    return summary_df


def build_extended_summaries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build optional extended summaries with overdue_count, avg_priority_score,
    and cross-tabulations. Clearly labeled as non-spec extras.
    """
    if df.empty:
        return pd.DataFrame(columns=["group_type", "group_value", "count",
                                      "overdue_count", "avg_priority_score"])

    rows = []

    # Extended: by category with overdue + avg_score
    for cat, grp in df.groupby("category"):
        rows.append({
            "group_type": "category",
            "group_value": str(cat),
            "count": len(grp),
            "overdue_count": int(grp["is_overdue"].sum()),
            "avg_priority_score": round(grp["priority_score"].mean(), 2),
        })

    # Extended: by lot
    for lot, grp in df.groupby("lot", dropna=False):
        lot_str = str(lot) if lot is not None and not (isinstance(lot, float) and pd.isna(lot)) else "UNKNOWN"
        rows.append({
            "group_type": "lot",
            "group_value": lot_str,
            "count": len(grp),
            "overdue_count": int(grp["is_overdue"].sum()),
            "avg_priority_score": round(grp["priority_score"].mean(), 2),
        })

    # Extended: by sheet
    for sheet, grp in df.groupby("source_sheet"):
        rows.append({
            "group_type": "sheet",
            "group_value": str(sheet),
            "count": len(grp),
            "overdue_count": int(grp["is_overdue"].sum()),
            "avg_priority_score": round(grp["priority_score"].mean(), 2),
        })

    # Cross-tab: category x lot
    for (cat, lot), grp in df.groupby(["category", "lot"], dropna=False):
        lot_str = str(lot) if lot is not None and not (isinstance(lot, float) and pd.isna(lot)) else "UNKNOWN"
        rows.append({
            "group_type": "category_x_lot",
            "group_value": f"{cat}|{lot_str}",
            "count": len(grp),
            "overdue_count": int(grp["is_overdue"].sum()),
            "avg_priority_score": round(grp["priority_score"].mean(), 2),
        })

    return pd.DataFrame(rows)
