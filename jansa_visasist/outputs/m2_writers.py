"""
Module 2 output writers: enriched master dataset, document family index,
and linking anomalies log in JSON, CSV, and Excel formats.
"""

import csv
import json
import math
import os
import logging
from typing import List

import pandas as pd
import numpy as np

from ..models.linking_anomaly import LinkingAnomalyEntry

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────
# JSON conversion (optimized: batch via to_dict + targeted fixup)
# ──────────────────────────────────────────────────

def _convert_for_json(obj):
    """Recursively convert a value into a JSON-safe Python primitive."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _convert_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_convert_for_json(item) for item in obj]
    if isinstance(obj, np.ndarray):
        return [_convert_for_json(item) for item in obj.tolist()]
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        native = obj.item()
        if isinstance(native, float) and (math.isnan(native) or math.isinf(native)):
            return None
        return native
    if isinstance(obj, pd.Timestamp):
        return None if pd.isna(obj) else obj.isoformat()
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, bool):
        return obj
    try:
        if pd.isna(obj):
            return None
    except (ValueError, TypeError):
        pass
    return obj


def _df_to_records_fast(df: pd.DataFrame) -> list:
    """Convert DataFrame to list of dicts with JSON-safe values.

    Uses to_dict('records') for bulk conversion, then fixes up
    only the columns that may contain non-JSON-native types.
    """
    # Identify columns needing special treatment
    list_cols = []
    for col in df.columns:
        sample = df[col].dropna().head(1)
        if len(sample) > 0 and isinstance(sample.iloc[0], (list, np.ndarray)):
            list_cols.append(col)

    records = df.to_dict("records")

    for record in records:
        for key, val in record.items():
            if val is None:
                continue
            if isinstance(val, float):
                if math.isnan(val) or math.isinf(val):
                    record[key] = None
                continue
            if isinstance(val, (np.integer, np.floating, np.bool_)):
                native = val.item()
                if isinstance(native, float) and (math.isnan(native) or math.isinf(native)):
                    record[key] = None
                else:
                    record[key] = native
                continue
            if isinstance(val, np.ndarray):
                record[key] = val.tolist()

    return records


def _df_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for CSV: lists -> semicolon-separated."""
    df_copy = df.copy()
    # Only process columns that actually contain lists
    for col in df_copy.columns:
        sample = df_copy[col].dropna().head(1)
        if len(sample) > 0 and isinstance(sample.iloc[0], (list, np.ndarray)):
            df_copy[col] = df_copy[col].apply(
                lambda x: ";".join(str(i) for i in x) if isinstance(x, list) else x
            )
    return df_copy


# ──────────────────────────────────────────────────
# Enriched Master Dataset
# ──────────────────────────────────────────────────

def write_enriched_master_json(df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "enriched_master_dataset.json")
    records = _df_to_records_fast(df)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote enriched_master_dataset.json: %d records", len(records))
    return path


def write_enriched_master_csv(df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "enriched_master_dataset.csv")
    df_copy = _df_for_csv(df)
    df_copy.to_csv(path, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    logger.info("Wrote enriched_master_dataset.csv: %d rows", len(df_copy))
    return path


def write_enriched_master_xlsx(df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "enriched_master_dataset.xlsx")
    df_copy = _df_for_csv(df)
    df_copy.to_excel(path, index=False, engine='openpyxl')
    logger.info("Wrote enriched_master_dataset.xlsx: %d rows", len(df_copy))
    return path


# ──────────────────────────────────────────────────
# Document Family Index
# ──────────────────────────────────────────────────

def build_family_index(df: pd.DataFrame, anomaly_log: List[LinkingAnomalyEntry]) -> pd.DataFrame:
    """
    Build Document Family Index: one row per (doc_family_key, source_sheet).

    Sourcing rules are [IMPLEMENTATION - deterministic by GP1]:
    - lot, first_ind, latest_ind: from row at min/max ind_sort_order,
      tie-break by lowest source_row.
    - latest_visa: if all is_latest rows agree -> that value; else null.
    - revision_count, has_revision_gap, is_cross_lot: from enriched data.
    """
    from ..config_m2 import ANOMALY_REVISION_GAP

    if df.empty:
        return pd.DataFrame()

    # Pre-compute set of (family, sheet) with revision gaps
    gap_families = set()
    for entry in anomaly_log:
        if entry.anomaly_type == ANOMALY_REVISION_GAP:
            gap_families.add((entry.doc_family_key, entry.source_sheet))

    # df is already sorted by (doc_family_key, source_sheet, ind_sort_order, source_row)
    # from chain_linking. Exploit this for efficient first/last extraction.

    grp = df.groupby(["doc_family_key", "source_sheet"], sort=False)

    # First row per group = row with lowest ind_sort_order + lowest source_row
    first_rows = grp.first()
    # Last row per group = row with highest ind_sort_order + highest source_row
    last_rows = grp.last()

    # But we need tie-break by lowest source_row for the last sort_order.
    # Since df is sorted by (sort_order ASC, source_row ASC), the last row
    # in the group has the highest sort_order AND highest source_row.
    # We need the lowest source_row at the highest sort_order.
    # Approach: for each group, get the max sort_order row with min source_row.
    max_order_per_group = grp["ind_sort_order"].transform("max")
    at_max = df[df["ind_sort_order"] == max_order_per_group]
    # First occurrence (lowest source_row due to sort) per group at max order
    latest_rows = at_max.groupby(["doc_family_key", "source_sheet"], sort=False).first()

    # Similarly for min sort_order:
    min_order_per_group = grp["ind_sort_order"].transform("min")
    at_min = df[df["ind_sort_order"] == min_order_per_group]
    earliest_rows = at_min.groupby(["doc_family_key", "source_sheet"], sort=False).first()

    # latest_visa: from is_latest=true rows
    latest_mask = df["is_latest"] == True  # noqa: E712
    latest_df = df[latest_mask]
    latest_visa_grp = latest_df.groupby(["doc_family_key", "source_sheet"], sort=False)

    def _resolve_visa(visa_series):
        """Same value (including all null) -> that value; different -> null."""
        vals = set()
        for v in visa_series:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                vals.add(None)
            else:
                vals.add(v)
        if len(vals) == 1:
            return vals.pop()
        return None

    latest_visa_map = latest_visa_grp["visa_global"].agg(_resolve_visa).to_dict()

    # Build result
    index_data = []
    for (fam_key, sheet) in grp.groups.keys():
        lot_val = latest_rows.loc[(fam_key, sheet), "lot"] if (fam_key, sheet) in latest_rows.index else None
        first_ind = earliest_rows.loc[(fam_key, sheet), "ind"] if (fam_key, sheet) in earliest_rows.index else None
        latest_ind = latest_rows.loc[(fam_key, sheet), "ind"] if (fam_key, sheet) in latest_rows.index else None
        rev_count = first_rows.loc[(fam_key, sheet), "revision_count"] if (fam_key, sheet) in first_rows.index else 0
        is_cross = bool(first_rows.loc[(fam_key, sheet), "is_cross_lot"]) if (fam_key, sheet) in first_rows.index else False

        latest_visa = latest_visa_map.get((fam_key, sheet))

        has_gap = (str(fam_key), str(sheet)) in gap_families

        # GP2 null handling
        if pd.isna(lot_val):
            lot_val = None
        if pd.isna(first_ind):
            first_ind = None
        if pd.isna(latest_ind):
            latest_ind = None

        index_data.append({
            "doc_family_key": fam_key,
            "source_sheet": sheet,
            "lot": lot_val,
            "first_ind": first_ind,
            "latest_ind": latest_ind,
            "revision_count": int(rev_count),
            "latest_visa": latest_visa,
            "has_revision_gap": has_gap,
            "is_cross_lot": is_cross,
        })

    return pd.DataFrame(index_data)


def write_family_index_json(family_df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "document_family_index.json")
    records = _df_to_records_fast(family_df) if not family_df.empty else []
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote document_family_index.json: %d families", len(records))
    return path


def write_family_index_csv(family_df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "document_family_index.csv")
    family_df.to_csv(path, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    logger.info("Wrote document_family_index.csv: %d families", len(family_df))
    return path


# ──────────────────────────────────────────────────
# Linking Anomalies Log
# ──────────────────────────────────────────────────

def write_anomalies_json(anomaly_log: List[LinkingAnomalyEntry], output_dir: str) -> str:
    path = os.path.join(output_dir, "linking_anomalies.json")
    records = [_convert_for_json(entry.to_dict()) for entry in anomaly_log]
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote linking_anomalies.json: %d anomalies", len(records))
    return path


def write_anomalies_csv(anomaly_log: List[LinkingAnomalyEntry], output_dir: str) -> str:
    path = os.path.join(output_dir, "linking_anomalies.csv")
    if not anomaly_log:
        with open(path, 'w', encoding='utf-8') as f:
            f.write("")
        return path

    fieldnames = [
        "anomaly_id", "anomaly_type", "doc_family_key",
        "source_sheet", "row_id", "ind", "severity", "details",
    ]
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for entry in anomaly_log:
            row = entry.to_dict()
            row["details"] = json.dumps(row["details"], ensure_ascii=False)
            writer.writerow(row)
    logger.info("Wrote linking_anomalies.csv: %d anomalies", len(anomaly_log))
    return path
