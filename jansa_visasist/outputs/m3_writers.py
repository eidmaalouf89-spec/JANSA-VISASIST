"""
Module 3 output writers: priority queue, category summaries,
exclusion log, and pipeline report in JSON, CSV, and Excel formats.
"""

import csv
import json
import math
import os
import logging
from typing import List

import pandas as pd
import numpy as np

from ..models.exclusion_entry import ExclusionEntry

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────
# JSON conversion (reuse M2 pattern)
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


def _df_to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to list of dicts with JSON-safe values."""
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


def _list_to_semicolon(val):
    """Convert list to semicolon-separated string for CSV (GP4)."""
    if isinstance(val, list):
        return ";".join(str(x) for x in val)
    return val


def _df_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for CSV: lists -> semicolon-separated strings (GP4)."""
    df_copy = df.copy()
    for col in df_copy.columns:
        sample = df_copy[col].dropna().head(1)
        if len(sample) > 0 and isinstance(sample.iloc[0], (list, np.ndarray)):
            df_copy[col] = df_copy[col].apply(_list_to_semicolon)
    return df_copy


# ──────────────────────────────────────────────────
# Priority Queue
# ──────────────────────────────────────────────────

def write_priority_queue_json(df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_priority_queue.json")
    records = _df_to_records(df) if not df.empty else []
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote m3_priority_queue.json: %d records", len(records))
    return path


def write_priority_queue_csv(df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_priority_queue.csv")
    df_copy = _df_for_csv(df)
    df_copy.to_csv(path, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    logger.info("Wrote m3_priority_queue.csv: %d rows", len(df_copy))
    return path


def write_priority_queue_xlsx(df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_priority_queue.xlsx")
    df_copy = _df_for_csv(df)
    df_copy.to_excel(path, index=False, engine='openpyxl')
    logger.info("Wrote m3_priority_queue.xlsx: %d rows", len(df_copy))
    return path


# ──────────────────────────────────────────────────
# Category Summaries
# ──────────────────────────────────────────────────

def write_category_summary_json(summary_df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_category_summary.json")
    records = _df_to_records(summary_df) if not summary_df.empty else []
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote m3_category_summary.json: %d entries", len(records))
    return path


def write_category_summary_csv(summary_df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_category_summary.csv")
    summary_df.to_csv(path, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
    logger.info("Wrote m3_category_summary.csv: %d rows", len(summary_df))
    return path


def write_extended_summary_json(ext_df: pd.DataFrame, output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_category_summary_extended.json")
    records = _df_to_records(ext_df) if not ext_df.empty else []
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote m3_category_summary_extended.json: %d entries", len(records))
    return path


# ──────────────────────────────────────────────────
# Exclusion Log (diagnostic output)
# ──────────────────────────────────────────────────

def write_exclusion_log_json(log: List[ExclusionEntry], output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_exclusion_log.json")
    records = [_convert_for_json(entry.to_dict()) for entry in log]
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote m3_exclusion_log.json: %d entries", len(records))
    return path


def write_exclusion_log_csv(log: List[ExclusionEntry], output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_exclusion_log.csv")
    if not log:
        with open(path, 'w', encoding='utf-8') as f:
            f.write("")
        return path

    fieldnames = ["row_id", "doc_family_key", "source_sheet",
                  "exclusion_reason", "visa_global"]
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for entry in log:
            writer.writerow(entry.to_dict())
    logger.info("Wrote m3_exclusion_log.csv: %d entries", len(log))
    return path


# ──────────────────────────────────────────────────
# Pipeline Report (diagnostic output)
# ──────────────────────────────────────────────────

def write_pipeline_report(report: dict, output_dir: str) -> str:
    path = os.path.join(output_dir, "m3_pipeline_report.json")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote m3_pipeline_report.json")
    return path
