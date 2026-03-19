"""
JSON output writer for master dataset, import log, header mapping report, and validation report.

The _convert_for_json function must handle every type that can appear
in a DataFrame cell, including containers (list, dict, tuple, ndarray)
that make pd.isna() raise "ambiguous truth value".  The fix is to check
for container types BEFORE ever calling pd.isna().
"""

import json
import math
import os
import logging
from typing import Dict, List

import pandas as pd
import numpy as np

from ..models.log_entry import ImportLogEntry
from ..models.header_report import HeaderMappingReport

logger = logging.getLogger(__name__)


def _convert_for_json(obj):
    """
    Recursively convert a value into a JSON-safe Python primitive.

    Order of checks matters:
    1. None              → None
    2. Containers first  → recurse (MUST come before pd.isna to avoid
       "ambiguous truth value of array" crash)
    3. numpy scalar      → .item() to native Python type
    4. pandas Timestamp  → ISO string
    5. float NaN / inf   → None  (checked via math.isnan after ruling out containers)
    6. pd.isna scalar    → None  (catches pd.NaT, pd.NA, np.nan)
    7. Everything else   → pass through
    """
    # ── 1. Python None ──
    if obj is None:
        return None

    # ── 2. Containers — recurse BEFORE any pd.isna call ──
    if isinstance(obj, dict):
        return {k: _convert_for_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [_convert_for_json(item) for item in obj]

    if isinstance(obj, np.ndarray):
        return [_convert_for_json(item) for item in obj.tolist()]

    # ── 3. numpy scalar types (np.int64, np.float64, np.bool_, etc.) ──
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        native = obj.item()
        # Check for NaN after converting to native float
        if isinstance(native, float) and (math.isnan(native) or math.isinf(native)):
            return None
        return native

    # ── 4. pandas Timestamp ──
    if isinstance(obj, pd.Timestamp):
        if pd.isna(obj):  # pd.NaT
            return None
        return obj.isoformat()

    # ── 5. float NaN / inf ──
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    # ── 6. Other pandas missing sentinels (pd.NA, pd.NaT already caught) ──
    try:
        if pd.isna(obj):
            return None
    except (ValueError, TypeError):
        # pd.isna raises ValueError on arrays, TypeError on some objects.
        # If we reach here something unexpected happened; pass through.
        pass

    # ── 7. Everything else (str, int, bool, …) ──
    return obj


def _log_non_scalar_columns(df: pd.DataFrame) -> None:
    """
    Debug helper: log which columns contain non-scalar (list/dict/array)
    values so we can trace the source of serialization issues.
    """
    non_scalar_cols = []
    for col in df.columns:
        # Sample first non-null value
        sample = df[col].dropna().head(1)
        if len(sample) > 0:
            val = sample.iloc[0]
            if isinstance(val, (list, tuple, dict, np.ndarray)):
                non_scalar_cols.append((col, type(val).__name__))

    if non_scalar_cols:
        col_report = ", ".join(f"{name} ({typ})" for name, typ in non_scalar_cols)
        logger.debug("Columns with non-scalar values: %s", col_report)


def write_master_dataset_json(df: pd.DataFrame, output_dir: str) -> str:
    """Write master dataset to JSON."""
    path = os.path.join(output_dir, "master_dataset.json")

    # Debug: report non-scalar columns before conversion
    _log_non_scalar_columns(df)

    # Convert DataFrame to list of dicts, handling all types safely
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            record[col] = _convert_for_json(row[col])
        records.append(record)

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)

    logger.info("Wrote master_dataset.json: %d records", len(records))
    return path


def write_import_log_json(log_entries: List[ImportLogEntry], output_dir: str) -> str:
    """Write import log to JSON."""
    path = os.path.join(output_dir, "import_log.json")
    records = [entry.to_dict() for entry in log_entries]

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)

    logger.info("Wrote import_log.json: %d entries", len(records))
    return path


def write_header_mapping_report_json(reports: List[HeaderMappingReport], output_dir: str) -> str:
    """Write header mapping reports to JSON."""
    path = os.path.join(output_dir, "header_mapping_report.json")
    records = [report.to_dict() for report in reports]

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)

    logger.info("Wrote header_mapping_report.json: %d sheets", len(records))
    return path


def write_validation_report_json(results: Dict[str, dict], output_dir: str) -> str:
    """Write validation report to JSON."""
    path = os.path.join(output_dir, "validation_report.json")

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info("Wrote validation_report.json")
    return path
