"""
Module 7 — Dataset Signature Computation.

Deterministic SHA-256 signature for dataset identity verification.
"""

import hashlib
import json
from typing import List


def compute_dataset_signature(
    source_file: str,
    total_rows: int,
    total_sheets: int,
    sheet_names: List[str],
    row_ids: List[str],
) -> str:
    """
    Compute a deterministic dataset signature (SHA-256 hex).

    The signature is computed from a canonical JSON manifest
    with sorted sheet_names and row_ids for stability.

    Args:
        source_file: Name of the source Excel file.
        total_rows: Total number of rows in the dataset.
        total_sheets: Total number of sheets.
        sheet_names: List of sheet names (will be sorted).
        row_ids: List of row IDs (will be sorted).

    Returns:
        Lowercase hex SHA-256 string (64 chars).
    """
    manifest = {
        "source_file": source_file,
        "total_rows": total_rows,
        "total_sheets": total_sheets,
        "sheet_names": sorted(sheet_names),
        "row_ids": sorted(row_ids),
    }
    canonical_json = json.dumps(manifest, separators=(",", ":"), ensure_ascii=False)
    sha256_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return sha256_hash
