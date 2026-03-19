"""
CSV output writer for master dataset and import log.
"""

import csv
import os
import logging
from typing import List

import pandas as pd

from ..models.log_entry import ImportLogEntry

logger = logging.getLogger(__name__)


def write_master_dataset_csv(df: pd.DataFrame, output_dir: str) -> str:
    """
    Write master dataset to CSV.
    Nulls as empty cells. Lists as semicolon-separated strings.
    """
    path = os.path.join(output_dir, "master_dataset.csv")

    # Convert list columns to semicolon-separated
    df_copy = df.copy()
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].apply(
            lambda x: ";".join(str(i) for i in x) if isinstance(x, list) else x
        )

    df_copy.to_csv(path, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)

    logger.info("Wrote master_dataset.csv: %d rows", len(df_copy))
    return path


def write_import_log_csv(log_entries: List[ImportLogEntry], output_dir: str) -> str:
    """Write import log to CSV."""
    path = os.path.join(output_dir, "import_log.csv")

    if not log_entries:
        with open(path, 'w', encoding='utf-8') as f:
            f.write("")
        return path

    fieldnames = [
        "log_id", "sheet", "row", "column", "severity",
        "category", "raw_value", "action_taken", "confidence"
    ]

    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for entry in log_entries:
            writer.writerow(entry.to_dict())

    logger.info("Wrote import_log.csv: %d entries", len(log_entries))
    return path
