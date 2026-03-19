"""
Optional Excel output writer for audit format.
"""

import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def write_master_dataset_xlsx(df: pd.DataFrame, output_dir: str) -> str:
    """Write master dataset to Excel for audit purposes."""
    path = os.path.join(output_dir, "master_dataset.xlsx")

    # Convert list columns to semicolon-separated for Excel
    df_copy = df.copy()
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].apply(
            lambda x: ";".join(str(i) for i in x) if isinstance(x, list) else x
        )

    df_copy.to_excel(path, index=False, engine='openpyxl')

    logger.info("Wrote master_dataset.xlsx: %d rows", len(df_copy))
    return path
