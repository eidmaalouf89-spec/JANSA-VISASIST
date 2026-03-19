"""
Step 1: doc_family_key Construction
[SPEC] V2.2 §2.4 Step 1
"""

import hashlib
import logging

import pandas as pd

from ...config_m2 import UNPARSEABLE_PREFIX, HASH_TRUNCATE_LENGTH, ANOMALY_UNPARSEABLE
from ...context_m2 import Module2Context
from ...models.linking_anomaly import LinkingAnomalyEntry

logger = logging.getLogger(__name__)


def build_family_keys(df: pd.DataFrame, ctx: Module2Context) -> pd.DataFrame:
    """
    Add doc_family_key column to every row.
    Primary path: document.replace("_", "") — already uppercase from M1.
    Fallback path: UNPARSEABLE::{hash} for null document rows.
    """
    # Primary path: vectorized for all non-null documents
    mask_valid = df["document"].notna()
    df.loc[mask_valid, "doc_family_key"] = (
        df.loc[mask_valid, "document"].str.replace("_", "", regex=False)
    )

    # Fallback path: loop over null-document rows (typically very few)
    null_mask = df["document"].isna()
    null_count = null_mask.sum()

    for idx in df.index[null_mask]:
        row = df.loc[idx]
        raw_str = "" if pd.isna(row.get("document_raw")) else str(row["document_raw"])
        hash_input = raw_str + "::" + str(row["source_sheet"]) + "::" + str(row["source_row"])
        hash_hex = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()[:HASH_TRUNCATE_LENGTH]
        df.at[idx, "doc_family_key"] = UNPARSEABLE_PREFIX + hash_hex

        # Log UNPARSEABLE_DOCUMENT anomaly
        ctx.log_anomaly(LinkingAnomalyEntry(
            anomaly_id=ctx.next_anomaly_id(),
            anomaly_type=ANOMALY_UNPARSEABLE,
            doc_family_key=df.at[idx, "doc_family_key"],
            source_sheet=str(row["source_sheet"]),
            row_id=str(row["row_id"]),
            ind=row.get("ind") if pd.notna(row.get("ind")) else None,
            severity="WARNING",
            details={
                "source_row": int(row["source_row"]),
                "document_raw": raw_str if raw_str else None,
            },
        ))

    ctx.unparseable_count = null_count
    logger.info("Step 1: Built family keys. %d valid, %d UNPARSEABLE", mask_valid.sum(), null_count)
    return df
