"""NM3-GED: Response Normalization & Classification.

Transforms raw GED response strings into canonical JANSA vocabulary.
Also classifies mission_type (moved from NM1 in V1.1).

NM3 is the single source of truth for all vocabulary normalization.
NM2 and NM4 both depend on NM3 output.
"""

import pandas as pd

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.logging import log_event
from jansa.adapters.ged.constants import (
    MISSION_TYPE_PATTERNS,
    VOCAB_MAP,
)


# ---------------------------------------------------------------------------
# Mission type classification
# ---------------------------------------------------------------------------

def classify_mission(mission: object) -> str:
    """Classify mission string into role enum.

    SAS / MOEX / REVIEWER / UNKNOWN.
    Workflow-semantic operation — belongs in NM3, not NM1.
    """
    if pd.isna(mission) or str(mission).strip() == '':
        return 'UNKNOWN'
    m = str(mission).strip()
    for pattern_type, patterns in MISSION_TYPE_PATTERNS.items():
        for pattern in patterns:
            if pattern in m:
                return pattern_type
    return 'REVIEWER'


# ---------------------------------------------------------------------------
# Response normalization
# ---------------------------------------------------------------------------

def normalize_response(raw: object) -> tuple:
    """Normalize a single response value using VOCAB_MAP prefix matching.

    Returns: (reponse_normalized, response_status)
    """
    if pd.isna(raw) or str(raw).strip() == '':
        return None, 'NOT_RESPONDED'
    raw_str = str(raw).strip()
    for prefix, normalized, status in VOCAB_MAP:
        if raw_str.startswith(prefix):
            return normalized, status
    return None, 'RESPONDED_AMBIGUOUS'


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _validate_contract(df: pd.DataFrame) -> None:
    """Validate NM3 input contract: required columns from NM1."""
    required = ['doc_id', 'mission', 'reponse_raw']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ContractError(f"NM3 input contract violated — missing columns: {missing}")


def normalize_responses(ged_long: pd.DataFrame) -> pd.DataFrame:
    """Apply vocabulary normalization and mission_type classification.

    Input: NM1 output (ged_long with reponse_raw, mission)
    Output: ged_long enriched with mission_type, reponse_normalized, response_status
    """
    _validate_contract(ged_long)

    df = ged_long.copy()

    # Step 0: Classify mission_type (vectorized via .map)
    df['mission_type'] = df['mission'].map(classify_mission)

    # Step 1: Apply vocab mapping (vectorized via .apply on Series)
    result = df['reponse_raw'].apply(normalize_response)
    df['reponse_normalized'] = result.apply(lambda x: x[0])
    df['response_status'] = result.apply(lambda x: x[1])

    # Log unknown vocabulary
    ambiguous_mask = df['response_status'] == 'RESPONDED_AMBIGUOUS'
    if ambiguous_mask.any():
        unknown_values = df.loc[ambiguous_mask, 'reponse_raw'].unique()
        for val in unknown_values:
            affected_docs = df.loc[
                (df['reponse_raw'] == val) & ambiguous_mask, 'doc_id'
            ].unique()
            for did in affected_docs:
                log_event(
                    int(did), 'NM3', 'WARNING', 'UNKNOWN_RESPONSE_VOCABULARY',
                    f'Unknown response vocabulary: {val}',
                    raw_value=str(val),
                    field='reponse_raw',
                )

    # Convert mission_type to category for memory efficiency (GP-SCALE)
    df['mission_type'] = df['mission_type'].astype('category')

    log_event(
        None, 'NM3', 'INFO', 'NM3_COMPLETE',
        f'NM3 complete: {len(df)} rows, '
        f'mission_type dist: {df["mission_type"].value_counts().to_dict()}, '
        f'response_status dist: {df["response_status"].value_counts().to_dict()}',
    )

    return df
