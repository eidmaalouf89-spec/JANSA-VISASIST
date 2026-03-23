"""NM2-GED: SAS State Interpreter.

Determines SAS conformity gate state for each document.
Requires NM3 to have run first (uses mission_type, reponse_normalized, response_status).
No inline vocabulary mapping — NM2 trusts NM3 output exclusively.
"""

import pandas as pd
import numpy as np

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.logging import log_event


def _validate_contract(df: pd.DataFrame) -> None:
    """Validate NM2 input contract: required columns from NM3."""
    required = ['doc_id', 'mission_type', 'reponse_normalized', 'response_status']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ContractError(f"NM2 input contract violated — missing columns: {missing}")


def _classify_sas_state(response_status: str, reponse_normalized: object) -> tuple:
    """Classify SAS state from NM3 response_status. No inline vocab mapping.

    Returns: (sas_state, sas_verdict)
    """
    if response_status == 'RESPONDED_REJECT':
        return 'SAS_BLOCKED', reponse_normalized
    elif response_status == 'RESPONDED_APPROVE':
        return 'SAS_PASSED', reponse_normalized
    elif response_status in ('NOT_RESPONDED', 'PENDING_CIRCUIT'):
        return 'SAS_PENDING', None
    else:
        return 'SAS_UNKNOWN', None


def interpret_sas(ged_long: pd.DataFrame) -> pd.DataFrame:
    """Interpret SAS state for each document.

    Input: ged_long enriched by NM3 (has mission_type, reponse_normalized, response_status)
    Output: DataFrame with one row per doc_id containing SAS state fields
    """
    _validate_contract(ged_long)

    # Step 1: Extract SAS rows
    sas_rows = ged_long[ged_long['mission_type'] == 'SAS'].copy()

    if len(sas_rows) == 0:
        log_event(
            None, 'NM2', 'INFO', 'NO_SAS_ROWS',
            'No SAS rows found in dataset — all docs will be SAS_UNKNOWN + SAS_ASSUMED_PASSED',
        )

    # Step 2: Handle multiple SAS rows per doc — keep most recent by date_reponse
    if len(sas_rows) > 0 and 'date_reponse' in sas_rows.columns:
        dup_docs = sas_rows.groupby('doc_id').size()
        multi_sas_docs = dup_docs[dup_docs > 1].index
        if len(multi_sas_docs) > 0:
            for did in multi_sas_docs:
                log_event(
                    int(did), 'NM2', 'WARNING', 'MULTIPLE_SAS_ROWS',
                    f'Multiple SAS rows for doc_id {did} — keeping most recent',
                )
            # Sort by date_reponse descending, keep first per doc_id
            sas_rows = sas_rows.sort_values('date_reponse', ascending=False, na_position='last')
            sas_rows = sas_rows.drop_duplicates(subset=['doc_id'], keep='first')

    # Step 3: Classify SAS state for each SAS row (vectorized)
    sas_result_rows = []
    if len(sas_rows) > 0:
        # Vectorized classification via apply on the two relevant columns
        classifications = sas_rows.apply(
            lambda row: _classify_sas_state(row['response_status'], row['reponse_normalized']),
            axis=1,
        )
        sas_rows = sas_rows.copy()
        sas_rows['sas_state'] = classifications.apply(lambda x: x[0])
        sas_rows['sas_verdict'] = classifications.apply(lambda x: x[1])

        # Log unexpected statuses on SAS rows
        unexpected = sas_rows[sas_rows['sas_state'] == 'SAS_UNKNOWN']
        if len(unexpected) > 0:
            uniq_statuses = unexpected[['doc_id', 'response_status']].drop_duplicates()
            doc_ids = uniq_statuses['doc_id'].values
            statuses = uniq_statuses['response_status'].values
            for did, status in zip(doc_ids[:20], statuses[:20]):
                log_event(
                    int(did), 'NM2', 'WARNING', 'UNEXPECTED_SAS_STATUS',
                    f"Unexpected response_status '{status}' on SAS mission",
                )

        # Build result for docs WITH SAS rows
        sas_result_rows = sas_rows[['doc_id']].copy()
        sas_result_rows['sas_state'] = sas_rows['sas_state'].values
        sas_result_rows['sas_verdict'] = sas_rows['sas_verdict'].values
        sas_result_rows['sas_repondant'] = sas_rows['repondant'].values if 'repondant' in sas_rows.columns else None
        sas_result_rows['sas_date'] = sas_rows['date_reponse'].values if 'date_reponse' in sas_rows.columns else None
        sas_result_rows['sas_confidence'] = 'HIGH'
        sas_result_rows['inference_flags'] = [[] for _ in range(len(sas_result_rows))]

    # Step 4: Handle docs with no SAS row → SAS_UNKNOWN + SAS_ASSUMED_PASSED
    all_doc_ids = ged_long['doc_id'].unique()
    sas_doc_ids = set(sas_rows['doc_id'].unique()) if len(sas_rows) > 0 else set()
    no_sas_docs = [did for did in all_doc_ids if did not in sas_doc_ids]

    no_sas_result = pd.DataFrame({
        'doc_id': no_sas_docs,
        'sas_state': 'SAS_UNKNOWN',
        'sas_verdict': None,
        'sas_repondant': None,
        'sas_date': pd.NaT,
        'sas_confidence': 'LOW',
        'inference_flags': [['SAS_ASSUMED_PASSED'] for _ in range(len(no_sas_docs))],
    })

    # Combine
    if len(sas_result_rows) > 0:
        result = pd.concat([sas_result_rows, no_sas_result], ignore_index=True)
    else:
        result = no_sas_result

    log_event(
        None, 'NM2', 'INFO', 'NM2_COMPLETE',
        f'NM2 complete: {len(result)} docs, '
        f'sas_state dist: {result["sas_state"].value_counts().to_dict()}',
    )

    return result
