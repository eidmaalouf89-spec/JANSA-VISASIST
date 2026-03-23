"""NM4-GED: Assignment Classification & Circuit Mapping.

Determines assignment_type for each (document × reviewer) pair.
In the GED, assignment is encoded directly in mission row presence.

Requires NM3 to have run first (uses mission_type, response_status).
Uses circuit_matrix.py for real circuit de diffusion lookup.
"""

import pandas as pd

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.logging import log_event
from jansa.adapters.ged.circuit_matrix import lookup_assignment


# ---------------------------------------------------------------------------
# Constants — keyword activation map (R-NM4-05: deterministic, named constant)
# ---------------------------------------------------------------------------

KEYWORD_ACTIVATION = {
    'acoustique':           'AVLS',
    'acousticien':          'AVLS',
    'AVLS':                 'AVLS',
    'socotec':              'SOCOTEC',
    'bureau de contrôle':   'SOCOTEC',
    'BC :':                 'SOCOTEC',
    'HQE':                  'LE_SOMMER',
    'environnement':        'LE_SOMMER',
    'le sommer':            'LE_SOMMER',
    'pollution':            'BET_POLLUTION',
    'DIE':                  'BET_POLLUTION',
    'géotechnique':         'GEOLIA',
    'GEOLIA':               'GEOLIA',
}

# Assignment type enum values
ASSIGNMENT_TYPE_VALUES = {
    'REQUIRED_VISA', 'INFORMATIONAL', 'CONDITIONAL',
    'NOT_ASSIGNED', 'UNKNOWN_REQUIRED',
}

ASSIGNMENT_SOURCE_VALUES = {
    'GED_PRESENCE', 'MATRIX', 'DATA_OVERRIDE', 'DISCIPLINE_FALLBACK',
    'KEYWORD_TRIGGER',
}


# ---------------------------------------------------------------------------
# Contract validation (P10)
# ---------------------------------------------------------------------------

def _validate_contract(df: pd.DataFrame) -> None:
    """Validate NM4 input contract: required columns from NM1+NM3."""
    required = [
        'doc_id', 'mission', 'mission_type', 'response_status', 'commentaire',
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ContractError(
            f"NM4 input contract violated — missing columns: {missing}"
        )


# ---------------------------------------------------------------------------
# Step 2 — Classify assignment type (vectorized)
# ---------------------------------------------------------------------------

def _classify_assignment(mission_type: str, lot: str, type_doc: str,
                         mission: str) -> tuple:
    """Classify a single row's assignment type.

    Uses circuit_matrix.lookup_assignment for real matrix matching:
      exact (lot, type_doc) → lot wildcard '*' → parent lot → GLOBAL → fallback.

    Returns: (assignment_type, assignment_source, inference_flags)
    """
    # R-NM4-07: MOEX, SAS, SUBCONTRACTOR rows excluded from reviewer assignment
    if mission_type in ('MOEX', 'SAS', 'SUBCONTRACTOR'):
        return 'NOT_ASSIGNED', 'GED_PRESENCE', []

    # UNKNOWN mission_type — still try classification
    if mission_type == 'UNKNOWN':
        return 'NOT_ASSIGNED', 'GED_PRESENCE', []

    # mission_type == 'REVIEWER' — classify via override rules then circuit matrix
    lookup_result = lookup_assignment(lot, type_doc, mission)
    if lookup_result is not None:
        assignment_type, source = lookup_result
        return assignment_type, source, []

    # R-NM4-02 / R-NM4-08 / R-NM4-09: No matrix entry
    # Conservative fallback: UNKNOWN_REQUIRED + UNKNOWN_ASSIGNMENT flag
    return 'UNKNOWN_REQUIRED', 'DISCIPLINE_FALLBACK', ['UNKNOWN_ASSIGNMENT']


# ---------------------------------------------------------------------------
# Step 3 — Keyword activation scan
# ---------------------------------------------------------------------------

def _check_keyword_activation(commentaire_texts: list, mission_name: str) -> bool:
    """Check if any keyword activates a conditional reviewer.

    R-NM4-05: deterministic, case-insensitive, multiple keywords may fire.
    """
    if not commentaire_texts or not mission_name:
        return False
    combined = ' '.join(
        str(c) for c in commentaire_texts if pd.notna(c)
    ).lower()
    if not combined.strip():
        return False
    for keyword, target_mission in KEYWORD_ACTIVATION.items():
        if keyword.lower() in combined and target_mission in mission_name.upper():
            return True
    return False


# ---------------------------------------------------------------------------
# Step 4 — Resolve final_response_status
# ---------------------------------------------------------------------------

def _resolve_final_status(assignment_type: str, response_status: str) -> str:
    """Resolve final_response_status after assignment classification."""
    if assignment_type == 'NOT_ASSIGNED':
        return 'NOT_APPLICABLE'
    if assignment_type == 'INFORMATIONAL':
        return 'NOT_APPLICABLE'
    if assignment_type == 'CONDITIONAL' and response_status == 'NOT_RESPONDED':
        return 'CONDITIONAL_NOT_TRIGGERED'
    # REQUIRED_VISA and UNKNOWN_REQUIRED pass through unchanged
    return response_status


# ---------------------------------------------------------------------------
# Step 5 — Document-level reviewer summary
# ---------------------------------------------------------------------------

def _build_doc_summary(doc_group: pd.DataFrame) -> dict:
    """Build reviewer summary for a single doc_id.

    R-NM4-06: HM reviewers excluded from consensus.
    R-NM4-07: MOEX/SAS excluded from assigned_reviewers.
    """
    # Only REVIEWER rows participate (MOEX/SAS excluded by assignment_type)
    reviewer_rows = doc_group[
        doc_group['assignment_type'].isin({'REQUIRED_VISA', 'UNKNOWN_REQUIRED'})
    ]

    assigned_reviewers = reviewer_rows['mission'].tolist()

    # HM handling (R-NM4-06)
    hm_mask = reviewer_rows['final_response_status'] == 'RESPONDED_HM'
    hm_reviewers = reviewer_rows.loc[hm_mask, 'mission'].tolist()
    relevant_rows = reviewer_rows[~hm_mask]
    relevant_reviewers = relevant_rows['mission'].tolist()

    # Informational reviewers
    informational_reviewers = doc_group.loc[
        doc_group['assignment_type'] == 'INFORMATIONAL', 'mission'
    ].tolist()

    # Conditional reviewers (still CONDITIONAL, not triggered)
    conditional_reviewers = doc_group.loc[
        doc_group['assignment_type'] == 'CONDITIONAL', 'mission'
    ].tolist()

    # Counts — on relevant_reviewers only
    responded_approve = int(
        (relevant_rows['final_response_status'] == 'RESPONDED_APPROVE').sum()
    )
    responded_reject = int(
        (relevant_rows['final_response_status'] == 'RESPONDED_REJECT').sum()
    )
    not_responded = int(
        (relevant_rows['final_response_status'] == 'NOT_RESPONDED').sum()
    )
    missing_reviewers = relevant_rows.loc[
        relevant_rows['final_response_status'] == 'NOT_RESPONDED', 'mission'
    ].tolist()
    blocking_reviewers = relevant_rows.loc[
        relevant_rows['final_response_status'] == 'RESPONDED_REJECT', 'mission'
    ].tolist()
    hm_count = len(hm_reviewers)

    return {
        'assigned_reviewers': assigned_reviewers,
        'hm_reviewers': hm_reviewers,
        'relevant_reviewers': relevant_reviewers,
        'informational_reviewers': informational_reviewers,
        'conditional_reviewers': conditional_reviewers,
        'responded_approve': responded_approve,
        'responded_reject': responded_reject,
        'not_responded': not_responded,
        'missing_reviewers': missing_reviewers,
        'blocking_reviewers': blocking_reviewers,
        'hm_count': hm_count,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def classify_assignments(
    ged_long: pd.DataFrame,
    circuit_matrix: dict = None,
) -> tuple:
    """Apply assignment classification to ged_long.

    Args:
        ged_long: NM1+NM3 output DataFrame (ged_long format)
        circuit_matrix: Deprecated, kept for API compatibility. Ignored.
                        The built-in circuit matrix from circuit_matrix.py is always used.

    Returns:
        (ged_long_enriched, nm4_summary)
        - ged_long_enriched: ged_long with assignment_type, assignment_source, final_response_status
        - nm4_summary: DataFrame with one row per doc_id containing reviewer summary
    """
    _validate_contract(ged_long)

    df = ged_long.copy()

    log_event(
        None, 'NM4', 'INFO', 'MATRIX_LOADED',
        'Using built-in circuit matrix from circuit_matrix.py',
    )

    # ------------------------------------------------------------------
    # Step 1+2: Classify assignment type (vectorized via apply on columns)
    # ------------------------------------------------------------------
    lot_vals = df['lot'].fillna('')
    type_doc_vals = df['type_doc'].fillna('') if 'type_doc' in df.columns else pd.Series('', index=df.index)
    mission_vals = df['mission'].fillna('')
    mt_vals = df['mission_type'].astype(str)

    results = []
    for mt, lot, td, mis in zip(mt_vals, lot_vals, type_doc_vals, mission_vals):
        results.append(_classify_assignment(mt, lot, td, mis))

    df['assignment_type'] = [r[0] for r in results]
    df['assignment_source'] = [r[1] for r in results]
    row_inference_flags = [r[2] for r in results]

    # ------------------------------------------------------------------
    # Step 3: Keyword activation for CONDITIONAL reviewers
    # ------------------------------------------------------------------
    conditional_mask = df['assignment_type'] == 'CONDITIONAL'
    if conditional_mask.any():
        # Build per-doc comment aggregation
        doc_comments = df.groupby('doc_id')['commentaire'].apply(list).to_dict()

        for idx in df.index[conditional_mask]:
            doc_id = df.at[idx, 'doc_id']
            mission_name = str(df.at[idx, 'mission'])
            comments = doc_comments.get(doc_id, [])
            try:
                activated = _check_keyword_activation(comments, mission_name)
            except Exception:
                log_event(
                    int(doc_id) if pd.notna(doc_id) else None,
                    'NM4', 'WARNING', 'KEYWORD_SCAN_FAILURE',
                    f'Keyword scan failed for mission {mission_name}',
                )
                activated = False

            if activated:
                df.at[idx, 'assignment_type'] = 'REQUIRED_VISA'
                df.at[idx, 'assignment_source'] = 'KEYWORD_TRIGGER'
                row_inference_flags[idx] = row_inference_flags[idx] + [
                    'CONDITIONAL_TRIGGERED_FROM_COMMENT'
                ]
                log_event(
                    int(doc_id) if pd.notna(doc_id) else None,
                    'NM4', 'INFO', 'CONDITIONAL_TRIGGERED',
                    f'Conditional reviewer {mission_name} activated via keyword',
                )

    # Store row-level inference flags (NM4-specific)
    df['nm4_inference_flags'] = row_inference_flags

    # ------------------------------------------------------------------
    # Handle duplicate reviewer rows per doc_id (edge case)
    # ------------------------------------------------------------------
    reviewer_mask = df['mission_type'].astype(str) == 'REVIEWER'
    if reviewer_mask.any():
        dup_check = df.loc[reviewer_mask].groupby(['doc_id', 'mission']).size()
        dups = dup_check[dup_check > 1]
        if len(dups) > 0:
            for (doc_id, mission_name), count in dups.items():
                log_event(
                    int(doc_id) if pd.notna(doc_id) else None,
                    'NM4', 'WARNING', 'DUPLICATE_REVIEWER_ROW',
                    f'Duplicate mission row: {mission_name} appears {count} times',
                )
            # Deduplicate: keep most recent date_reponse
            if 'date_reponse' in df.columns:
                df = df.sort_values('date_reponse', ascending=False, na_position='last')
                before_count = len(df)
                df = df.drop_duplicates(
                    subset=['doc_id', 'mission'],
                    keep='first',
                )
                # Re-index nm4_inference_flags after dedup
                row_inference_flags = df['nm4_inference_flags'].tolist()

    # ------------------------------------------------------------------
    # Step 4: Resolve final_response_status (vectorized)
    # ------------------------------------------------------------------
    df['final_response_status'] = [
        _resolve_final_status(at, rs)
        for at, rs in zip(df['assignment_type'], df['response_status'])
    ]

    # ------------------------------------------------------------------
    # Step 5: Build document-level summary
    # ------------------------------------------------------------------
    summary_records = []
    for doc_id, group in df.groupby('doc_id', sort=False):
        summary = _build_doc_summary(group)
        summary['doc_id'] = doc_id

        # Collect all nm4 inference flags for this doc
        doc_flags = []
        for flags in group['nm4_inference_flags']:
            if isinstance(flags, list):
                doc_flags.extend(flags)
        summary['inference_flags'] = list(set(doc_flags))

        summary_records.append(summary)

    nm4_summary = pd.DataFrame(summary_records)

    # ------------------------------------------------------------------
    # Convert assignment_type to category (GP-SCALE)
    # ------------------------------------------------------------------
    df['assignment_type'] = df['assignment_type'].astype('category')
    df['assignment_source'] = df['assignment_source'].astype('category')

    # ------------------------------------------------------------------
    # Validation checks
    # ------------------------------------------------------------------
    assert df['assignment_type'].notna().all(), "assignment_type has nulls"
    assert df['final_response_status'].notna().all(), "final_response_status has nulls"

    # INFORMATIONAL → NOT_APPLICABLE
    info_mask = df['assignment_type'] == 'INFORMATIONAL'
    if info_mask.any():
        assert (df.loc[info_mask, 'final_response_status'] == 'NOT_APPLICABLE').all(), \
            "INFORMATIONAL rows must have final_response_status=NOT_APPLICABLE"

    log_event(
        None, 'NM4', 'INFO', 'NM4_COMPLETE',
        f'NM4 complete: {len(df)} rows, '
        f'assignment_type dist: {df["assignment_type"].value_counts().to_dict()}, '
        f'summary: {len(nm4_summary)} docs',
    )

    return df, nm4_summary
