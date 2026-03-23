"""NM7-GED: Lifecycle State Engine, Blocker Model & Priority Scoring.

Computes operational lifecycle state, current blocker, and priority score
for every active document. Produces the priority queue and intake issues bucket.

NM7 operates on active_dataset from NM5 + NM2/NM4 summaries.
NM7 never modifies NM1–NM5 data (R-NM7-10). Read-only access.
"""

import pandas as pd
import numpy as np
from datetime import date

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.logging import log_event
from jansa.adapters.ged.constants import (
    LIFECYCLE_WEIGHT,
    BLOCKER_WEIGHT,
    CONFIDENCE_DEDUCTIONS,
    MOEX_DEFINITIVE_VERDICTS,
)
from jansa.adapters.ged.doc_type_config import (
    EXCLUDED_DOC_TYPES,
    get_priority_weight,
)


# ---------------------------------------------------------------------------
# Contract validation (P10)
# ---------------------------------------------------------------------------

def _validate_nm7_contract(merged: pd.DataFrame) -> None:
    """Validate NM7 merged input contract."""
    required = [
        'doc_id', 'famille_key', 'sas_state',
        'assigned_reviewers', 'relevant_reviewers',
        'responded_approve', 'responded_reject', 'not_responded',
        'revision_count', 'is_active',
    ]
    missing = [c for c in required if c not in merged.columns]
    if missing:
        raise ContractError(
            f"NM7 input contract violated — missing columns: {missing}"
        )


# ---------------------------------------------------------------------------
# Step 1 — MOEX synthesis identification
# ---------------------------------------------------------------------------

def _get_moex_verdict(moex_rows: list, doc_id) -> tuple:
    """Extract MOEX verdict from MOEX mission rows.

    Returns: (verdict, source_mission, confidence, extra_flags)
    """
    if not moex_rows:
        return None, None, 1.0, []

    decided = [
        r for r in moex_rows
        if r.get('reponse_normalized') in MOEX_DEFINITIVE_VERDICTS
    ]

    if len(decided) == 1:
        r = decided[0]
        return r['reponse_normalized'], r['mission'], 1.0, []

    if len(decided) > 1:
        # R-NM7-05: Multiple definitive MOEX verdicts — most recent wins
        decided.sort(
            key=lambda r: r.get('date_reponse') or pd.Timestamp.min,
            reverse=True,
        )
        log_event(
            int(doc_id) if pd.notna(doc_id) else None,
            'NM7', 'WARNING', 'MULTIPLE_MOEX_VERDICTS',
            f"Conflicting MOEX verdicts: "
            f"{[(r['mission'], r['reponse_normalized']) for r in decided]}. "
            f"Most recent ({decided[0]['mission']}: {decided[0]['reponse_normalized']}) used.",
        )
        return (
            decided[0]['reponse_normalized'],
            decided[0]['mission'],
            0.0,  # LOW confidence — will be clamped at 0.1
            ['MULTIPLE_MOEX_VERDICTS'],
        )

    # All MOEX rows pending — no verdict
    return None, None, 1.0, []


# ---------------------------------------------------------------------------
# Step 3 — Consensus type computation
# ---------------------------------------------------------------------------

def _compute_consensus(relevant_reviewers: list, responded_approve: int,
                       responded_reject: int, not_responded: int,
                       hm_count: int) -> str:
    """Compute consensus type from reviewer counts."""
    total_relevant = len(relevant_reviewers)
    if total_relevant == 0:
        return 'ALL_HM'
    if not_responded == total_relevant:
        return 'NOT_STARTED'
    if not_responded > 0:
        return 'INCOMPLETE'
    if responded_reject == 0 and responded_approve > 0:
        return 'ALL_APPROVE'
    if responded_reject > 0 and responded_approve == 0:
        return 'ALL_REJECT'
    if responded_reject > 0 and responded_approve > 0:
        return 'MIXED'
    return 'NOT_STARTED'


# ---------------------------------------------------------------------------
# Step 4 — Confidence score
# ---------------------------------------------------------------------------

def _compute_confidence(inference_flags: list) -> float:
    """Compute confidence score from inference flags."""
    score = 1.0
    for flag in inference_flags:
        score += CONFIDENCE_DEDUCTIONS.get(flag, 0)
    return max(0.1, score)


# ---------------------------------------------------------------------------
# Step 5 — Time metrics
# ---------------------------------------------------------------------------

def _compute_time_metrics(row: dict, ref_date: date) -> dict:
    """Compute time-based metrics for a document."""
    date_depot = row.get('date_depot')
    deadline = row.get('deadline')

    days_since_depot = None
    if pd.notna(date_depot):
        try:
            days_since_depot = (ref_date - pd.Timestamp(date_depot).date()).days
        except Exception:
            days_since_depot = None

    days_until_deadline = None
    if pd.notna(deadline):
        try:
            days_until_deadline = (pd.Timestamp(deadline).date() - ref_date).days
        except Exception:
            days_until_deadline = None

    is_overdue = days_until_deadline is not None and days_until_deadline < 0
    days_overdue = abs(days_until_deadline) if is_overdue else 0
    has_deadline = days_until_deadline is not None

    return {
        'days_since_depot': days_since_depot,
        'days_until_deadline': days_until_deadline,
        'is_overdue': is_overdue,
        'days_overdue': days_overdue,
        'has_deadline': has_deadline,
    }


# ---------------------------------------------------------------------------
# Step 6 — Priority scoring
# ---------------------------------------------------------------------------

def _delay_weight(days_overdue: int, has_deadline: bool) -> int:
    """Compute delay weight component."""
    if not has_deadline:
        return -10
    if days_overdue > 30:
        return 40
    if days_overdue > 15:
        return 25
    if days_overdue > 0:
        return 10
    return 0


def _revision_penalty(revision_count: int) -> int:
    """Compute revision penalty component."""
    if revision_count > 2:
        return 15
    if revision_count == 2:
        return 8
    return 0


def _priority_category(score: int) -> str:
    """Map priority score to category."""
    if score >= 150:
        return 'CRITICAL'
    if score >= 100:
        return 'HIGH'
    if score >= 60:
        return 'MEDIUM'
    return 'LOW'


# ---------------------------------------------------------------------------
# Step 2 — Lifecycle state decision tree (per document)
# ---------------------------------------------------------------------------

def _classify_document(doc: dict, moex_rows: list) -> dict:
    """Apply lifecycle decision tree to a single document.

    Returns dict with all NM7 output fields.
    """
    doc_id = doc['doc_id']

    # --- MOEX verdict extraction ---
    visa_global, visa_global_source, moex_conf, extra_flags = _get_moex_verdict(
        moex_rows, doc_id
    )

    # Accumulate inference flags from all upstream modules
    inference_flags = list(doc.get('inference_flags', []))
    # Merge nm4 flags
    nm4_flags = doc.get('nm4_inference_flags', [])
    if isinstance(nm4_flags, list):
        inference_flags.extend(nm4_flags)
    # Merge MOEX extra_flags (P6 mandatory propagation)
    inference_flags.extend(extra_flags)

    sas_state = doc.get('sas_state', 'SAS_UNKNOWN')
    assigned_reviewers = doc.get('assigned_reviewers', [])
    relevant_reviewers = doc.get('relevant_reviewers', [])
    responded_approve = int(doc.get('responded_approve', 0))
    responded_reject = int(doc.get('responded_reject', 0))
    not_responded = int(doc.get('not_responded', 0))
    missing_reviewers = doc.get('missing_reviewers', [])
    blocking_reviewers = doc.get('blocking_reviewers', [])
    hm_count = int(doc.get('hm_count', 0))
    revision_count = doc.get('revision_count', 1)
    if pd.isna(revision_count) or revision_count is None:
        revision_count = 1
        log_event(int(doc_id) if pd.notna(doc_id) else None,
                  'NM7', 'WARNING', 'NULL_REVISION_COUNT',
                  'revision_count is null — treated as 1')
    revision_count = int(revision_count)
    deposant = doc.get('deposant', 'Unknown company')

    # Result template
    result = {
        'doc_id': doc_id,
        'famille_key': doc.get('famille_key'),
        'lot': doc.get('lot'),
        'batiment': doc.get('batiment'),
        'type_doc': doc.get('type_doc'),
        'visa_global': visa_global,
        'visa_global_source': visa_global_source,
        'sas_state': sas_state,
        'lifecycle_state': None,
        'queue_destination': None,
        'current_blocker': None,
        'blocker_type': 'NONE',
        'blocker_reason': None,
        'consensus_type': None,
        'revision_count': revision_count,
    }

    # === Branch 1: SYNTHESIS_ISSUED ===
    if visa_global is not None:
        result['lifecycle_state'] = 'SYNTHESIS_ISSUED'
        result['queue_destination'] = 'EXCLUDED'
        result['blocker_type'] = 'NONE'
        result['consensus_type'] = _compute_consensus(
            relevant_reviewers, responded_approve, responded_reject,
            not_responded, hm_count,
        )
        result['inference_flags'] = inference_flags
        return result

    # === Branch 2: SAS_BLOCKED ===
    if sas_state == 'SAS_BLOCKED':
        result['lifecycle_state'] = 'SAS_BLOCKED'
        result['queue_destination'] = 'INTAKE_ISSUES'
        result['current_blocker'] = deposant
        result['blocker_type'] = 'COMPANY'
        result['blocker_reason'] = (
            "Document rejected at SAS intake. Company must correct and re-submit."
        )
        result['consensus_type'] = _compute_consensus(
            relevant_reviewers, responded_approve, responded_reject,
            not_responded, hm_count,
        )
        result['inference_flags'] = inference_flags
        return result

    # === Branch 3: SAS_PENDING ===
    if sas_state == 'SAS_PENDING':
        result['lifecycle_state'] = 'SAS_PENDING'
        result['queue_destination'] = 'INTAKE_ISSUES'
        result['current_blocker'] = 'GEMO-SAS'
        result['blocker_type'] = 'GEMO_SAS'
        result['blocker_reason'] = "Awaiting SAS conformity check by GEMO."
        result['consensus_type'] = _compute_consensus(
            relevant_reviewers, responded_approve, responded_reject,
            not_responded, hm_count,
        )
        result['inference_flags'] = inference_flags
        return result

    # === Branch 4: SAS_UNKNOWN → treated as passed ===
    if sas_state == 'SAS_UNKNOWN':
        if 'SAS_ASSUMED_PASSED' not in inference_flags:
            inference_flags.append('SAS_ASSUMED_PASSED')
        # Continue to branch 5

    # === Branch 5: HM_EXCLUDED ===
    if len(relevant_reviewers) == 0 and len(assigned_reviewers) > 0:
        result['lifecycle_state'] = 'HM_EXCLUDED'
        result['queue_destination'] = 'EXCLUDED'
        result['blocker_type'] = 'NONE'
        result['consensus_type'] = 'ALL_HM'
        result['inference_flags'] = inference_flags
        return result

    # === Branch 6: ZERO REVIEWER ROWS ===
    if len(assigned_reviewers) == 0:
        result['lifecycle_state'] = 'NOT_STARTED'
        result['queue_destination'] = 'PRIORITY_QUEUE'
        result['current_blocker'] = 'MOEX — no reviewers assigned'
        result['blocker_type'] = 'GEMO_MOEX'
        result['blocker_reason'] = (
            "No reviewers assigned to this document. "
            "MOEX should verify circuit assignment."
        )
        result['consensus_type'] = 'NOT_STARTED'
        result['inference_flags'] = inference_flags
        return result

    total_responded = responded_approve + responded_reject

    # === Branch 7: NOT_STARTED ===
    if len(relevant_reviewers) > 0 and total_responded == 0:
        first_reviewer = sorted(assigned_reviewers)[0] if assigned_reviewers else None
        result['lifecycle_state'] = 'NOT_STARTED'
        result['queue_destination'] = 'PRIORITY_QUEUE'
        result['current_blocker'] = first_reviewer
        result['blocker_type'] = 'CONSULTANT'
        result['blocker_reason'] = (
            "No responses yet. Reviewers: " + ', '.join(assigned_reviewers)
        )
        result['consensus_type'] = 'NOT_STARTED'
        result['inference_flags'] = inference_flags
        return result

    # === Branch 8: WAITING_RESPONSES ===
    if not_responded > 0 and total_responded > 0:
        first_missing = missing_reviewers[0] if missing_reviewers else None
        result['lifecycle_state'] = 'WAITING_RESPONSES'
        result['queue_destination'] = 'PRIORITY_QUEUE'
        result['current_blocker'] = first_missing
        result['blocker_type'] = 'CONSULTANT'
        result['blocker_reason'] = (
            "Awaiting responses from: " + ', '.join(missing_reviewers)
        )
        result['consensus_type'] = 'INCOMPLETE'
        result['inference_flags'] = inference_flags
        return result

    # === Branch 9: ALL RESPONSES RECEIVED ===
    if not_responded == 0:
        # Sub-case: ALL_APPROVE
        if responded_reject == 0 and responded_approve > 0:
            result['consensus_type'] = 'ALL_APPROVE'
            result['lifecycle_state'] = 'READY_TO_ISSUE'
            result['current_blocker'] = 'MOEX'
            result['blocker_type'] = 'GEMO_MOEX'
            result['blocker_reason'] = (
                "All required reviewers approved. MOEX to issue synthesis."
            )
            result['queue_destination'] = 'PRIORITY_QUEUE'

            # R-NM7-12: ALL_APPROVE + no MOEX row
            if not moex_rows:
                log_event(
                    int(doc_id) if pd.notna(doc_id) else None,
                    'NM7', 'WARNING', 'MISSING_MOEX_ASSIGNMENT',
                    "All reviewers approved but no MOEX mission row found. "
                    "MOEX circuit assignment should be verified.",
                )
                inference_flags.append('MISSING_MOEX_ASSIGNMENT')

            result['inference_flags'] = inference_flags
            return result

        # Sub-case: ALL_REJECT
        if responded_reject > 0 and responded_approve == 0:
            result['consensus_type'] = 'ALL_REJECT'
            if revision_count == 1:
                result['lifecycle_state'] = 'FAST_REJECT'
            else:
                result['lifecycle_state'] = 'CHRONIC_BLOCKED'
            result['current_blocker'] = deposant
            result['blocker_type'] = 'COMPANY'
            result['blocker_reason'] = (
                "First submission rejected by all reviewers."
                if revision_count == 1
                else "Repeated rejection across multiple revisions."
            )
            result['queue_destination'] = 'PRIORITY_QUEUE'
            result['inference_flags'] = inference_flags
            return result

        # Sub-case: MIXED
        if responded_reject > 0 and responded_approve > 0:
            result['consensus_type'] = 'MIXED'
            result['lifecycle_state'] = 'CONFLICT'
            result['current_blocker'] = 'MOEX'
            result['blocker_type'] = 'GEMO_MOEX'
            result['blocker_reason'] = (
                "Conflicting reviewer opinions. Blocking: "
                + ', '.join(blocking_reviewers)
            )
            result['queue_destination'] = 'PRIORITY_QUEUE'
            result['inference_flags'] = inference_flags
            return result

    # Fallback — should not reach here
    log_event(
        int(doc_id) if pd.notna(doc_id) else None,
        'NM7', 'ERROR', 'UNCLASSIFIED_DOCUMENT',
        'Document fell through all lifecycle branches — defaulting to NOT_STARTED',
    )
    result['lifecycle_state'] = 'NOT_STARTED'
    result['queue_destination'] = 'PRIORITY_QUEUE'
    result['blocker_type'] = 'NONE'
    result['consensus_type'] = 'NOT_STARTED'
    result['inference_flags'] = inference_flags
    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_nm7(
    active_dataset: pd.DataFrame,
    nm2_result: pd.DataFrame,
    nm4_summary: pd.DataFrame,
    nm5_doc_level: pd.DataFrame,
    reference_date: date = None,
) -> tuple:
    """Run NM7 lifecycle engine on active dataset.

    Args:
        active_dataset: NM5 active_dataset (ged_long filtered to is_active=True)
        nm2_result: NM2 output (one row per doc_id with sas_state)
        nm4_summary: NM4 document-level summary
        nm5_doc_level: NM5 doc_level with revision metadata
        reference_date: Reference date for time computations (default: today)

    Returns:
        (priority_queue, intake_issues, nm7_output)
        - priority_queue: docs routed to PRIORITY_QUEUE, sorted by priority
        - intake_issues: docs routed to INTAKE_ISSUES
        - nm7_output: all classified docs (including EXCLUDED), full NM7 columns
    """
    if reference_date is None:
        reference_date = date.today()

    # ------------------------------------------------------------------
    # Step 0: Compute is_late on active_dataset (P9 — vectorized)
    # ------------------------------------------------------------------
    active_dataset = active_dataset.copy()
    ecart_numeric = pd.to_numeric(active_dataset['ecart_reponse'], errors='coerce')
    active_dataset['is_late'] = (
        active_dataset['date_reponse'].notna() &
        ecart_numeric.notna() &
        (ecart_numeric < 0)
    )

    # ------------------------------------------------------------------
    # Step 1: Build document-level merge
    # ------------------------------------------------------------------
    # Get unique doc-level info from active_dataset
    doc_info_cols = ['doc_id', 'famille_key', 'lot', 'batiment', 'type_doc', 'deposant']
    available_doc_cols = [c for c in doc_info_cols if c in active_dataset.columns]
    doc_info = active_dataset.drop_duplicates('doc_id')[available_doc_cols].copy()

    # Get date_depot and deadline per doc (first non-null)
    if 'date_depot' in active_dataset.columns:
        depot_map = active_dataset.drop_duplicates('doc_id').set_index('doc_id')['date_depot']
        doc_info['date_depot'] = doc_info['doc_id'].map(depot_map)
    if 'deadline' in active_dataset.columns:
        deadline_map = active_dataset.drop_duplicates('doc_id').set_index('doc_id')['deadline']
        doc_info['deadline'] = doc_info['doc_id'].map(deadline_map)

    # Merge NM2 (sas_state, inference_flags)
    nm2_cols = ['doc_id', 'sas_state', 'sas_confidence', 'inference_flags']
    nm2_avail = [c for c in nm2_cols if c in nm2_result.columns]
    merged = doc_info.merge(nm2_result[nm2_avail], on='doc_id', how='left')

    # Merge NM4 summary
    nm4_cols = [
        'doc_id', 'assigned_reviewers', 'relevant_reviewers',
        'hm_reviewers', 'informational_reviewers', 'conditional_reviewers',
        'responded_approve', 'responded_reject', 'not_responded',
        'missing_reviewers', 'blocking_reviewers', 'hm_count',
    ]
    nm4_avail = [c for c in nm4_cols if c in nm4_summary.columns]
    # Also grab nm4 inference_flags separately to avoid collision
    if 'inference_flags' in nm4_summary.columns:
        nm4_for_merge = nm4_summary[nm4_avail].copy()
        nm4_for_merge['nm4_inference_flags'] = nm4_summary['inference_flags']
    else:
        nm4_for_merge = nm4_summary[nm4_avail].copy()
        nm4_for_merge['nm4_inference_flags'] = [[] for _ in range(len(nm4_for_merge))]
    merged = merged.merge(nm4_for_merge, on='doc_id', how='left')

    # Merge NM5 doc_level (revision_count, is_active)
    nm5_cols = ['doc_id', 'revision_count', 'is_active']
    nm5_avail = [c for c in nm5_cols if c in nm5_doc_level.columns]
    merged = merged.merge(nm5_doc_level[nm5_avail], on='doc_id', how='left')

    # Fill defaults for missing merges
    merged['sas_state'] = merged['sas_state'].fillna('SAS_UNKNOWN')
    merged['revision_count'] = merged['revision_count'].fillna(1).astype(int)
    merged['is_active'] = merged['is_active'].fillna(True)

    for col in ['assigned_reviewers', 'relevant_reviewers', 'hm_reviewers',
                'missing_reviewers', 'blocking_reviewers']:
        merged[col] = merged[col].apply(lambda x: x if isinstance(x, list) else [])
    for col in ['responded_approve', 'responded_reject', 'not_responded', 'hm_count']:
        merged[col] = merged[col].fillna(0).astype(int)
    merged['inference_flags'] = merged['inference_flags'].apply(
        lambda x: x if isinstance(x, list) else []
    )
    merged['nm4_inference_flags'] = merged['nm4_inference_flags'].apply(
        lambda x: x if isinstance(x, list) else []
    )

    # Contract validation
    _validate_nm7_contract(merged)

    # ------------------------------------------------------------------
    # R-NM7-11: Exclude docs by doc_type_config workflow=EXCLUDED
    # ------------------------------------------------------------------
    if 'type_doc' in merged.columns:
        excluded_mask = merged['type_doc'].isin(EXCLUDED_DOC_TYPES)
        if excluded_mask.any():
            excluded_count = excluded_mask.sum()
            log_event(
                None, 'NM7', 'INFO', 'EXCLUDED_BY_DOC_TYPE_CONFIG',
                f'{excluded_count} docs excluded by doc_type_config: '
                f'{merged.loc[excluded_mask, "type_doc"].value_counts().to_dict()}',
            )
            merged = merged[~excluded_mask].copy()

    # ------------------------------------------------------------------
    # Step 2: Collect MOEX rows per doc from active_dataset
    # ------------------------------------------------------------------
    moex_rows_all = active_dataset[
        active_dataset['mission_type'].astype(str) == 'MOEX'
    ]
    moex_by_doc = {}
    if len(moex_rows_all) > 0:
        for doc_id, group in moex_rows_all.groupby('doc_id', sort=False):
            moex_by_doc[doc_id] = group.to_dict('records')

    # ------------------------------------------------------------------
    # Step 3: Classify each document (vectorized via list comprehension)
    # ------------------------------------------------------------------
    results = [
        _classify_document(
            row.to_dict(),
            moex_by_doc.get(row['doc_id'], []),
        )
        for _, row in merged.iterrows()
    ]
    # Note: iterrows used here on merged (doc-level, not row-level).
    # This is a doc-level aggregate — one row per doc_id, not the full ged_long.

    nm7_output = pd.DataFrame(results)

    # ------------------------------------------------------------------
    # Step 4: Compute confidence score
    # ------------------------------------------------------------------
    nm7_output['confidence_score'] = nm7_output['inference_flags'].apply(
        _compute_confidence
    )

    # ------------------------------------------------------------------
    # Step 5: Compute time metrics
    # ------------------------------------------------------------------
    time_records = []
    for _, row in nm7_output.iterrows():
        time_records.append(_compute_time_metrics(
            {
                'date_depot': merged.loc[merged['doc_id'] == row['doc_id'], 'date_depot'].iloc[0]
                if 'date_depot' in merged.columns and len(merged[merged['doc_id'] == row['doc_id']]) > 0
                else None,
                'deadline': merged.loc[merged['doc_id'] == row['doc_id'], 'deadline'].iloc[0]
                if 'deadline' in merged.columns and len(merged[merged['doc_id'] == row['doc_id']]) > 0
                else None,
            },
            reference_date,
        ))
    time_df = pd.DataFrame(time_records, index=nm7_output.index)
    for col in time_df.columns:
        nm7_output[col] = time_df[col]

    # ------------------------------------------------------------------
    # Step 6: Compute priority score and category
    # ------------------------------------------------------------------
    nm7_output['priority_score_base'] = nm7_output.apply(
        lambda row: (
            LIFECYCLE_WEIGHT.get(row['lifecycle_state'], 0) +
            _delay_weight(row['days_overdue'], row['has_deadline']) +
            BLOCKER_WEIGHT.get(row['blocker_type'], 0) +
            _revision_penalty(row['revision_count'])
        ),
        axis=1,
    )

    # Apply doc_type priority_weight (Phase 1)
    nm7_output['doc_type_weight'] = nm7_output['type_doc'].apply(
        lambda td: get_priority_weight(td) if pd.notna(td) else 1.0
    )
    nm7_output['priority_score'] = (
        nm7_output['priority_score_base'] * nm7_output['doc_type_weight']
    ).round(0).astype(int)

    # Clamp to 0-200
    nm7_output['priority_score'] = nm7_output['priority_score'].clip(0, 200)
    nm7_output['priority_category'] = nm7_output['priority_score'].apply(
        _priority_category
    )

    # ------------------------------------------------------------------
    # Step 6b: Aggregate is_late per doc_id from active_dataset
    # ------------------------------------------------------------------
    late_counts = active_dataset.groupby('doc_id')['is_late'].sum().astype(int)
    nm7_output['late_response_count'] = nm7_output['doc_id'].map(late_counts).fillna(0).astype(int)

    # ------------------------------------------------------------------
    # Step 7: Route to queues
    # ------------------------------------------------------------------
    pq_mask = nm7_output['queue_destination'] == 'PRIORITY_QUEUE'
    ii_mask = nm7_output['queue_destination'] == 'INTAKE_ISSUES'

    priority_queue = nm7_output[pq_mask].copy()
    intake_issues = nm7_output[ii_mask].copy()

    # Deterministic sort: priority_score DESC, days_overdue DESC,
    # days_since_depot DESC, famille_key ASC
    priority_queue = priority_queue.sort_values(
        ['priority_score', 'days_overdue', 'days_since_depot', 'famille_key'],
        ascending=[False, False, False, True],
        na_position='last',
    ).reset_index(drop=True)

    intake_issues = intake_issues.sort_values(
        ['lifecycle_state', 'famille_key'],
        ascending=[True, True],
    ).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Validation checks
    # ------------------------------------------------------------------
    assert nm7_output['lifecycle_state'].notna().all(), "lifecycle_state has nulls"
    assert nm7_output['priority_score'].notna().all(), "priority_score has nulls"
    assert nm7_output['queue_destination'].notna().all(), "queue_destination has nulls"
    assert nm7_output['consensus_type'].notna().all(), "consensus_type has nulls"
    assert nm7_output['priority_score'].between(0, 200).all(), "priority_score out of 0-200 range"

    # SYNTHESIS_ISSUED must be EXCLUDED
    synth = nm7_output[nm7_output['lifecycle_state'] == 'SYNTHESIS_ISSUED']
    if len(synth) > 0:
        assert (synth['queue_destination'] == 'EXCLUDED').all(), \
            "SYNTHESIS_ISSUED docs must be in EXCLUDED"

    # SAS_BLOCKED/PENDING must be INTAKE_ISSUES
    sas_issues = nm7_output[nm7_output['lifecycle_state'].isin({'SAS_BLOCKED', 'SAS_PENDING'})]
    if len(sas_issues) > 0:
        assert (sas_issues['queue_destination'] == 'INTAKE_ISSUES').all(), \
            "SAS_BLOCKED/PENDING must be in INTAKE_ISSUES"

    log_event(
        None, 'NM7', 'INFO', 'NM7_COMPLETE',
        f'NM7 complete: {len(nm7_output)} docs processed, '
        f'{len(priority_queue)} in priority queue, '
        f'{len(intake_issues)} in intake issues, '
        f'{len(nm7_output) - len(priority_queue) - len(intake_issues)} excluded',
    )

    return priority_queue, intake_issues, nm7_output
