"""Export NM1→NM7 pipeline results as JSON files for the visasist-ui.

Runs the full pipeline on the new Excel file and outputs JSON in the format
expected by the UI's data adapters (queue-adapter, dashboard-adapter, document-adapter).
"""

import sys
import os
import json
import math
from datetime import date, datetime

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))

from jansa.adapters.ged.nm1_loader import load_ged_export
from jansa.adapters.ged.nm3_vocab import normalize_responses
from jansa.adapters.ged.nm2_sas import interpret_sas
from jansa.adapters.ged.nm4_assignment import classify_assignments
from jansa.adapters.ged.nm5_revision import compute_active_dataset
from jansa.adapters.ged.legacy_loader import load_ancien_flags
from jansa.pipeline.nm7_lifecycle import run_nm7
from jansa.adapters.ged.logging import get_log, clear_log


# ── Lifecycle → UI category mapping ─────────────────────────────────────
LIFECYCLE_TO_CATEGORY = {
    'CONFLICT':           'CONFLICT',
    'NEEDS_ARBITRATION':  'CONFLICT',
    'READY_TO_ISSUE':     'EASY_WIN_APPROVE',
    'READY_TO_REJECT':    'FAST_REJECT',
    'CHRONIC_BLOCKED':    'BLOCKED',
    'BLOCKED':            'BLOCKED',
    'SAS_BLOCKED':        'BLOCKED',
    'WAITING_RESPONSES':  'WAITING',
    'SAS_PENDING':        'WAITING',
    'NOT_STARTED':        'NOT_STARTED',
    'SYNTHESIS_ISSUED':   'NOT_STARTED',
    'EXCLUDED':           'NOT_STARTED',
}


def safe_val(v):
    """Convert numpy/pandas types to JSON-safe Python types."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        if pd.isna(v):
            return None
        return v.isoformat()
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (np.ndarray, list)):
        return [safe_val(x) for x in v]
    return v


def run_pipeline(data_dir: str, ref_date: date):
    """Run full NM1→NM7 pipeline and return all intermediate results."""
    filepath = os.path.join(data_dir, '17&CO Tranche 2 du 23 mars 2026 07_45.xlsx')
    if not os.path.exists(filepath):
        filepath = os.path.join(data_dir, 'GrandFichier_1.xlsx')

    gf_path = os.path.join(data_dir, 'GrandFichier_1.xlsx')
    ancien_set = load_ancien_flags(gf_path) if os.path.exists(gf_path) else set()

    print(f'[NM1] Loading {os.path.basename(filepath)}...')
    ged_long, nm1_log = load_ged_export(filepath)
    print(f'[NM1] {len(ged_long):,} rows, {ged_long["doc_id"].nunique():,} docs')

    print('[NM3] Normalizing responses...')
    ged_long = normalize_responses(ged_long)

    print('[NM2] Interpreting SAS...')
    nm2_result = interpret_sas(ged_long)

    print('[NM4] Classifying assignments...')
    ged_long, nm4_summary = classify_assignments(ged_long, circuit_matrix=None)

    print('[NM5] Computing active dataset...')
    ged_enriched, doc_level, active_dataset = compute_active_dataset(
        ged_long, ancien_set=ancien_set if ancien_set else None,
    )

    print('[NM7] Running lifecycle/priority...')
    priority_queue, intake_issues, nm7_output = run_nm7(
        active_dataset, nm2_result, nm4_summary, doc_level,
        reference_date=ref_date,
    )
    print(f'[NM7] {len(nm7_output):,} docs, {len(priority_queue):,} in priority queue')

    logs = get_log()
    return {
        'ged_long': ged_long,
        'nm1_log': nm1_log,
        'nm2_result': nm2_result,
        'nm4_summary': nm4_summary,
        'active_dataset': active_dataset,
        'doc_level': doc_level,
        'priority_queue': priority_queue,
        'intake_issues': intake_issues,
        'nm7_output': nm7_output,
        'logs': logs,
        'source_file': os.path.basename(filepath),
    }


def build_approver_map(active_dataset: pd.DataFrame) -> dict:
    """Build per-doc reviewer status pivot from long-format active_dataset.

    Returns: {doc_id: [{mission, repondant, response, status, assignment}, ...]}
    """
    approver_map = {}
    cols = ['doc_id', 'mission', 'repondant', 'reponse_normalized',
            'response_status', 'assignment_type', 'deadline', 'date_reponse']
    subset = active_dataset[cols].copy()

    for doc_id, group in subset.groupby('doc_id'):
        reviewers = []
        for _, row in group.iterrows():
            if pd.isna(row['mission']):
                continue
            reviewers.append({
                'mission': safe_val(row['mission']),
                'repondant': safe_val(row['repondant']),
                'response': safe_val(row['reponse_normalized']),
                'status': safe_val(row['response_status']),
                'assignment': safe_val(row['assignment_type']),
                'deadline': safe_val(row['deadline']),
                'date_reponse': safe_val(row['date_reponse']),
            })
        approver_map[doc_id] = reviewers
    return approver_map


def derive_approver_summary(reviewers: list) -> dict:
    """From a list of reviewer dicts, derive missing and blocking approvers."""
    missing = []
    blocking = []
    approver_details = []

    for r in reviewers:
        mission = r.get('mission') or 'Unknown'
        status = r.get('status')
        response = r.get('response')

        detail = {
            'canonical_key': mission.replace(' ', '_').replace('-', '_').upper(),
            'display_name': mission,
            'is_assigned': True,
            'statut': response,
            'statut_raw': response,
            'date': r.get('date_reponse'),
            'numero_visa': None,
            'is_blocking': status in ('RESPONDED_REJECT', 'RESPONDED_RESERVE'),
            'is_pending': status in ('NOT_RESPONDED', None),
            'is_hm': response == 'HM',
        }
        approver_details.append(detail)

        if status in ('NOT_RESPONDED', None) and r.get('assignment') == 'REQUIRED_VISA':
            missing.append(mission)
        if status in ('RESPONDED_REJECT', 'RESPONDED_RESERVE'):
            blocking.append(mission)

    return {
        'missing_approvers': missing,
        'blocking_approvers': blocking,
        'approver_details': approver_details,
    }


def build_queue_json(pipeline_data: dict, ref_date: date) -> list:
    """Convert NM7 priority_queue to the JSON format expected by queue-adapter.ts."""
    pq = pipeline_data['priority_queue']
    doc_level = pipeline_data['doc_level']
    active_dataset = pipeline_data['active_dataset']
    approver_map = build_approver_map(active_dataset)

    # Build doc_level lookup
    dl_lookup = {}
    for _, row in doc_level.iterrows():
        dl_lookup[row['doc_id']] = row

    # Build first active_dataset row per doc for metadata
    first_row_map = {}
    for _, row in active_dataset.iterrows():
        did = row['doc_id']
        if did not in first_row_map:
            first_row_map[did] = row

    items = []
    for idx, row in pq.iterrows():
        doc_id = row['doc_id']
        dl = dl_lookup.get(doc_id)
        first_row = first_row_map.get(doc_id)

        # Get approver summary
        reviewers = approver_map.get(doc_id, [])
        appr_summary = derive_approver_summary(reviewers)

        # Map lifecycle_state to UI category
        ls = row.get('lifecycle_state', 'NOT_STARTED')
        category = LIFECYCLE_TO_CATEGORY.get(ls, 'NOT_STARTED')

        # Build doc_version_key from doc_level
        dvk = dl['doc_version_key'] if dl is not None else str(doc_id)
        dfk = row.get('famille_key', dl['famille_key'] if dl is not None else str(doc_id))

        # Get metadata from first active_dataset row
        chemin = first_row['chemin'] if first_row is not None else None
        libelle = first_row['libelle'] if first_row is not None else None
        indice = dl['indice'] if dl is not None else None
        ind_sort = dl['indice_sort_order'] if dl is not None else 0
        date_depot = first_row['date_depot'] if first_row is not None else None
        date_prevue = first_row['date_prevue'] if first_row is not None else None
        lot_val = row.get('lot')
        is_cross_lot = bool(dl['is_cross_lot']) if dl is not None else False

        item = {
            'doc_version_key': safe_val(dvk),
            'doc_family_key': safe_val(dfk),
            'row_id': f'nm7_{doc_id}',
            'source_sheet': safe_val(lot_val) or 'Unknown',
            'source_row': int(doc_id) if isinstance(doc_id, (int, np.integer)) else 0,
            'document': safe_val(dfk),
            'document_raw': safe_val(chemin),
            'titre': safe_val(libelle),
            'lot': safe_val(lot_val),
            'type_doc': safe_val(row.get('type_doc')),
            'niv': None,
            'ind': safe_val(indice),
            'ind_sort_order': safe_val(ind_sort) or 0,
            'date_diffusion': safe_val(date_depot),
            'date_contractuelle_visa': safe_val(date_prevue),
            'days_since_diffusion': safe_val(row.get('days_since_depot')),
            'days_until_deadline': safe_val(row.get('days_until_deadline')),
            'days_overdue': safe_val(row.get('days_overdue', 0)) or 0,
            'is_overdue': bool(row.get('is_overdue', False)),
            'priority_score': safe_val(row.get('priority_score', 0)) or 0,
            'category': category,
            'consensus_type': safe_val(row.get('consensus_type', 'NOT_STARTED')),
            'missing_approvers': appr_summary['missing_approvers'],
            'blocking_approvers': appr_summary['blocking_approvers'],
            'revision_count': safe_val(row.get('revision_count', 1)) or 1,
            'is_latest': True,
            'is_cross_lot': is_cross_lot,
            'row_quality': 'OK',
            'duplicate_flag': 'UNIQUE',
            # Extra NM7 fields
            'lifecycle_state': safe_val(ls),
            'priority_category': safe_val(row.get('priority_category')),
            'blocker_type': safe_val(row.get('blocker_type')),
            'blocker_reason': safe_val(row.get('blocker_reason')),
            'confidence_score': safe_val(row.get('confidence_score')),
            'inference_flags': safe_val(row.get('inference_flags', [])),
        }
        items.append(item)

    return items


def build_enriched_dataset_json(pipeline_data: dict, ref_date: date) -> list:
    """Convert NM7 full output to enriched_master_dataset format for document detail."""
    nm7 = pipeline_data['nm7_output']
    doc_level = pipeline_data['doc_level']
    active_dataset = pipeline_data['active_dataset']
    approver_map = build_approver_map(active_dataset)

    dl_lookup = {row['doc_id']: row for _, row in doc_level.iterrows()}
    first_row_map = {}
    for _, row in active_dataset.iterrows():
        did = row['doc_id']
        if did not in first_row_map:
            first_row_map[did] = row

    docs = []
    for _, row in nm7.iterrows():
        doc_id = row['doc_id']
        dl = dl_lookup.get(doc_id)
        first_row = first_row_map.get(doc_id)
        reviewers = approver_map.get(doc_id, [])
        appr_summary = derive_approver_summary(reviewers)

        ls = row.get('lifecycle_state', 'NOT_STARTED')
        category = LIFECYCLE_TO_CATEGORY.get(ls, 'NOT_STARTED')

        dvk = dl['doc_version_key'] if dl is not None else str(doc_id)
        dfk = row.get('famille_key', dl['famille_key'] if dl is not None else str(doc_id))
        chemin = first_row['chemin'] if first_row is not None else None
        libelle = first_row['libelle'] if first_row is not None else None
        indice = dl['indice'] if dl is not None else None
        ind_sort = dl['indice_sort_order'] if dl is not None else 0
        date_depot = first_row['date_depot'] if first_row is not None else None
        date_prevue = first_row['date_prevue'] if first_row is not None else None
        lot_val = row.get('lot')
        is_cross_lot = bool(dl['is_cross_lot']) if dl is not None else False
        cross_lot_list = safe_val(dl['cross_lot_list']) if dl is not None else None

        doc = {
            'doc_version_key': safe_val(dvk),
            'doc_family_key': safe_val(dfk),
            'row_id': f'nm7_{doc_id}',
            'source_sheet': safe_val(lot_val) or 'Unknown',
            'source_row': int(doc_id) if isinstance(doc_id, (int, np.integer)) else 0,
            'document': safe_val(dfk),
            'document_raw': safe_val(chemin),
            'titre': safe_val(libelle),
            'lot': safe_val(lot_val),
            'type_doc': safe_val(row.get('type_doc')),
            'niv': None,
            'ind': safe_val(indice),
            'ind_sort_order': safe_val(ind_sort) or 0,
            'date_diffusion': safe_val(date_depot),
            'date_contractuelle_visa': safe_val(date_prevue),
            'days_since_diffusion': safe_val(row.get('days_since_depot')),
            'days_until_deadline': safe_val(row.get('days_until_deadline')),
            'days_overdue': safe_val(row.get('days_overdue', 0)) or 0,
            'is_overdue': bool(row.get('is_overdue', False)),
            'priority_score': safe_val(row.get('priority_score', 0)) or 0,
            'score_band': 'CRITICAL' if (row.get('priority_score', 0) or 0) >= 80
                         else 'HIGH' if (row.get('priority_score', 0) or 0) >= 60
                         else 'MEDIUM' if (row.get('priority_score', 0) or 0) >= 40
                         else 'LOW',
            'category': category,
            'consensus_type': safe_val(row.get('consensus_type', 'NOT_STARTED')),
            'missing_approvers': appr_summary['missing_approvers'],
            'blocking_approvers': appr_summary['blocking_approvers'],
            'suggested_action': {
                'EASY_WIN_APPROVE': 'ISSUE_VISA',
                'BLOCKED': 'ESCALATE',
                'FAST_REJECT': 'ISSUE_VISA',
                'CONFLICT': 'ARBITRATE',
                'WAITING': 'CHASE_APPROVERS',
            }.get(category, 'HOLD'),
            'revision_count': safe_val(row.get('revision_count', 1)) or 1,
            'is_latest': True,
            'is_cross_lot': is_cross_lot,
            'cross_lot_sheets': cross_lot_list if isinstance(cross_lot_list, list) else None,
            'row_quality': 'OK',
            'duplicate_flag': 'UNIQUE',
            'zone': None,
            'n_doc': None,
            'type_format': None,
            'ancien': None,
            'n_bdx': None,
            'date_reception': None,
            'non_recu_papier': None,
            'observations': None,
            'visa_global': safe_val(row.get('visa_global')),
            'visa_global_raw': safe_val(row.get('visa_global')),
            'lifecycle_state': safe_val(ls),
            'previous_version_key': None,
            'approvers': appr_summary['approver_details'],
            'assigned_approvers': [d['display_name'] for d in appr_summary['approver_details']],
            'score_components': {
                'overdue_points': min(safe_val(row.get('days_overdue', 0)) or 0, 50),
                'deadline_points': 0,
                'completeness_points': 0,
                'revision_points': min((safe_val(row.get('revision_count', 1)) or 1) * 5, 20),
                'deadline_penalty': -10 if not row.get('has_deadline', False) else 0,
            },
            'ai_suggestion': None,
            # Extra NM7 fields
            'priority_category': safe_val(row.get('priority_category')),
            'blocker_type': safe_val(row.get('blocker_type')),
            'blocker_reason': safe_val(row.get('blocker_reason')),
            'confidence_score': safe_val(row.get('confidence_score')),
            'inference_flags': safe_val(row.get('inference_flags', [])),
        }
        docs.append(doc)

    return docs


def build_category_summary(nm7_output: pd.DataFrame) -> list:
    """Build category summary for dashboard adapter."""
    summaries = []

    # By lifecycle_state (mapped to category)
    cat_counts = {}
    for ls in nm7_output['lifecycle_state']:
        cat = LIFECYCLE_TO_CATEGORY.get(ls, 'NOT_STARTED')
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        summaries.append({'group_type': 'category', 'group_value': cat, 'count': count})

    # By lot
    lot_counts = nm7_output['lot'].value_counts()
    for lot, count in lot_counts.items():
        summaries.append({'group_type': 'lot', 'group_value': safe_val(lot), 'count': int(count)})

    # By lifecycle_state directly
    ls_counts = nm7_output['lifecycle_state'].value_counts()
    for ls, count in ls_counts.items():
        summaries.append({'group_type': 'lifecycle_state', 'group_value': safe_val(ls), 'count': int(count)})

    return summaries


def build_pipeline_report(pipeline_data: dict, ref_date: date) -> dict:
    """Build pipeline report for dashboard adapter."""
    nm7 = pipeline_data['nm7_output']
    pq = pipeline_data['priority_queue']
    ii = pipeline_data['intake_issues']

    cat_counts = {}
    for ls in nm7['lifecycle_state']:
        cat = LIFECYCLE_TO_CATEGORY.get(ls, 'NOT_STARTED')
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    overdue_count = int(nm7['is_overdue'].sum())

    return {
        'reference_date': ref_date.isoformat(),
        'input_rows': len(nm7),
        'pending_count': len(pq),
        'overdue_count': overdue_count,
        'excluded_count': len(nm7) - len(pq) - len(ii),
        'intake_issues_count': len(ii),
        'category_distribution': cat_counts,
        'priority_queue_size': len(pq),
    }


def build_import_log(pipeline_data: dict) -> list:
    """Build import log from pipeline logs."""
    logs = pipeline_data.get('logs', [])
    result = []
    for i, entry in enumerate(logs):
        result.append({
            'id': i + 1,
            'module': safe_val(entry.get('module')),
            'severity': safe_val(entry.get('severity')),
            'message': safe_val(entry.get('message')),
            'context': safe_val(entry.get('context')),
        })
    return result


def export_all(output_dir: str, data_dir: str = None):
    """Run the full pipeline and export all JSON files."""
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(__file__), 'data')

    ref_date = date(2026, 3, 23)
    pipeline_data = run_pipeline(data_dir, ref_date)

    # Ensure output directories exist
    for sub in ['m1', 'm2', 'm3']:
        os.makedirs(os.path.join(output_dir, sub), exist_ok=True)

    # 1. Priority queue (m3_priority_queue.json)
    print('\n[EXPORT] Building priority queue...')
    queue_items = build_queue_json(pipeline_data, ref_date)
    queue_path = os.path.join(output_dir, 'm3', 'm3_priority_queue.json')
    with open(queue_path, 'w', encoding='utf-8') as f:
        json.dump(queue_items, f, ensure_ascii=False, indent=2)
    print(f'  → {len(queue_items):,} items → {queue_path}')

    # 2. Category summary (m3_category_summary.json)
    print('[EXPORT] Building category summary...')
    cat_summary = build_category_summary(pipeline_data['nm7_output'])
    cat_path = os.path.join(output_dir, 'm3', 'm3_category_summary.json')
    with open(cat_path, 'w', encoding='utf-8') as f:
        json.dump(cat_summary, f, ensure_ascii=False, indent=2)
    print(f'  → {len(cat_summary)} entries → {cat_path}')

    # 3. Pipeline report (m3_pipeline_report.json)
    print('[EXPORT] Building pipeline report...')
    report = build_pipeline_report(pipeline_data, ref_date)
    report_path = os.path.join(output_dir, 'm3', 'm3_pipeline_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f'  → {report_path}')

    # 4. Enriched master dataset (enriched_master_dataset.json)
    print('[EXPORT] Building enriched dataset...')
    enriched = build_enriched_dataset_json(pipeline_data, ref_date)
    enriched_path = os.path.join(output_dir, 'm2', 'enriched_master_dataset.json')
    with open(enriched_path, 'w', encoding='utf-8') as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)
    print(f'  → {len(enriched):,} docs → {enriched_path}')

    # 5. Import log (import_log.json)
    print('[EXPORT] Building import log...')
    import_log = build_import_log(pipeline_data)
    log_path = os.path.join(output_dir, 'm1', 'import_log.json')
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(import_log, f, ensure_ascii=False, indent=2)
    print(f'  → {len(import_log)} entries → {log_path}')

    # 6. Master dataset stub (m1/master_dataset.json) — count only
    m1_path = os.path.join(output_dir, 'm1', 'master_dataset.json')
    with open(m1_path, 'w', encoding='utf-8') as f:
        json.dump({'total_rows': len(pipeline_data['active_dataset']),
                    'total_docs': len(pipeline_data['nm7_output']),
                    'source_file': pipeline_data['source_file']}, f, indent=2)
    print(f'  → {m1_path}')

    # Summary
    nm7 = pipeline_data['nm7_output']
    cat_dist = {}
    for ls in nm7['lifecycle_state']:
        cat = LIFECYCLE_TO_CATEGORY.get(ls, 'NOT_STARTED')
        cat_dist[cat] = cat_dist.get(cat, 0) + 1

    print(f'\n{"="*60}')
    print(f'  EXPORT COMPLETE')
    print(f'{"="*60}')
    print(f'  Source: {pipeline_data["source_file"]}')
    print(f'  Total NM7 docs: {len(nm7):,}')
    print(f'  Priority queue: {len(pipeline_data["priority_queue"]):,}')
    print(f'  Intake issues: {len(pipeline_data["intake_issues"]):,}')
    print(f'  Overdue: {int(nm7["is_overdue"].sum()):,}')
    print(f'  Categories: {cat_dist}')
    print(f'  Unique lots: {nm7["lot"].nunique()}')
    print(f'  Output dir: {output_dir}')
    print(f'{"="*60}')


if __name__ == '__main__':
    base = os.path.dirname(__file__)
    output_dir = os.path.join(base, 'output')
    export_all(output_dir)
