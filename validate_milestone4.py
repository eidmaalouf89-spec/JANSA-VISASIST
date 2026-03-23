"""Milestone 4 Validation: NM7 integration on real GED data.

Runs full pipeline: NM1 → NM3 → NM2 → NM4 → NM5 → NM7
Reports: lifecycle_state distribution, priority_category distribution,
priority queue size, intake_issues size, confidence_score distribution,
top anomaly flags, deterministic check result.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from datetime import date

from jansa.adapters.ged.nm1_loader import load_ged_export
from jansa.adapters.ged.nm3_vocab import normalize_responses
from jansa.adapters.ged.nm2_sas import interpret_sas
from jansa.adapters.ged.nm4_assignment import classify_assignments
from jansa.adapters.ged.nm5_revision import compute_active_dataset
from jansa.adapters.ged.legacy_loader import load_ancien_flags
from jansa.pipeline.nm7_lifecycle import run_nm7
from jansa.adapters.ged.logging import get_log, clear_log


def run_milestone4(filepath: str, grandfichier_path: str = None) -> str:
    """Run full NM1→NM7 pipeline and report results."""
    sep = '=' * 70
    ref_date = date(2026, 3, 23)

    # ── Legacy flags (ancien) from GrandFichier ───────────────────────
    ancien_set = set()
    if grandfichier_path and os.path.exists(grandfichier_path):
        ancien_set = load_ancien_flags(grandfichier_path)

    # ── NM1 ──────────────────────────────────────────────────────────
    ged_long, nm1_log = load_ged_export(filepath)
    nm1_rows = len(ged_long)
    nm1_docs = ged_long['doc_id'].nunique()

    # ── NM3 ──────────────────────────────────────────────────────────
    ged_long = normalize_responses(ged_long)

    # ── NM2 ──────────────────────────────────────────────────────────
    nm2_result = interpret_sas(ged_long)

    # ── NM4 ──────────────────────────────────────────────────────────
    ged_long, nm4_summary = classify_assignments(ged_long, circuit_matrix=None)

    # ── NM5 ──────────────────────────────────────────────────────────
    ged_enriched, doc_level, active_dataset = compute_active_dataset(
        ged_long, ancien_set=ancien_set if ancien_set else None,
    )

    # ── NM7 ──────────────────────────────────────────────────────────
    priority_queue, intake_issues, nm7_output = run_nm7(
        active_dataset, nm2_result, nm4_summary, doc_level,
        reference_date=ref_date,
    )

    # ── Determinism check ────────────────────────────────────────────
    clear_log()
    pq2, ii2, nm7_out2 = run_nm7(
        active_dataset, nm2_result, nm4_summary, doc_level,
        reference_date=ref_date,
    )
    deterministic = (
        list(priority_queue['doc_id']) == list(pq2['doc_id']) and
        list(intake_issues['doc_id']) == list(ii2['doc_id']) and
        list(priority_queue['priority_score']) == list(pq2['priority_score'])
    )

    # ══════════════════════════════════════════════════════════════════
    # REPORT
    # ══════════════════════════════════════════════════════════════════

    report_lines = []
    def p(s=''):
        report_lines.append(str(s))

    p(sep)
    p('  MILESTONE 4 VALIDATION REPORT')
    p(sep)

    # ── 1. Pipeline flow ─────────────────────────────────────────────
    p('\n--- 1. Pipeline Flow ---')
    p(f'NM1 output: {nm1_rows:,} rows, {nm1_docs:,} documents')
    legacy_count = int(doc_level['is_legacy'].sum()) if 'is_legacy' in doc_level.columns else 0
    p(f'Legacy (ancien=1) docs: {legacy_count:,}')
    p(f'NM5 active docs: {doc_level["is_active"].sum():,}')
    p(f'Active dataset rows: {len(active_dataset):,}')
    p(f'NM7 processed docs: {len(nm7_output):,}')

    # ── 2. Lifecycle state distribution ──────────────────────────────
    p('\n--- 2. Lifecycle State Distribution ---')
    ls_dist = nm7_output['lifecycle_state'].value_counts()
    for state, count in ls_dist.items():
        pct = count / len(nm7_output) * 100
        p(f'  {state}: {count:,} ({pct:.1f}%)')

    # ── 3. Priority category distribution ────────────────────────────
    p('\n--- 3. Priority Category Distribution ---')
    pc_dist = nm7_output['priority_category'].value_counts()
    for cat, count in pc_dist.items():
        pct = count / len(nm7_output) * 100
        p(f'  {cat}: {count:,} ({pct:.1f}%)')

    # ── 4. Queue sizes ───────────────────────────────────────────────
    p('\n--- 4. Queue Sizes ---')
    p(f'Priority queue:  {len(priority_queue):,} docs')
    p(f'Intake issues:   {len(intake_issues):,} docs')
    excluded_count = len(nm7_output) - len(priority_queue) - len(intake_issues)
    p(f'Excluded:        {excluded_count:,} docs')
    p(f'Total NM7 docs:  {len(nm7_output):,}')

    # ── 5. Confidence score distribution ─────────────────────────────
    p('\n--- 5. Confidence Score Distribution ---')
    conf = nm7_output['confidence_score']
    p(f'  Mean:   {conf.mean():.3f}')
    p(f'  Median: {conf.median():.3f}')
    p(f'  Min:    {conf.min():.3f}')
    p(f'  Max:    {conf.max():.3f}')
    # Bucket distribution
    buckets = pd.cut(conf, bins=[0, 0.2, 0.4, 0.6, 0.8, 1.01],
                     labels=['0.0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1.0'],
                     right=False)
    p(f'\n  Confidence buckets:')
    for bucket, count in buckets.value_counts().sort_index().items():
        pct = count / len(nm7_output) * 100
        p(f'    {bucket}: {count:,} ({pct:.1f}%)')

    # ── 6. Top inference flags / anomalies ───────────────────────────
    p('\n--- 6. Top Inference Flags ---')
    all_flags = []
    for flags in nm7_output['inference_flags']:
        if isinstance(flags, list):
            all_flags.extend(flags)
    if all_flags:
        flag_dist = pd.Series(all_flags).value_counts().head(15)
        for flag, count in flag_dist.items():
            p(f'  {flag}: {count:,}')
    else:
        p('  No inference flags.')

    # ── 7. Blocker type distribution ─────────────────────────────────
    p('\n--- 7. Blocker Type Distribution ---')
    bt_dist = nm7_output['blocker_type'].value_counts()
    for bt, count in bt_dist.items():
        pct = count / len(nm7_output) * 100
        p(f'  {bt}: {count:,} ({pct:.1f}%)')

    # ── 8. Consensus type distribution ───────────────────────────────
    p('\n--- 8. Consensus Type Distribution ---')
    ct_dist = nm7_output['consensus_type'].value_counts()
    for ct, count in ct_dist.items():
        pct = count / len(nm7_output) * 100
        p(f'  {ct}: {count:,} ({pct:.1f}%)')

    # ── 9. Priority queue top 10 ─────────────────────────────────────
    p('\n--- 9. Priority Queue — Top 10 ---')
    if len(priority_queue) > 0:
        top10 = priority_queue.head(10)
        for _, row in top10.iterrows():
            p(f'  {row["famille_key"]} | {row["lifecycle_state"]} | '
              f'score={row["priority_score"]:.0f} | {row["priority_category"]} | '
              f'blocker={row["blocker_type"]}')
    else:
        p('  Priority queue is empty.')

    # ── 10. Validation checks ────────────────────────────────────────
    p('\n--- 10. Validation Checks ---')

    # lifecycle_state non-null
    ls_null = nm7_output['lifecycle_state'].isna().sum()
    p(f'lifecycle_state nulls: {ls_null}')

    # priority_score range
    ps_range = nm7_output['priority_score'].between(0, 200).all()
    p(f'priority_score in [0,200]: {ps_range}')

    # queue_destination non-null
    qd_null = nm7_output['queue_destination'].isna().sum()
    p(f'queue_destination nulls: {qd_null}')

    # SYNTHESIS_ISSUED → EXCLUDED
    synth = nm7_output[nm7_output['lifecycle_state'] == 'SYNTHESIS_ISSUED']
    synth_excl = (synth['queue_destination'] == 'EXCLUDED').all() if len(synth) > 0 else True
    p(f'SYNTHESIS_ISSUED → EXCLUDED: {synth_excl}')

    # SAS_BLOCKED/PENDING → INTAKE_ISSUES
    sas = nm7_output[nm7_output['lifecycle_state'].isin({'SAS_BLOCKED', 'SAS_PENDING'})]
    sas_ii = (sas['queue_destination'] == 'INTAKE_ISSUES').all() if len(sas) > 0 else True
    p(f'SAS_BLOCKED/PENDING → INTAKE_ISSUES: {sas_ii}')

    # Determinism
    p(f'Deterministic output: {deterministic}')

    # Confidence floor
    conf_floor = (nm7_output['confidence_score'] >= 0.1).all()
    p(f'Confidence ≥ 0.1: {conf_floor}')

    all_pass = (
        ls_null == 0 and ps_range and qd_null == 0 and
        synth_excl and sas_ii and deterministic and conf_floor
    )
    p(f'\nALL CHECKS PASSED: {all_pass}')

    # ── 11. Log summary ──────────────────────────────────────────────
    p(f'\n--- 11. Pipeline Log Summary ---')
    logs = get_log()
    log_df = pd.DataFrame(logs)
    if len(log_df) > 0:
        for module in ['NM1', 'NM2', 'NM3', 'NM4', 'LEGACY', 'NM5', 'NM7']:
            mod_logs = log_df[log_df['module'] == module]
            if len(mod_logs) > 0:
                sev_dist = mod_logs['severity'].value_counts().to_dict()
                p(f'  {module}: {len(mod_logs):,} events — {sev_dist}')

    p(f'\n{sep}')
    p(f'  END OF MILESTONE 4 REPORT')
    p(f'{sep}')

    full_report = '\n'.join(report_lines)
    sys.stdout.write(full_report + '\n')
    return full_report


if __name__ == '__main__':
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    data_file = os.path.join(
        data_dir,
        '17&CO Tranche 2 du 23 mars 2026 07_45.xlsx',
    )
    if not os.path.exists(data_file):
        data_file = os.path.join(data_dir, 'GrandFichier_1.xlsx')

    # Auto-detect GrandFichier for ancien cross-reference
    gf_path = os.path.join(data_dir, 'GrandFichier_1.xlsx')
    if not os.path.exists(gf_path):
        gf_path = None

    run_milestone4(data_file, grandfichier_path=gf_path)
