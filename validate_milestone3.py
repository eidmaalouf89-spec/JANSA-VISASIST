"""Milestone 3 Validation: NM4 + NM5 integration on real GED data.

Runs full pipeline: NM1 → NM3 → NM2 → NM4 → NM5
Reports distributions, anomalies, and active dataset metrics.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from jansa.adapters.ged.nm1_loader import load_ged_export
from jansa.adapters.ged.nm3_vocab import normalize_responses
from jansa.adapters.ged.nm2_sas import interpret_sas
from jansa.adapters.ged.nm4_assignment import classify_assignments
from jansa.adapters.ged.nm5_revision import compute_active_dataset
from jansa.adapters.ged.logging import get_log


def run_milestone3(filepath: str) -> None:
    """Run full NM1→NM3→NM2→NM4→NM5 pipeline and report results."""
    sep = '=' * 70

    # ── NM1 ──────────────────────────────────────────────────────────
    ged_long, nm1_log = load_ged_export(filepath)
    nm1_rows = len(ged_long)
    nm1_docs = ged_long['doc_id'].nunique()

    # ── NM3 ──────────────────────────────────────────────────────────
    ged_long = normalize_responses(ged_long)

    # ── NM2 ──────────────────────────────────────────────────────────
    nm2_sas = interpret_sas(ged_long)

    # ── NM4 ──────────────────────────────────────────────────────────
    ged_long, nm4_summary = classify_assignments(ged_long, circuit_matrix=None)

    # ── NM5 ──────────────────────────────────────────────────────────
    ged_enriched, doc_level, active_dataset = compute_active_dataset(ged_long)

    # ══════════════════════════════════════════════════════════════════
    # REPORT
    # ══════════════════════════════════════════════════════════════════

    report_lines = []
    def p(s=''):
        report_lines.append(str(s))

    p(sep)
    p('  MILESTONE 3 VALIDATION REPORT')
    p(sep)

    # ── 1. Pipeline flow ─────────────────────────────────────────────
    p('\n--- 1. Pipeline Flow ---')
    p(f'NM1 output: {nm1_rows:,} rows, {nm1_docs:,} documents')
    p(f'NM3 output: mission_type dist:')
    mt_dist = ged_long['mission_type'].value_counts()
    for k, v in mt_dist.items():
        p(f'  {k}: {v:,}')
    p(f'NM2 output: {len(nm2_sas):,} docs with SAS state')
    sas_dist = nm2_sas['sas_state'].value_counts()
    for k, v in sas_dist.items():
        p(f'  {k}: {v:,}')

    # ── 2. NM4 Assignment Distribution ────────────────────────────────
    p(f'\n--- 2. NM4 Assignment Classification ---')
    p(f'Total rows after NM4: {len(ged_long):,}')
    at_dist = ged_long['assignment_type'].value_counts()
    p(f'\nassignment_type distribution:')
    for k, v in at_dist.items():
        pct = v / len(ged_long) * 100
        p(f'  {k}: {v:,} ({pct:.1f}%)')

    as_dist = ged_long['assignment_source'].value_counts()
    p(f'\nassignment_source distribution:')
    for k, v in as_dist.items():
        p(f'  {k}: {v:,}')

    frs_dist = ged_long['final_response_status'].value_counts()
    p(f'\nfinal_response_status distribution:')
    for k, v in frs_dist.items():
        pct = v / len(ged_long) * 100
        p(f'  {k}: {v:,} ({pct:.1f}%)')

    # ── 3. NM4 Document-level summary ─────────────────────────────────
    p(f'\n--- 3. NM4 Document-Level Summary ---')
    p(f'Total doc summaries: {len(nm4_summary):,}')

    # Reviewer counts
    nm4_summary['n_assigned'] = nm4_summary['assigned_reviewers'].apply(len)
    nm4_summary['n_relevant'] = nm4_summary['relevant_reviewers'].apply(len)
    nm4_summary['n_hm'] = nm4_summary['hm_reviewers'].apply(len)

    p(f'Docs with assigned_reviewers > 0: {(nm4_summary["n_assigned"] > 0).sum():,}')
    p(f'Docs with relevant_reviewers == 0: {(nm4_summary["n_relevant"] == 0).sum():,}')
    p(f'Docs with HM reviewers > 0: {(nm4_summary["n_hm"] > 0).sum():,}')
    p(f'Docs with blocking_reviewers > 0: {(nm4_summary["blocking_reviewers"].apply(len) > 0).sum():,}')
    p(f'Docs with missing_reviewers > 0: {(nm4_summary["missing_reviewers"].apply(len) > 0).sum():,}')

    # HM exclusion validation
    p(f'\n  HM Exclusion Check:')
    total_hm = nm4_summary['n_hm'].sum()
    p(f'  Total HM reviewer assignments: {total_hm:,}')
    all_hm_docs = (nm4_summary['n_assigned'] > 0) & (nm4_summary['n_relevant'] == 0)
    p(f'  Docs where ALL reviewers are HM: {all_hm_docs.sum():,}')

    # UNKNOWN_REQUIRED check
    p(f'\n  UNKNOWN_REQUIRED Check:')
    has_unknown_flag = nm4_summary['inference_flags'].apply(lambda x: 'UNKNOWN_ASSIGNMENT' in x)
    p(f'  Docs with UNKNOWN_ASSIGNMENT flag: {has_unknown_flag.sum():,}')

    # ── 4. NM5 Revision & Active Dataset ──────────────────────────────
    p(f'\n--- 4. NM5 Revision Handling & Active Dataset ---')
    p(f'Total documents (doc_level): {len(doc_level):,}')
    p(f'Active documents: {doc_level["is_active"].sum():,}')
    p(f'Inactive documents: {(~doc_level["is_active"]).sum():,}')
    p(f'Active dataset rows: {len(active_dataset):,}')

    p(f'\nActive vs NM1:')
    active_docs_count = doc_level['is_active'].sum()
    p(f'  NM1 docs:    {nm1_docs:,}')
    p(f'  Active docs: {active_docs_count:,}')
    p(f'  Reduction:   {nm1_docs - active_docs_count:,} ({(nm1_docs - active_docs_count) / nm1_docs * 100:.1f}%)')

    p(f'\nRevision count distribution:')
    rc_dist = doc_level['revision_count'].value_counts().sort_index()
    for k, v in rc_dist.items():
        p(f'  revision_count={k}: {v:,} docs')

    p(f'\nCross-lot detection:')
    p(f'  Cross-lot families: {doc_level["is_cross_lot"].sum():,}')

    p(f'\nRevision gap detection:')
    p(f'  Docs with revision gap: {doc_level["has_revision_gap"].sum():,}')

    # Anomaly flags
    all_anomalies = []
    for flags in doc_level['anomaly_flags']:
        if isinstance(flags, list):
            all_anomalies.extend(flags)
    if all_anomalies:
        anomaly_dist = pd.Series(all_anomalies).value_counts()
        p(f'\nAnomaly flags:')
        for k, v in anomaly_dist.items():
            p(f'  {k}: {v:,}')
    else:
        p(f'\nNo anomaly flags detected.')

    # ── 5. Uniqueness validation ──────────────────────────────────────
    p(f'\n--- 5. Validation Checks ---')
    active_docs = doc_level[doc_level['is_active']]
    uniqueness = active_docs.groupby(['famille_key', 'lot', 'batiment']).size()
    violations = uniqueness[uniqueness > 1]
    p(f'Active uniqueness (famille_key, lot, batiment):')
    p(f'  Total groups: {len(uniqueness):,}')
    p(f'  Violations (>1 active per group): {len(violations):,}')
    if len(violations) > 0:
        p(f'  VIOLATION DETAILS (first 10):')
        for (fk, lot, bat), cnt in violations.head(10).items():
            p(f'    ({fk}, {lot}, {bat}): {cnt} active docs')

    # assignment_type non-null check
    at_null = ged_long['assignment_type'].isna().sum()
    p(f'\nassignment_type nulls: {at_null}')
    frs_null = ged_long['final_response_status'].isna().sum()
    p(f'final_response_status nulls: {frs_null}')
    ia_null = doc_level['is_active'].isna().sum()
    p(f'is_active nulls: {ia_null}')
    rc_lt1 = (doc_level['revision_count'] < 1).sum()
    p(f'revision_count < 1: {rc_lt1}')

    all_pass = (at_null == 0 and frs_null == 0 and ia_null == 0 and
                rc_lt1 == 0 and len(violations) == 0)
    p(f'\nALL CHECKS PASSED: {all_pass}')

    # ── 6. Log summary ────────────────────────────────────────────────
    p(f'\n--- 6. Pipeline Log Summary ---')
    logs = get_log()
    log_df = pd.DataFrame(logs)
    if len(log_df) > 0:
        for module in ['NM1', 'NM2', 'NM3', 'NM4', 'NM5']:
            mod_logs = log_df[log_df['module'] == module]
            if len(mod_logs) > 0:
                sev_dist = mod_logs['severity'].value_counts().to_dict()
                p(f'  {module}: {len(mod_logs):,} events — {sev_dist}')

    p(f'\n{sep}')
    p(f'  END OF MILESTONE 3 REPORT')
    p(f'{sep}')

    # Print report
    full_report = '\n'.join(report_lines)
    sys.stdout.write(full_report + '\n')

    return full_report


if __name__ == '__main__':
    data_file = os.path.join(
        os.path.dirname(__file__),
        'data',
        '17&CO Tranche 2 du 23 mars 2026 07_45.xlsx',
    )
    if not os.path.exists(data_file):
        data_file = os.path.join(
            os.path.dirname(__file__),
            'data',
            'GrandFichier_1.xlsx',
        )
    run_milestone3(data_file)
