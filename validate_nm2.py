"""Milestone 2 Validation Script — run from project root in Cursor terminal.

Usage:
    cd JANSA VISASIST
    python validate_nm2.py
"""

import sys
sys.path.insert(0, '.')

import pandas as pd
from jansa.adapters.ged.nm1_loader import load_ged_export
from jansa.adapters.ged.nm3_vocab import normalize_responses
from jansa.adapters.ged.nm2_sas import interpret_sas
from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe

FILEPATH = 'data/17&CO Tranche 2 du 23 mars 2026 07_45.xlsx'

print(f'Loading: {FILEPATH}')
print('-' * 60)

# NM1
ged_long, _ = load_ged_export(FILEPATH)
clear_log()

# NM3 first, NM2 second
ged_long_nm3 = normalize_responses(ged_long)
nm2_result = interpret_sas(ged_long_nm3)

checks = []

def check(name, condition, detail=''):
    status = 'PASS' if condition else 'FAIL'
    checks.append((name, status))
    print(f'  [{status}] {name}' + (f' — {detail}' if detail and not condition else ''))

# --- NM3 checks ---
print('=== NM3 Checks ===')
check('mission_type in output', 'mission_type' in ged_long_nm3.columns)
check('response_status in output', 'response_status' in ged_long_nm3.columns)
check('reponse_normalized in output', 'reponse_normalized' in ged_long_nm3.columns)
check('mission_type 100% non-null', ged_long_nm3['mission_type'].isna().sum() == 0)
mt_vals = set(ged_long_nm3['mission_type'].unique())
check('mission_type valid enum', mt_vals <= {'SAS', 'MOEX', 'REVIEWER', 'UNKNOWN'}, str(mt_vals))
check('response_status 100% non-null', ged_long_nm3['response_status'].isna().sum() == 0)
ambig = (ged_long_nm3['response_status'] == 'RESPONDED_AMBIGUOUS').sum()
check('RESPONDED_AMBIGUOUS = 0', ambig == 0, f'{ambig} ambiguous')
check('reponse_raw preserved',
      ged_long_nm3['reponse_raw'].notna().sum() >= ged_long['reponse_raw'].notna().sum())

# --- NM2 checks ---
print('\n=== NM2 Checks ===')
check('sas_state 100% non-null', nm2_result['sas_state'].notna().all())
check('sas_confidence 100% non-null', nm2_result['sas_confidence'].notna().all())
check('One row per doc_id', nm2_result['doc_id'].nunique() == len(nm2_result))
check('Doc count matches NM1', len(nm2_result) == ged_long['doc_id'].nunique())

assumed = nm2_result[nm2_result['inference_flags'].apply(lambda f: 'SAS_ASSUMED_PASSED' in f)]
pct = len(assumed) / len(nm2_result) * 100
check('>90% SAS_ASSUMED_PASSED', pct > 90, f'{pct:.1f}%')

log_df = get_log_as_dataframe()
errors = log_df[log_df['severity'] == 'ERROR'] if len(log_df) > 0 else pd.DataFrame()
check('No ERROR log events', len(errors) == 0, f'{len(errors)} errors')

# --- Distributions ---
print('\n--- NM3 Distributions ---')
print(f'mission_type:       {ged_long_nm3["mission_type"].value_counts().to_dict()}')
print(f'response_status:    {ged_long_nm3["response_status"].value_counts().to_dict()}')
print(f'reponse_normalized: {ged_long_nm3["reponse_normalized"].value_counts(dropna=False).to_dict()}')

print('\n--- NM2 Distributions ---')
print(f'sas_state:      {nm2_result["sas_state"].value_counts().to_dict()}')
print(f'sas_confidence: {nm2_result["sas_confidence"].value_counts().to_dict()}')
print(f'SAS_ASSUMED:    {len(assumed)}/{len(nm2_result)} ({pct:.1f}%)')

print(f'\n--- Log: {len(log_df)} events ---')
if len(log_df) > 0:
    print(f'By code: {log_df["code"].value_counts().to_dict()}')

# --- Verdict ---
print()
failed = [name for name, status in checks if status == 'FAIL']
if failed:
    print(f'MILESTONE 2: FAIL ({len(failed)} check(s) failed)')
    sys.exit(1)
else:
    print('MILESTONE 2: PASS')
