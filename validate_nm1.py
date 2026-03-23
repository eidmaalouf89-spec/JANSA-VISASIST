"""Milestone 1 Validation Script — run from project root in Cursor terminal.

Usage:
    cd JANSA VISASIST
    python validate_nm1.py
"""

import sys
sys.path.insert(0, '.')

import pandas as pd
from jansa.adapters.ged.nm1_loader import load_ged_export
from jansa.adapters.ged.logging import get_log_as_dataframe

FILEPATH = 'data/17&CO Tranche 2 du 23 mars 2026 07_45.xlsx'

print(f'Loading: {FILEPATH}')
print('-' * 60)

ged_long, import_log = load_ged_export(FILEPATH)

# --- Row consistency ---
print(f'Total rows:          {len(ged_long)}')
print(f'Unique doc_ids:      {ged_long["doc_id"].nunique()}')
print(f'Unique famille_keys: {ged_long["famille_key"].nunique()}')
print(f'Unique doc_ver_keys: {ged_long["doc_version_key"].nunique()}')
print()

# --- Assertions ---
checks = []

def check(name, condition, detail=''):
    status = 'PASS' if condition else 'FAIL'
    checks.append((name, status))
    print(f'  [{status}] {name}' + (f' — {detail}' if detail and not condition else ''))

check('Rows loaded', len(ged_long) > 0)
check('No null doc_ids', ged_long['doc_id'].isna().sum() == 0)
check('No empty famille_key',
      (ged_long['famille_key'].isna() | (ged_long['famille_key'] == '')).sum() == 0)
check('mission_type NOT in output', 'mission_type' not in ged_long.columns)
check('is_late NOT in output', 'is_late' not in ged_long.columns)
check('reponse_raw has values', ged_long['reponse_raw'].notna().sum() > 0)

log_df = get_log_as_dataframe()
errors = log_df[log_df['severity'] == 'ERROR'] if len(log_df) > 0 else pd.DataFrame()
check('No ERROR-level log events', len(errors) == 0,
      f'{len(errors)} errors found')

print()

# --- Distributions ---
print('--- Distributions ---')
print(f'row_quality:     {ged_long["row_quality"].value_counts().to_dict()}')
print(f'version_number:  {ged_long["version_number"].value_counts().to_dict()}')
print(f'indice:          {ged_long["indice"].value_counts().to_dict()}')
print(f'batiment:        {ged_long["batiment"].value_counts().to_dict()}')
print(f'reponse_raw:     {ged_long["reponse_raw"].value_counts().to_dict()}')
print(f'mission (top 5): {ged_long["mission"].value_counts().head().to_dict()}')
print()

# --- Log summary ---
print(f'--- Log: {len(log_df)} events ---')
if len(log_df) > 0:
    print(f'By severity: {log_df["severity"].value_counts().to_dict()}')
    print(f'By code:     {log_df["code"].value_counts().to_dict()}')
print()

# --- Output columns ---
print(f'--- Output columns ({len(ged_long.columns)}) ---')
print(list(ged_long.columns))
print()

# --- Final verdict ---
failed = [name for name, status in checks if status == 'FAIL']
if failed:
    print(f'MILESTONE 1: FAIL ({len(failed)} check(s) failed)')
    sys.exit(1)
else:
    print('MILESTONE 1: PASS')
