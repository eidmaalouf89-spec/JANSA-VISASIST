# JANSA VISASIST — GED Cowork Implementation Plan

**Version:** 1.2  
**Date:** March 2026  
**For:** Claude Cowork — implementation worker  
**Authority:** **GED_PHASE_B_SPECS_V1_2.md** + GED_DIAGNOSTIC_V1_1.md

> **SPEC AUTHORITY RULE [P1]:** This implementation plan must strictly follow `GED_PHASE_B_SPECS_V1_2.md`. If any conflict exists between this plan and the spec, the spec takes precedence. If ambiguity is detected, **STOP and report instead of guessing.**

> **EXECUTION ORDER RULE [P2]:** The fixed pipeline order is `NM1 → NM3 → NM2 → NM4 → NM5 → NM7`. This order is non-negotiable. **NM3 MUST run before NM2.** NM2 has no inline vocabulary — it depends entirely on NM3 output. Any deviation from this order is a bug.

> **NO ROW LOOPS RULE [P6]:** Use of `iterrows()`, `itertuples()`, or row-level `for` loops is strictly forbidden in all modules. All transformations must use vectorized pandas operations or `groupby/transform`. Violations are rejected at code review.

> **LOGGING RULE [P9]:** All modules must use the centralized `log_event()` interface defined in the spec (`jansa/adapters/ged/logging.py`). No local `logging.getLogger()`, no `print()`, no ad-hoc log lists.

---

## V1.0 → V1.1 Patch Summary

| Patch | Section | Change |
|---|---|---|
| P1 | M1 brief, M2 brief, M2 tests | `mission_type` removed from NM1 → moved to NM3 brief. `is_late` removed from NM1 → moved to NM7. |
| P2 | M2 NM2 brief | NM2 now consumes NM3 output. Inline SAS mapping removed. Execution order enforced: NM3 before NM2. |
| P3 | M2 NM4 brief + tests | HM exclusion made explicit in NM4 brief and test list. |
| P4 | M2 NM5 brief + tests + validation | Uniqueness key updated to `(famille_key, lot, batiment)`. |
| P5 | M3 tests + NM7 brief | `test_all_approve_no_moex_row()` added. `MISSING_MOEX_ASSIGNMENT` added to constants. |
| P6 | M3 tests + NM7 brief | `test_multiple_moex_verdicts_flag_propagated()` added. Flag propagation made explicit. |
| P7 | What Cowork Must NOT Do | `iterrows()` and row-level `for` loops explicitly forbidden. Lint check added to CI. |
| P8 | M1 brief, What Cowork Must NOT Do | `log_event()` centralized logging enforced. `logging.getLogger()` forbidden. |
| P9 | M1 tests, M3 tests | `test_mission_type_classification` and `test_is_late_computation` moved from M1 → M2 (NM3) and M3 (NM7) respectively. |
| P10 | M2 integration test | New mandatory integration test `test_full_pipeline_sample()` added before Milestone 3 validation. |

**V1.1 → V1.2 patches:**

| Patch | Change |
|---|---|
| P1 | Spec authority rule added at top: spec wins on conflict; stop on ambiguity. |
| P2 | Execution order enforced globally. NM3 before NM2 is non-negotiable. |
| P3 | Milestones restructured: M1=NM1, M2=NM3+NM2, M3=NM4+NM5, M4=NM7. |
| P4 | Explicit STOP rule added after every milestone. |
| P5 | Contract validation step added to each module brief. |
| P6 | Row loop prohibition added as global rule. |
| P7 | NM1→NM5 integration checkpoint added at end of Milestone 3. |
| P8 | Full NM1→NM7 determinism test added at end of Milestone 4. |
| P9 | Centralized logging rule added globally. |
| P10 | Validation metrics simplified to distributions + consistency (no brittle exact counts). |

---

## How to Use This Document

This plan is structured as four discrete milestones. Each milestone has a single goal, a set of files to create, implementation briefs, and a mandatory STOP with validation.

**Do not proceed to the next milestone until the current milestone passes all validation checks.**

**Do not implement more than what is specified per milestone.**

**If any spec instruction conflicts with this plan, follow the spec.**

---

## Architecture Overview

```
GED Export (.xlsx)
       │
   [NM1-GED]  ← Milestone 1 (ingestion only)
       │ ged_long
       │
   [NM3-GED]  ← Milestone 2 STEP 1 (vocab + mission_type — MUST run before NM2)
       │ + reponse_normalized, response_status, mission_type
       │
   [NM2-GED]  ← Milestone 2 STEP 2 (SAS — depends on NM3)
       │ + sas_state
       │
   [NM4-GED]  ← Milestone 3 STEP 1 (assignment — depends on NM3)
   [NM5-GED]  ← Milestone 3 STEP 2 (revisions + active dataset)
       │ active_dataset
       │
   [NM7]      ← Milestone 4 (lifecycle + scoring + is_late)
       │
   priority_queue + intake_issues
```

**Fixed execution order: `NM1 → NM3 → NM2 → NM4 → NM5 → NM7`**  
This order is non-negotiable. NM3 runs before NM2 — no exceptions.

---

## Project File Structure

```
jansa/
  adapters/
    ged/
      nm1_loader.py          ← NM1-GED
      nm2_sas.py             ← NM2-GED
      nm3_vocab.py           ← NM3-GED (runs first in M2 — produces mission_type)
      nm4_assignment.py      ← NM4-GED
      nm5_revisions.py       ← NM5-GED
      constants.py           ← All shared constants (vocab maps, weights)
      exceptions.py          ← NM1InputError, NM1OutputError
      logging.py             ← Centralized log_event() interface (GP-LOG)
  pipeline/
    nm7_lifecycle.py         ← NM7
  tests/
    ged/
      test_nm1_loader.py
      test_nm2_sas.py
      test_nm3_vocab.py
      test_nm4_assignment.py
      test_nm5_revisions.py
      test_nm7_lifecycle.py
      test_integration.py    ← Full pipeline integration test (new M2 deliverable)
      fixtures/
        sample_ged_rows.py   ← Inline test fixtures (no file I/O in tests)
  data/                      ← NOT committed to git
    17_CO_Tranche_2_*.xlsx   ← Real GED export (for integration tests only)
```

---

## Milestone 1 — NM1: GED Ingestion

### Goal

Ingest the GED export file. Produce a canonical long-format normalized dataset. Zero workflow reconstruction. Zero VISA state interpretation.

At the end of Milestone 1: trusted `ged_long` DataFrame with structural fields only. Nothing more.

**Scope:** `nm1_loader.py` only. Do not implement NM3, NM2, NM4, NM5, or NM7.

> **STOP RULE [P4]:** After completing Milestone 1 validation, STOP. Do NOT proceed to Milestone 2. Report results and wait for explicit validation approval.

### Files to Create

| File | Purpose |
|---|---|
| `jansa/adapters/ged/exceptions.py` | Pipeline exceptions |
| `jansa/adapters/ged/constants.py` | Shared constants (column names, FAMILY_KEY_COLS) |
| `jansa/adapters/ged/logging.py` | **[V1.1 — P8]** Centralized `log_event()` interface |
| `jansa/adapters/ged/nm1_loader.py` | NM1 main module |
| `jansa/tests/ged/fixtures/sample_ged_rows.py` | Inline test data |
| `jansa/tests/ged/test_nm1_loader.py` | NM1 unit tests |

### Files to Modify

None — this is a greenfield implementation.

---

### Implementation Brief: `exceptions.py`

```python
class NM1InputError(Exception):
    """Raised when required input column is missing or sheet not found."""
    pass

class NM1OutputError(Exception):
    """Raised when NM1 produces zero output rows."""
    pass
```

---

### Implementation Brief: `logging.py` [V1.1 — P8]

All modules must use this interface. No local `logging.getLogger()` calls or `print()` statements anywhere in the pipeline.

```python
from datetime import datetime
from typing import Optional

_event_log: list = []

def log_event(
    doc_id: Optional[int],
    module: str,
    severity: str,      # 'ERROR' | 'WARNING' | 'INFO'
    code: str,          # machine-readable (e.g. 'FFILL_IDENTITY_MISMATCH')
    message: str,       # human-readable description
    raw_value: Optional[str] = None,
    field: Optional[str] = None,
) -> None:
    """
    Emit a pipeline event to the centralized log.
    All modules (NM1–NM7) must use this function exclusively.
    Never use print(), logging.getLogger(), or local log lists.
    """
    _event_log.append({
        'timestamp': datetime.utcnow().isoformat(),
        'doc_id': doc_id,
        'module': module,
        'severity': severity,
        'code': code,
        'message': message,
        'raw_value': raw_value,
        'field': field,
    })

def get_log() -> list:
    """Return all accumulated log events."""
    return list(_event_log)

def clear_log() -> None:
    """Clear log — call at start of each pipeline run."""
    _event_log.clear()

def get_log_as_dataframe():
    """Return log as pandas DataFrame for export."""
    import pandas as pd
    return pd.DataFrame(_event_log)
```

---

### Implementation Brief: `constants.py`

```python
# Source sheet name (primary)
GED_PRIMARY_SHEET = 'Vue détaillée des documents 1'
GED_FALLBACK_SHEET = 'Vue détaillée des documents'

# Column name in source
IDENTITY_COLS = [
    'Chemin', 'AFFAIRE', 'PROJET', 'BATIMENT', 'PHASE', 'EMETTEUR',
    'SPECIALITE', 'LOT', 'TYPE DE DOC', 'ZONE', 'NIVEAU', 'NUMERO',
    'INDICE', 'Libellé du fichier', 'Description', 'Format',
    'Version créée par', 'Date prévisionnelle', 'Date de dépôt effectif',
    'Écart avec la date de dépôt prévue', 'Version', 'Dernière modification',
    'Taille (Mo)', 'Statut final du document'
]

FAMILY_KEY_COLS = [
    'AFFAIRE', 'PROJET', 'BATIMENT', 'PHASE', 'EMETTEUR',
    'SPECIALITE', 'LOT', 'TYPE DE DOC', 'ZONE', 'NIVEAU', 'NUMERO'
]

REQUIRED_COLUMNS = IDENTITY_COLS + [
    'Mission', 'Répondant', 'Date limite pour répondre',
    'Réponse donnée le', 'Écart avec la date de réponse prévue',
    'Réponse', 'Commentaire', 'Pièces jointes', 'Type de réponse',
    'Mission associée'
]
```

---

### Implementation Brief: `nm1_loader.py`

Implement `load_ged_export(filepath: str) -> tuple[pd.DataFrame, pd.DataFrame]`

Returns: `(ged_long, import_log)`

**Contract validation [P5]:** Verify all `REQUIRED_COLUMNS` are present before processing. If any column is missing, raise `NM1InputError` immediately.

Implement each step from the NM1-GED spec (§5 Processing Logic) in exact order:

1. Load sheet with `header=1, dtype=str`
2. Add `row_index`
3. Forward-fill `doc_id` from `Identifiant`
4. Forward-fill all `IDENTITY_COLS`
5. Parse types (version_number, dates, ecart fields)
6. Compute `famille_key` from `FAMILY_KEY_COLS`
7. Compute `indice_sort_order` via `indice_to_sort()`
8. Compute `doc_version_key`
9. **[V1.1 — P1] Do NOT classify `mission_type` here.** Pass `mission` through raw.
10. Parse reviewer date fields (`deadline`, `date_reponse`, `ecart_reponse`). **Do NOT compute `is_late`** — that is NM7.
11. Preserve `reponse_raw` (copy of `Réponse`)
12. Initialize `row_quality = 'OK'`, `row_quality_details = []`
13. Run anomaly detection — emit all events via `log_event()` (GP-LOG)
14. Run post-load validation checks
15. Return `(ged_long, get_log_as_dataframe())`

**Do not implement:**
- `mission_type` classification → NM3
- `is_late` flag → NM7
- Response normalization (`reponse_normalized`) → NM3
- SAS state → NM2
- Assignment type → NM4
- `is_active` → NM5

**[V1.1 — P8]** Import and use `log_event` from `jansa.adapters.ged.logging`. Never use `print()` or `logging.getLogger()`.

---

### Implementation Brief: `fixtures/sample_ged_rows.py`

Create Python dictionaries (not CSV files) representing rows from the GED export. Include:

```python
SAMPLE_ROWS = [
    # Document 60003453 — 3 reviewer rows + separator
    {
        'Identifiant': '60003453', 'AFFAIRE': 'P17', 'PROJET': 'T2',
        'BATIMENT': 'GE', 'PHASE': 'EXE', 'EMETTEUR': 'LGD',
        'SPECIALITE': 'GOE', 'LOT': 'I003', 'TYPE DE DOC': 'NDC',
        'ZONE': 'TZ', 'NIVEAU': 'TX', 'NUMERO': '028000', 'INDICE': 'A',
        'Libellé du fichier': 'Hypothèses générales.pdf',
        'Version': '0.2', 'Réponse donnée le': '13/12/2023',
        'Mission': '0-BET Structure', 'Répondant': 'Olga DELAURISTON',
        'Date limite pour répondre': '26/12/2023',
        'Écart avec la date de réponse prévue': '-13',
        'Réponse': 'Validé avec observation',
        'Commentaire': 'Voir la note annotée',
    },
    {
        'Identifiant': None, 'Mission': '0-Bureau de Contrôle',
        'Répondant': 'Marion MOTTIER', 'Réponse': 'Défavorable',
        'Date limite pour répondre': '26/12/2023',
        'Réponse donnée le': '16/02/2024',
        'Écart avec la date de réponse prévue': '51',
    },
    {
        'Identifiant': None, 'Mission': '0-AMO HQE',
        'Répondant': 'Jean-François LABORDE', 'Réponse': 'Hors Mission',
    },
    {
        'Identifiant': None, 'Mission': None, 'Réponse': None,  # separator row
    },
    # Document 60003454 — new group
    {
        'Identifiant': '60003454', 'AFFAIRE': 'P17', 'PROJET': 'T2',
        'BATIMENT': 'GE', 'LOT': 'I003', 'NUMERO': '028001', 'INDICE': 'A',
        'Version': '0.2', 'Mission': '0-BET Structure',
        'Réponse': 'En attente',
    },
    # Document 60009000 — with SAS mission
    {
        'Identifiant': '60009000', 'AFFAIRE': 'P17', 'LOT': 'B041',
        'NUMERO': '049219', 'INDICE': 'A', 'Version': '0.1',
        'Mission': '0-SAS', 'Répondant': 'Patrice BRIN',
        'Réponse': 'Refusé',
    },
    # Document 60009001 — MOEX mission
    {
        'Identifiant': '60009001', 'AFFAIRE': 'P17', 'BATIMENT': 'BX',
        'LOT': 'B041', 'NUMERO': '049220', 'INDICE': 'B', 'Version': '0.1',
        'Mission': "B-Maître d'Oeuvre EXE", 'Répondant': 'Patrice BRIN',
        'Réponse': 'Validé avec observation',
    },
]
```

---

### Implementation Brief: `test_nm1_loader.py`

Write tests for each of the following. Tests must use fixtures only — no reading from disk:

```python
def test_forward_fill_doc_id()
  # Verify doc_id is filled for all rows including separator rows

def test_forward_fill_integrity()
  # Verify AFFAIRE, LOT, NUMERO consistent within each doc_id group

def test_famille_key_construction()
  # Verify famille_key excludes INDICE
  # Two rows with same family but different INDICE → same famille_key

def test_doc_version_key_uniqueness()
  # Verify unique key per (famille_key, INDICE, version_number)

def test_indice_sort_order()
  # A=1, B=2, Z=26, AA=27, AB=28

# [V1.1 — P9] mission_type test REMOVED from NM1 — moved to test_nm3_vocab.py
# [V1.1 — P9] is_late test REMOVED from NM1 — moved to test_nm7_lifecycle.py

def test_reponse_raw_preserved()
  # reponse_raw always equals source Réponse
  # Never modified, never normalized

def test_mission_column_passed_through_raw()
  # [V1.1 — P1] mission column preserved as-is, no mission_type column in output
  # assert 'mission_type' not in ged_long.columns

def test_ecart_reponse_preserved_no_is_late()
  # [V1.1 — P1] ecart_reponse present, is_late NOT present in output
  # assert 'is_late' not in ged_long.columns

def test_separator_row_handling()
  # Rows with null Mission and null Réponse are kept with row_quality=WARNING

def test_date_parsing()
  # Valid dates parsed to datetime
  # Invalid dates → null + WARNING in log

def test_log_event_used_for_anomalies()
  # [V1.1 — P8] All anomaly events appear in log returned by get_log_as_dataframe()
  # No direct assertions on local variables — check the centralized log

def test_missing_sheet_raises_error()
  # NM1InputError raised when primary sheet not found

def test_zero_rows_raises_error()
  # NM1OutputError raised when output is empty

def test_no_iterrows_in_nm1()
  # [V1.1 — P7] Lint check: nm1_loader.py source must not contain 'iterrows'
  # import inspect, jansa.adapters.ged.nm1_loader as m
  # assert 'iterrows' not in inspect.getsource(m)
```

---

### Milestone 1 Validation Checkpoint

Before proceeding to Milestone 2, run the following manually against the real GED file:

```python
ged_long, import_log = load_ged_export('data/17_CO_Tranche_2_*.xlsx')

# Row consistency
print(f"Total rows: {len(ged_long)}")             # Expected: ~15,000–16,000
print(f"Unique doc_ids: {ged_long['doc_id'].nunique()}")  # Expected: ~2,700–2,800
assert len(ged_long) > 0, "No rows loaded"

# Forward-fill integrity
assert ged_long['doc_id'].isna().sum() == 0, "Null doc_ids after ffill"
assert (ged_long['famille_key'].isna() | (ged_long['famille_key'] == '')).sum() == 0

# Scope check — NM1 must not produce these
assert 'mission_type' not in ged_long.columns
assert 'is_late' not in ged_long.columns

# Raw preservation
assert ged_long['reponse_raw'].notna().sum() > 0

# No unexplained errors
from jansa.adapters.ged.logging import get_log_as_dataframe
log_df = get_log_as_dataframe()
errors = log_df[log_df['severity'] == 'ERROR'] if len(log_df) > 0 else pd.DataFrame()
assert len(errors) == 0, f"Errors: {errors.to_dict('records')}"

print("Milestone 1: PASS")
```

**Milestone 1 done when:** Row counts consistent with source, no unexplained errors, scope checks pass.

> **STOP. Report results. Wait for validation before starting Milestone 2.**

---

## Milestone 2 — NM3 + NM2: Response Normalization & SAS

### Goal

Starting from the trusted `ged_long` from Milestone 1, add vocabulary normalization (`mission_type`, `reponse_normalized`, `response_status`) via NM3, then interpret SAS state via NM2.

**NM3 runs first. NM2 runs second. This order is mandatory.**

At the end of Milestone 2: `ged_long` enriched with NM3 fields, plus `nm2_result` with SAS state per document.

**Scope:** `nm3_vocab.py` and `nm2_sas.py` only. Do not implement NM4, NM5, or NM7.

> **STOP RULE [P4]:** After completing Milestone 2 validation, STOP. Do NOT proceed to Milestone 3. Report results and wait for explicit validation approval.

### Files to Create

| File | Purpose |
|---|---|
| `jansa/adapters/ged/nm3_vocab.py` | NM3 — response normalization + mission_type |
| `jansa/adapters/ged/nm2_sas.py` | NM2 — SAS interpretation (depends on NM3) |
| `jansa/tests/ged/test_nm3_vocab.py` | NM3 unit tests |
| `jansa/tests/ged/test_nm2_sas.py` | NM2 unit tests |

### Files to Modify

| File | Change |
|---|---|
| `jansa/adapters/ged/constants.py` | Add `VOCAB_MAP`, `MISSION_TYPE_PATTERNS` |

---

### Implementation Brief: `nm3_vocab.py` [Run FIRST in Milestone 2]

**[V1.1 — P1+P2] NM3 runs before NM2 and NM4. It produces `mission_type`, `reponse_normalized`, and `response_status` that both depend on.**

Implement `normalize_responses(ged_long: pd.DataFrame) -> pd.DataFrame`

**Contract validation [P5]:** Verify `doc_id`, `mission`, `reponse_raw` are present before processing. Raise `ContractError` if missing.

Returns: `ged_long` with three new columns: **`mission_type`**, `reponse_normalized`, `response_status`

Logic (from NM3-GED spec §5):
1. **Step 0 first:** Classify `mission_type` from `mission` using `classify_mission()` — vectorized `.map()`, no loops
2. Apply `VOCAB_MAP` prefix matching to `reponse_raw` for `reponse_normalized` + `response_status`
3. `Validé sans observation - SAS` must be checked before `Validé sans observation`
4. Preserve `reponse_raw` unchanged (assert this in tests)
5. Unknown vocabulary → `RESPONDED_AMBIGUOUS` + `log_event()` WARNING
6. Null `reponse_raw` → `NOT_RESPONDED` + `reponse_normalized = null`

**Add to constants.py:**
```python
MISSION_TYPE_PATTERNS = {
    'SAS': ['SAS'],
    'MOEX': ["Maître d'Oeuvre EXE", "Maitre d'Oeuvre EXE"],
    # anything else → REVIEWER; null/empty → UNKNOWN
}

VOCAB_MAP = [
    ('Validé sans observation - SAS', 'VSO', 'RESPONDED_APPROVE'),
    ('Validé sans observation',       'VSO', 'RESPONDED_APPROVE'),
    ('Validé avec observation',       'VAO', 'RESPONDED_APPROVE'),
    ('Favorable',                     'FAV', 'RESPONDED_APPROVE'),
    ('Refusé',                        'REF', 'RESPONDED_REJECT'),
    ('Défavorable',                   'DEF', 'RESPONDED_REJECT'),
    ('Hors Mission',                  'HM',  'RESPONDED_HM'),
    ('Suspendu',                      'SUS', 'RESPONDED_OTHER'),
    ('Sollicitation supplémentaire',  'SUP', 'RESPONDED_OTHER'),
    ('En attente',                    None,  'NOT_RESPONDED'),
    ('Soumis',                        None,  'PENDING_CIRCUIT'),
]
```

---

### Implementation Brief: `nm2_sas.py` [Run SECOND — after NM3]

**[V1.1 — P2] NM2 requires NM3 output. Remove all inline SAS vocabulary mapping.**

Implement `interpret_sas(ged_long: pd.DataFrame) -> pd.DataFrame`

**Contract validation [P5]:** Verify `doc_id`, `mission_type`, `reponse_normalized`, `response_status` are present before processing. Raise `ContractError` if missing.

**`ged_long` must already have `mission_type`, `reponse_normalized`, `response_status` from NM3.**

Returns: one row per `doc_id` with columns: `doc_id`, `sas_state`, `sas_verdict`, `sas_repondant`, `sas_date`, `sas_confidence`, `inference_flags`

Logic (from NM2-GED spec §5):
1. Filter `ged_long` to `mission_type == 'SAS'`
2. For each doc with SAS row: classify state from **`reponse_normalized` and `response_status`** (NOT `reponse_raw`)
3. For docs with multiple SAS rows: keep most recent by `date_reponse`, `log_event()` WARNING
4. For docs with no SAS row: `sas_state = SAS_UNKNOWN`, `inference_flags = ['SAS_ASSUMED_PASSED']`
5. Merge back to produce one row per `doc_id`

**[V1.1 — P2] No inline SAS vocabulary.** NM2 trusts `response_status`:
- `RESPONDED_REJECT` on SAS row → `SAS_BLOCKED`
- `RESPONDED_APPROVE` on SAS row → `SAS_PASSED`
- `NOT_RESPONDED` / `PENDING_CIRCUIT` on SAS row → `SAS_PENDING`
- Anything else on SAS row → `SAS_UNKNOWN` + WARNING

---

### Implementation Brief: `nm4_assignment.py` [Run THIRD — after NM3]

Implement `classify_assignments(ged_long: pd.DataFrame, circuit_matrix=None) -> pd.DataFrame`

Returns: `ged_long` with new columns: `assignment_type`, `assignment_source`, `final_response_status`
Plus: one document-level summary DataFrame.

Logic (from NM4-GED spec):
1. For all REVIEWER rows: classify assignment type from matrix or default to `REQUIRED_VISA`
2. Apply keyword scan on `commentaire` for CONDITIONAL reviewer activation
3. Compute `final_response_status` from assignment_type + response_status

**[V1.1 — P3] HM handling is explicit — enforce in implementation:**
```
hm_reviewers = REQUIRED_VISA rows where final_response_status = RESPONDED_HM
relevant_reviewers = assigned_reviewers − hm_reviewers
responded_approve = count(relevant_reviewers where RESPONDED_APPROVE)
responded_reject  = count(relevant_reviewers where RESPONDED_REJECT)
not_responded     = count(relevant_reviewers where NOT_RESPONDED)
missing_reviewers = list(relevant_reviewers where NOT_RESPONDED)
blocking_reviewers = list(relevant_reviewers where RESPONDED_REJECT)
```
HM reviewers do NOT count as missing. HM reviewers do NOT block consensus.

5. Exclude MOEX and SAS rows from assigned_reviewers

**V1 constraint:** If `circuit_matrix=None`, all present REVIEWER rows default to `REQUIRED_VISA` with `assignment_source = GED_PRESENCE`. `log_event()` INFO `MATRIX_NOT_LOADED`.

---

### Implementation Brief: `nm5_revisions.py` [Run FOURTH — after NM4]

Implement `compute_active_dataset(ged_long: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]`

Returns: `(ged_long_enriched, active_dataset)`

Where `ged_long_enriched` has: `is_latest_version`, `is_latest_indice`, `is_active`, `revision_count`, `previous_indice`, `is_cross_lot`, `has_revision_gap`

Logic (from NM5-GED spec):
1. Deduplicate to doc-level for version/indice analysis
2. Compute `is_latest_version` via groupby `(famille_key, INDICE)` on `version_number`
3. **[V1.1 — P4]** Compute `is_latest_indice` via groupby **`(famille_key, lot, batiment)`** on `indice_sort_order` — NOT just `famille_key`
4. Compute `is_active = is_latest_version AND is_latest_indice`
5. Compute revision chains: revision_count, previous_indice, has_revision_gap
6. Detect cross-lot families
7. Filter to `active_dataset`

**Performance:** All operations must use `.groupby().transform()` or `.groupby().agg()`. No loops.

---

### Test Briefs: Milestone 2

**`test_nm3_vocab.py`:**
```python
def test_mission_type_sas()          # '0-SAS' → 'SAS'
def test_mission_type_moex()         # "B-Maître d'Oeuvre EXE" → 'MOEX'
def test_mission_type_reviewer()     # '0-BET Structure' → 'REVIEWER'
def test_mission_type_unknown()      # None/empty → 'UNKNOWN'
def test_mission_type_never_null()   # 100% non-null
def test_all_known_vocab()           # All 11 known values map correctly
def test_sas_variant_priority()      # 'Validé sans observation - SAS' before 'Validé sans observation'
def test_favorable_maps_approve()    # FAV → RESPONDED_APPROVE
def test_defavorable_maps_reject()   # DEF → RESPONDED_REJECT
def test_en_attente_dual_repr()      # response_normalized=null, status=NOT_RESPONDED, raw preserved
def test_soumis_dual_repr()          # response_normalized=null, status=PENDING_CIRCUIT
def test_unknown_vocab_ambiguous()   # Unknown → RESPONDED_AMBIGUOUS, log_event called
def test_reponse_raw_unchanged()     # reponse_raw never modified
def test_response_status_never_null()
def test_nm3_contract_missing_column()  # [P5] ContractError raised if mission/reponse_raw absent
def test_no_iterrows_in_nm3()
```

**`test_nm2_sas.py`:**
```python
def test_sas_blocked_from_responded_reject()
def test_sas_passed_from_responded_approve()
def test_sas_pending_from_not_responded()
def test_sas_unknown_no_row()        # No SAS row → SAS_UNKNOWN + SAS_ASSUMED_PASSED flag
def test_sas_unknown_unexpected_status()  # HM on SAS row → SAS_UNKNOWN + WARNING
def test_multiple_sas_rows()
def test_one_row_per_doc()
def test_nm2_contract_missing_column()  # [P5] ContractError raised if mission_type absent
```

---

### Milestone 2 Validation Checkpoint

```python
from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe
clear_log()

# Order: NM3 first, NM2 second
ged_long_nm3 = normalize_responses(ged_long)
nm2_result   = interpret_sas(ged_long_nm3)

# NM3 scope check
assert 'mission_type' in ged_long_nm3.columns
assert 'response_status' in ged_long_nm3.columns
print(ged_long_nm3['mission_type'].value_counts())  # REVIEWER >> MOEX > SAS

# NM2 scope check
assert nm2_result['sas_state'].notna().all()
print(nm2_result['sas_state'].value_counts())       # SAS_UNKNOWN dominant

# SAS_ASSUMED_PASSED majority (most docs have no SAS row)
assumed = nm2_result[nm2_result['inference_flags'].apply(lambda f: 'SAS_ASSUMED_PASSED' in f)]
assert len(assumed) > len(nm2_result) * 0.9, "Expected >90% SAS_ASSUMED_PASSED"

# No data loss
assert ged_long_nm3['reponse_raw'].notna().sum() >= ged_long['reponse_raw'].notna().sum()

# No unexplained errors
log_df = get_log_as_dataframe()
assert log_df[log_df['severity'] == 'ERROR'].empty if len(log_df) > 0 else True

print("Milestone 2: PASS")
```

**Milestone 2 done when:** `mission_type` and `response_status` in NM3 output, `sas_state` complete, no data loss, no errors.

> **STOP. Report results. Wait for validation before starting Milestone 3.**

---

## Milestone 3 — NM4 + NM5: Assignment & Active Dataset

### Goal

Classify reviewer assignment types and produce the active document dataset. Input: `ged_long` enriched by NM3. NM4 runs before NM5.

At the end of Milestone 3: `ged_long` with `assignment_type` and `final_response_status`, plus `active_dataset` filtered to latest active documents.

**Scope:** `nm4_assignment.py` and `nm5_revisions.py` only. Do not implement NM7.

> **STOP RULE [P4]:** After completing Milestone 3 validation, STOP. Do NOT proceed to Milestone 4. Report results and wait for explicit validation approval.

### Files to Create

| File | Purpose |
|---|---|
| `jansa/adapters/ged/nm4_assignment.py` | NM4 — assignment classification |
| `jansa/adapters/ged/nm5_revisions.py` | NM5 — revision linking + active dataset |
| `jansa/tests/ged/test_nm4_assignment.py` | NM4 unit tests |
| `jansa/tests/ged/test_nm5_revisions.py` | NM5 unit tests |
| `jansa/tests/ged/test_integration.py` | NM1→NM5 integration test |

### Files to Modify

| File | Change |
|---|---|
| `jansa/adapters/ged/constants.py` | Add `KEYWORD_ACTIVATION`, `CONFIDENCE_DEDUCTIONS` stub |

---

### Implementation Brief: `nm4_assignment.py` [Run FIRST in Milestone 3]

Implement `classify_assignments(ged_long: pd.DataFrame, circuit_matrix=None) -> pd.DataFrame`

**Contract validation [P5]:** Verify `doc_id`, `mission`, `mission_type`, `response_status`, `commentaire` present. Raise `ContractError` if missing.

Logic (from NM4-GED spec §5):
1. For REVIEWER rows: classify from matrix → `REQUIRED_VISA` / `INFORMATIONAL` / `CONDITIONAL`; else → `UNKNOWN_REQUIRED` + `UNKNOWN_ASSIGNMENT` flag (see spec §V1.2 R-NM4-02)
2. Keyword scan on `commentaire` for CONDITIONAL activation
3. Compute `final_response_status`; HM excluded from `relevant_reviewers` (see spec R-NM4-06)
4. Build document-level summary

**V1 default:** If `circuit_matrix=None`, all present REVIEWER rows → `UNKNOWN_REQUIRED` + `UNKNOWN_ASSIGNMENT` flag. Log INFO `MATRIX_NOT_LOADED`.

---

### Implementation Brief: `nm5_revisions.py` [Run SECOND in Milestone 3]

Implement `compute_active_dataset(ged_long: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]`

**Contract validation [P5]:** Verify `doc_id`, `famille_key`, `indice`, `indice_sort_order`, `version_number`, `lot`, `batiment` present. Raise `ContractError` if missing.

Logic (from NM5-GED spec §5):
1. `is_latest_version` via groupby `(famille_key, INDICE)` on `version_number`
2. `is_latest_indice` via groupby **`(famille_key, lot, batiment)`** on `indice_sort_order`
3. `is_active = is_latest_version AND is_latest_indice`
4. Revision chains, cross-lot detection
5. Filter to `active_dataset`

All operations must use `.groupby().transform()`. No loops.

---

### Test Briefs: Milestone 3

**`test_nm4_assignment.py`:**
```python
def test_present_row_is_assigned()
def test_absent_row_not_assigned()
def test_hm_excluded_from_relevant_reviewers()
def test_hm_not_counted_as_missing()
def test_hm_not_counted_as_blocking()
def test_unknown_required_fallback()  # [V1.2 P4] no matrix → UNKNOWN_REQUIRED + flag
def test_unknown_assignment_flag_set()
def test_keyword_activation_acoustique()
def test_zero_reviewer_rows()
def test_informational_not_applicable()
def test_nm4_contract_missing_column()  # [P5] ContractError
def test_no_iterrows_in_nm4()
```

**`test_nm5_revisions.py`:**
```python
def test_is_latest_version()
def test_is_latest_indice_family_lot_batiment()  # (famille_key, lot, batiment) key
def test_is_active_both_conditions()
def test_active_dataset_one_per_family_lot_batiment()
def test_revision_count()
def test_revision_gap_detection()
def test_cross_lot_detection()
def test_no_superseded_in_active()
def test_nm5_contract_missing_column()  # [P5] ContractError
def test_no_iterrows_in_nm5()
```

**`test_integration.py`** (NM1→NM5, mandatory):
```python
def test_full_pipeline_nm1_to_nm5():
    # Run NM1 → NM3 → NM2 → NM4 → NM5 on sample fixtures
    # Assert: no crash, active_dataset not empty, deterministic
    from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe
    from jansa.tests.ged.fixtures.sample_ged_rows import SAMPLE_ROWS
    import pandas as pd
    clear_log()
    df = pd.DataFrame(SAMPLE_ROWS)
    # NM1
    from jansa.adapters.ged.nm1_loader import _process_dataframe
    ged_long = _process_dataframe(df)
    assert 'mission_type' not in ged_long.columns
    # NM3
    from jansa.adapters.ged.nm3_vocab import normalize_responses
    ged_long = normalize_responses(ged_long)
    # NM2
    from jansa.adapters.ged.nm2_sas import interpret_sas
    nm2 = interpret_sas(ged_long)
    assert nm2['sas_state'].notna().all()
    # NM4
    from jansa.adapters.ged.nm4_assignment import classify_assignments
    ged_long, nm4_summary = classify_assignments(ged_long)
    # NM5
    from jansa.adapters.ged.nm5_revisions import compute_active_dataset
    ged_long, active_dataset = compute_active_dataset(ged_long)
    assert len(active_dataset) > 0
    # Determinism
    clear_log()
    _, active2 = compute_active_dataset(classify_assignments(
        normalize_responses(_process_dataframe(df)))[0])
    assert active_dataset.equals(active2)
    assert get_log_as_dataframe()[get_log_as_dataframe()['severity']=='ERROR'].empty
```

---

### Milestone 3 Validation Checkpoint [P7 — NM1→NM5 Integration]

```python
from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe
clear_log()

ged_long_nm3 = normalize_responses(ged_long)
nm2_result = interpret_sas(ged_long_nm3)
ged_long_nm4, nm4_summary = classify_assignments(ged_long_nm3)
ged_long_nm5, active_dataset = compute_active_dataset(ged_long_nm4)

# No crash
assert active_dataset is not None

# Active dataset not empty
assert len(active_dataset) > 0, "active_dataset must not be empty"
assert active_dataset['doc_id'].nunique() > 0

# One active doc per (famille_key, lot, batiment)
counts = active_dataset.drop_duplicates('doc_id').groupby(
    ['famille_key', 'lot', 'batiment']).size()
assert (counts == 1).all(), "Multiple active docs per (famille_key, lot, batiment)"

# Assignment types populated
assert ged_long_nm4['assignment_type'].notna().all()

# No data loss vs NM1
assert len(ged_long_nm5) >= len(ged_long) * 0.95, "Unexpected row loss"

# No unexplained errors
log_df = get_log_as_dataframe()
assert log_df[log_df['severity'] == 'ERROR'].empty if len(log_df) > 0 else True

# Integration test passes
# Run: pytest jansa/tests/ged/test_integration.py -v

print("Milestone 3: PASS")
```

**Milestone 3 done when:** No crash, `active_dataset` not empty, uniqueness on `(famille_key, lot, batiment)`, integration test passes.

> **STOP. Report results. Wait for validation before starting Milestone 4.**

---

## Milestone 4 — NM7: Lifecycle State, Blocker & Priority

### Goal

Build the prioritization engine on top of the verified workflow state from Milestones 1–3. Produce the priority queue and intake issues bucket.

**Scope:** `nm7_lifecycle.py` only.

> **STOP RULE [P4]:** After completing Milestone 4 validation, STOP. Report final results.

### Files to Create

| File | Purpose |
|---|---|
| `jansa/pipeline/nm7_lifecycle.py` | NM7 main module |
| `jansa/tests/ged/test_nm7_lifecycle.py` | NM7 unit tests |

### Files to Modify

| File | Change |
|---|---|
| `jansa/adapters/ged/constants.py` | Add `LIFECYCLE_WEIGHT`, `BLOCKER_WEIGHT`, `CONFIDENCE_DEDUCTIONS` (complete) |

---

### Implementation Brief: `nm7_lifecycle.py`

Implement `run_nm7(active_dataset, nm2_result, nm4_summary, nm5_enriched, reference_date=None) -> tuple[pd.DataFrame, pd.DataFrame]`

**Contract validation [P5]:** Verify `doc_id`, `famille_key`, `sas_state`, `assigned_reviewers`, `relevant_reviewers`, `responded_approve`, `responded_reject`, `not_responded`, `revision_count`, `is_active` are present. Raise `ContractError` if missing.

**Add to constants.py:**
```python
LIFECYCLE_WEIGHT = {
    'CONFLICT':           100,
    'READY_TO_ISSUE':      90,
    'CHRONIC_BLOCKED':     80,
    'FAST_REJECT':         70,
    'WAITING_RESPONSES':   60,
    'NOT_STARTED':         30,
    'SAS_PENDING':         10,
    'SAS_BLOCKED':         10,
}

BLOCKER_WEIGHT = {
    'GEMO_MOEX': 20,
    'CONSULTANT': 10,
    'COMPANY':    5,
    'GEMO_SAS':   5,
    'NONE':       0,
}

CONFIDENCE_DEDUCTIONS = {
    'SAS_ASSUMED_PASSED':               -0.30,
    'USED_FALLBACK_DISCIPLINE':         -0.20,
    'CONDITIONAL_TRIGGERED_FROM_COMMENT': -0.10,
    'AMBIGUOUS_RESPONSE':               -0.20,
    'MULTIPLE_MOEX_VERDICTS':           -0.25,
    'MISSING_ASSIGNMENT_DATA':          -0.20,
    'MISSING_MOEX_ASSIGNMENT':          -0.15,
    'UNKNOWN_ASSIGNMENT':               -0.15,   # [V1.2] reviewer present but role unclassified
}
```

Logic (from NM7 spec):
1. Build document-level view: merge active_dataset + nm2_result + nm4_summary + nm5_enriched
2. For each doc: collect all MOEX rows and apply `get_moex_verdict(moex_rows, doc_id)` → `(visa_global, source, confidence, extra_flags)`. Merge `extra_flags` into `inference_flags` immediately.
3. Apply lifecycle state decision tree (9 branches in strict order per spec)
4. Compute consensus_type
5. Compute confidence_score from accumulated inference_flags using CONFIDENCE_DEDUCTIONS
6. **[V1.1 — P9]** Compute `is_late` per reviewer row: `ecart_reponse < 0 AND date_reponse not null`
7. Compute time metrics per document: `days_since_depot`, `days_until_deadline`, `is_overdue`, `days_overdue`
8. Compute priority_score and priority_category
9. Route: `PRIORITY_QUEUE` or `INTAKE_ISSUES` or `EXCLUDED`
10. Sort PRIORITY_QUEUE by: priority_score DESC, days_overdue DESC, days_since_depot DESC, famille_key ASC

**[V1.1 — P6] Flag propagation is mandatory:**
```python
visa_global, source, conf, extra_flags = get_moex_verdict(moex_rows, doc_id)
inference_flags = inference_flags + extra_flags  # merges MULTIPLE_MOEX_VERDICTS if present
```

**[V1.1 — P5] ALL_APPROVE + no MOEX row:**
```python
if consensus_type == 'ALL_APPROVE' and not moex_rows:
    log_event(doc_id, 'NM7', 'WARNING', 'MISSING_MOEX_ASSIGNMENT',
              "All reviewers approved but no MOEX mission row found.")
    inference_flags.append('MISSING_MOEX_ASSIGNMENT')
    # lifecycle_state = READY_TO_ISSUE unchanged — action is the same
```

**[V1.1 — P9]** `is_late` is computed here, not in NM1:
```python
# Vectorized — GP-NOVECLOOP compliant
active_dataset['is_late'] = (
    active_dataset['date_reponse'].notna() &
    active_dataset['ecart_reponse'].notna() &
    (pd.to_numeric(active_dataset['ecart_reponse'], errors='coerce') < 0)
)
```

**Performance:** NM7 lifecycle state operates on one row per document (doc-level aggregate). `is_late` is a row-level computation on `active_dataset` (O(n)). All groupby operations. No loops.

---

### Test Brief: `test_nm7_lifecycle.py`

```python
def test_synthesis_issued_excluded()
  # visa_global = VAO → SYNTHESIS_ISSUED, EXCLUDED

def test_sas_blocked_intake()
  # sas_state = SAS_BLOCKED → lifecycle = SAS_BLOCKED, INTAKE_ISSUES

def test_sas_pending_intake()
  # sas_state = SAS_PENDING → lifecycle = SAS_PENDING, INTAKE_ISSUES

def test_sas_unknown_treated_as_passed()
  # sas_state = SAS_UNKNOWN → inference_flags has SAS_ASSUMED_PASSED, continues to reviewer branches

def test_hm_excluded()
  # All relevant reviewers = HM → HM_EXCLUDED, EXCLUDED

def test_zero_reviewer_rows()
  # No reviewer rows → NOT_STARTED, blocker_type = GEMO_MOEX

def test_not_started()
  # All assigned, zero responses → NOT_STARTED, blocker = first reviewer

def test_waiting_responses()
  # Some responded, some NOT_RESPONDED → WAITING_RESPONSES

def test_ready_to_issue()
  # All approved → READY_TO_ISSUE, blocker_type = GEMO_MOEX

def test_fast_reject()
  # All rejected, revision_count = 1 → FAST_REJECT, blocker_type = COMPANY

def test_chronic_blocked()
  # All rejected, revision_count = 3 → CHRONIC_BLOCKED, blocker_type = COMPANY

def test_conflict()
  # Mixed approve/reject → CONFLICT, blocker_type = GEMO_MOEX

def test_soumis_moex_not_synthesized()
  # MOEX response_status = PENDING_CIRCUIT → visa_global = null → reviewer logic continues

def test_multiple_moex_verdicts_most_recent()
  # [V1.1 — P6] Two MOEX rows with different verdicts → most recent wins, WARNING via log_event()
  # LOW confidence

def test_multiple_moex_verdicts_flag_propagated()
  # [V1.1 — P6] MULTIPLE_MOEX_VERDICTS in inference_flags when conflict exists
  # assert 'MULTIPLE_MOEX_VERDICTS' in doc['inference_flags']

def test_all_approve_no_moex_row()
  # [V1.1 — P5] All reviewers approved, no MOEX mission row present
  # lifecycle_state = READY_TO_ISSUE (unchanged action)
  # inference_flags contains 'MISSING_MOEX_ASSIGNMENT'
  # WARNING logged via log_event()

def test_missing_moex_confidence_deduction()
  # [V1.1 — P5] MISSING_MOEX_ASSIGNMENT flag reduces confidence by 0.15

def test_is_late_computed_in_nm7()
  # [V1.1 — P9] is_late present in active_dataset after NM7 Step 6
  # ecart_reponse < 0 AND date_reponse not null → is_late = True
  # ecart_reponse >= 0 → is_late = False
  # date_reponse null → is_late = False (not yet responded, not late)

def test_is_late_not_in_nm1_output()
  # [V1.1 — P9] Confirm is_late absent from raw NM1 output (guard test)
  # assert 'is_late' not in ged_long_nm1.columns

def test_priority_score_conflict_highest()
  # CONFLICT doc scores higher than WAITING doc with same overdue status

def test_priority_score_overdue_bonus()
  # Overdue WAITING scores higher than non-overdue WAITING

def test_no_deadline_penalty()
  # has_deadline = False → delay_weight = -10

def test_sas_assumed_passed_confidence_deduction()
  # SAS_ASSUMED_PASSED flag → confidence_score reduced by 0.30

def test_confidence_floor()
  # Multiple bad flags → confidence_score never below 0.1

def test_sort_order_deterministic()
  # Same scores → sorted by famille_key ASC (reproducible)
  # Run twice → identical row order

def test_priority_category_bands()
  # ≥150 → CRITICAL, ≥100 → HIGH, ≥60 → MEDIUM, <60 → LOW

def test_all_weights_are_constants()
  # LIFECYCLE_WEIGHT, BLOCKER_WEIGHT, CONFIDENCE_DEDUCTIONS imported from constants
  # No numeric literals for weights in nm7_lifecycle.py source
  # grep/inspect check: no magic numbers

def test_no_iterrows_in_nm7()
  # [V1.1 — P7] 'iterrows' not in nm7_lifecycle source
  # import inspect, jansa.pipeline.nm7_lifecycle as m
  # assert 'iterrows' not in inspect.getsource(m)

def test_log_event_used_not_print()
  # [V1.1 — P8] 'print(' not in nm7_lifecycle source
  # All events emitted via log_event()
```

---

### Milestone 3 Validation Checkpoint

Run against real GED file:

```python
from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe
clear_log()

priority_queue, intake_issues = run_nm7(
    active_dataset, nm2_result, nm4_summary, nm5_enriched,
    reference_date=date.today()
)

# Check 1: Queue sizes
print(f"Priority queue: {len(priority_queue)} docs")
print(f"Intake issues: {len(intake_issues)} docs")
# Expected: priority_queue > 0, intake_issues small (few SAS records)

# Check 2: Lifecycle distribution
print(priority_queue['lifecycle_state'].value_counts())
# Expected: mix of NOT_STARTED, WAITING_RESPONSES, READY_TO_ISSUE, CONFLICT, etc.

# Check 3: No SYNTHESIS_ISSUED in queue
assert 'SYNTHESIS_ISSUED' not in priority_queue['lifecycle_state'].values

# Check 4: No SAS_BLOCKED in priority queue
assert 'SAS_BLOCKED' not in priority_queue['lifecycle_state'].values

# Check 5: Priority scores reasonable
print(priority_queue['priority_score'].describe())
# Expected: min >= 0, max <= 200, mean in 40-100 range

# Check 6: Priority categories present
print(priority_queue['priority_category'].value_counts())

# Check 7: Deterministic — run twice, compare
pq1, _ = run_nm7(active_dataset, nm2_result, nm4_summary, nm5_enriched, reference_date=date.today())
pq2, _ = run_nm7(active_dataset, nm2_result, nm4_summary, nm5_enriched, reference_date=date.today())
assert pq1.equals(pq2), "NM7 is not deterministic"

# Check 8: [V1.1 — P9] is_late present in active_dataset after NM7
assert 'is_late' in active_dataset.columns, "is_late must be added by NM7"
late_count = active_dataset['is_late'].sum()
print(f"Late reviewer responses: {late_count}")  # Expected: several hundred

# Check 9: [V1.1 — P6] MULTIPLE_MOEX_VERDICTS logged when applicable
log = get_log_as_dataframe()
multi_moex = log[log['code'] == 'MULTIPLE_MOEX_VERDICTS'] if len(log) > 0 else pd.DataFrame()
print(f"MULTIPLE_MOEX_VERDICTS warnings: {len(multi_moex)}")  # Expected: low number or 0

# Check 10: Top 10 items make operational sense
print(priority_queue.head(10)[
    ['doc_id', 'lifecycle_state', 'priority_score', 'blocker_type', 'days_overdue',
     'confidence_score', 'inference_flags']
])
```

### Milestone 4 Validation Checkpoint [P8 — Full NM1→NM7 Pipeline Test]

```python
from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe
from datetime import date
clear_log()

priority_queue, intake_issues = run_nm7(
    active_dataset, nm2_result, nm4_summary, nm5_enriched,
    reference_date=date.today()
)

# Not empty
assert len(priority_queue) > 0, "Priority queue must not be empty"

# Routing correctness
assert 'SYNTHESIS_ISSUED' not in priority_queue['lifecycle_state'].values
assert 'SAS_BLOCKED' not in priority_queue['lifecycle_state'].values

# No nulls in critical fields
assert priority_queue['lifecycle_state'].notna().all()
assert priority_queue['priority_score'].notna().all()
assert priority_queue['queue_destination'].notna().all()

# Lifecycle distribution (expected mix — no single state should dominate abnormally)
print(priority_queue['lifecycle_state'].value_counts())

# Priority score range
assert priority_queue['priority_score'].between(0, 200).all()
print(priority_queue['priority_score'].describe())

# is_late present (NM7 adds it)
assert 'is_late' in active_dataset.columns

# Deterministic — two runs must produce identical output [P8]
pq1, _ = run_nm7(active_dataset, nm2_result, nm4_summary, nm5_enriched, reference_date=date.today())
pq2, _ = run_nm7(active_dataset, nm2_result, nm4_summary, nm5_enriched, reference_date=date.today())
assert pq1.equals(pq2), "NM7 is not deterministic — same input must produce same output"

# No unexplained errors
log_df = get_log_as_dataframe()
errors = log_df[log_df['severity'] == 'ERROR'] if len(log_df) > 0 else pd.DataFrame()
assert errors.empty, f"Unexpected errors: {errors[['code','message']].to_dict('records')}"

# Operationally plausible top 10
print(priority_queue.head(10)[['doc_id','lifecycle_state','priority_score','blocker_type','days_overdue']])

print("Milestone 4: PASS")
```

**Milestone 4 done when:** Priority queue not empty, no nulls in critical fields, deterministic across two runs, no unexplained errors, top 10 items plausible.

> **STOP. Report final results. Implementation complete.**

---

## What Cowork Must NOT Do

### Never do across all milestones:

- Do not implement more than the current milestone specifies
- Do not add UI, dashboards, or visualization code
- Do not implement AI text generation (that is NM9, out of scope)
- Do not build a REST API (not in scope for this plan)
- Do not read from files in unit tests — use inline fixtures only
- Do not hardcode magic numbers — all weights and constants in `constants.py`
- Do not modify `reponse_raw` after NM1 sets it
- Do not use the `Statut final du document` field for lifecycle state

### [V1.1 — P7] Row-level loops are STRICTLY FORBIDDEN:

```
FORBIDDEN in all modules:
  - iterrows()
  - itertuples()
  - for row in df.iterrows(): ...
  - for i, row in df.iterrows(): ...
  - for idx in range(len(df)): df.iloc[idx]...

REQUIRED instead:
  - df['col'].map(func)         for scalar per-row transforms
  - df.apply(func, axis=1)      only when vectorization truly impossible
  - df.groupby(...).transform() for grouped computations
  - df.groupby(...).agg()       for aggregations
  - vectorized pandas operations (comparisons, arithmetic, .str., .dt.)
```

A lint check MUST run as part of any test suite:
```python
def test_no_iterrows_anywhere():
    import inspect, pkgutil, importlib
    import jansa.adapters.ged as pkg
    for _, name, _ in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + '.'):
        mod = importlib.import_module(name)
        src = inspect.getsource(mod)
        assert 'iterrows' not in src, f"iterrows found in {name}"
        assert 'itertuples' not in src, f"itertuples found in {name}"
```

### [V1.1 — P8] Centralized logging is MANDATORY:

```
FORBIDDEN:
  - print(...)
  - logging.getLogger(...)
  - local_log.append(...)
  - any ad-hoc list used as a log

REQUIRED:
  - from jansa.adapters.ged.logging import log_event
  - log_event(doc_id, module, severity, code, message, ...)
```

Every anomaly, warning, and info event in every module must go through `log_event()`. This is enforced by the following test in `test_nm1_loader.py` and `test_integration.py`:
```python
def test_no_print_in_pipeline():
    import inspect, jansa.adapters.ged.nm1_loader as m1
    assert 'print(' not in inspect.getsource(m1)
```

### [V1.1 — P1+P2] Module boundary violations are bugs:

```
NM1 must NOT produce: mission_type, is_late, reponse_normalized, response_status
NM3 must run BEFORE NM2 and NM4 — never the other way around
NM2 must NOT contain VOCAB_MAP or inline response string matching
NM7 computes is_late — it must NOT be expected from upstream modules
```

### Architecture boundaries (reinforced for V1.2):

- NM1: ingestion + structural normalization only. No workflow semantics.
- NM3: vocabulary normalization + `mission_type` classification. **Runs before NM2 and NM4.**
- NM2: SAS interpretation only. Depends on NM3. No inline vocab.
- NM4: assignment classification only. Fallback is `UNKNOWN_REQUIRED` (not `REQUIRED_VISA`). No lifecycle state.
- NM5: active dataset computation only. Uniqueness key is `(famille_key, lot, batiment)`. No priority scoring.
- NM7: lifecycle + scoring + routing. Computes `is_late`. Read-only access to NM1–NM5. Never modifies upstream.

### Code quality:

- Every function has a docstring: inputs, outputs, what it does NOT do
- Every module has a module-level docstring referencing `GED_PHASE_B_SPECS_V1_2.md` and its section
- No `try/except` that silently swallows exceptions
- If spec and plan conflict: **follow the spec, stop and report**

---

## Definition of Done (Overall)

The implementation is complete when:

1. ✅ Milestone 1 validation passes on real GED file
2. ✅ Milestone 2 validation passes (NM3 output verified, SAS complete, no errors)
3. ✅ Milestone 3 validation passes (`active_dataset` non-empty, uniqueness on `(famille_key, lot, batiment)`, integration test passes)
4. ✅ Milestone 4 validation passes (priority queue non-empty, deterministic, no nulls in critical fields)
5. ✅ All unit tests pass: `pytest jansa/tests/ged/ -v`
6. ✅ `mission_type` absent from NM1 output, present in NM3 output
7. ✅ `is_late` absent from NM1 output, present after NM7 runs
8. ✅ `UNKNOWN_REQUIRED` used for unclassified reviewers (not direct `REQUIRED_VISA`)
9. ✅ `iterrows` and `itertuples` absent from all module source files
10. ✅ `print(` absent from all pipeline module source files
11. ✅ All weights and vocab constants in `constants.py` — no magic numbers in module files
12. ✅ All modules use `log_event()` — zero unexplained ERRORs in pipeline log
13. ✅ NM7 output is deterministic: same input → same output across two runs
14. ✅ Top 10 priority queue items reviewed and operationally plausible

---

## Prompt-Ready Execution Blocks

Use these prompts verbatim to start each Cowork session.

### Prompt: Start Milestone 1

```
You are implementing Milestone 1 of the JANSA VISASIST GED pipeline.

Authoritative spec: GED_PHASE_B_SPECS_V1_2.md
Plan: GED_COWORK_IMPLEMENTATION_PLAN_V1_2.md (Milestone 1 section)

If this plan and the spec conflict, follow the spec and report the conflict.
If anything is ambiguous, STOP and report instead of guessing.

Your task — create these files only:
1. jansa/adapters/ged/exceptions.py
2. jansa/adapters/ged/logging.py — log_event() interface
3. jansa/adapters/ged/constants.py — IDENTITY_COLS, FAMILY_KEY_COLS, REQUIRED_COLUMNS
4. jansa/adapters/ged/nm1_loader.py — load_ged_export()
5. jansa/tests/ged/fixtures/sample_ged_rows.py — SAMPLE_ROWS
6. jansa/tests/ged/test_nm1_loader.py

Rules:
- NM1 = ingestion + structural normalization ONLY
- NM1 must NOT produce: mission_type, is_late, reponse_normalized, response_status
- Validate input schema before processing — raise NM1InputError if columns missing
- FORBIDDEN: iterrows(), itertuples(), row-level for loops
- FORBIDDEN: print() — use log_event()
- Tests: fixtures only, no file I/O

STOP after Milestone 1 validation passes. Report results. Wait for approval.
```

### Prompt: Start Milestone 2

```
You are implementing Milestone 2 of the JANSA VISASIST GED pipeline.
Milestone 1 is complete and validated.

Authoritative spec: GED_PHASE_B_SPECS_V1_2.md (NM3, NM2 sections)
Plan: GED_COWORK_IMPLEMENTATION_PLAN_V1_2.md (Milestone 2 section)

If this plan and the spec conflict, follow the spec and report the conflict.
If anything is ambiguous, STOP and report instead of guessing.

CRITICAL: NM3 runs before NM2. This order is mandatory.

Your task — create these files only:
1. jansa/adapters/ged/nm3_vocab.py — normalize_responses() — adds mission_type, reponse_normalized, response_status
2. jansa/adapters/ged/nm2_sas.py — interpret_sas() — uses NM3 response_status (NO inline vocab)
3. jansa/tests/ged/test_nm3_vocab.py
4. jansa/tests/ged/test_nm2_sas.py
5. Update constants.py — add VOCAB_MAP, MISSION_TYPE_PATTERNS

Rules:
- NM3 runs FIRST — mission_type is NM3's output, not NM1's
- NM2 must NOT contain any vocab mapping — use response_status from NM3
- Validate input contracts — raise ContractError if required columns missing
- FORBIDDEN: iterrows(), itertuples(), row-level for loops
- FORBIDDEN: print() — use log_event()

STOP after Milestone 2 validation passes. Report results. Wait for approval.
Do not implement NM4, NM5, or NM7.
```

### Prompt: Start Milestone 3

```
You are implementing Milestone 3 of the JANSA VISASIST GED pipeline.
Milestones 1 and 2 are complete and validated.
ged_long has mission_type, reponse_normalized, response_status from NM3.
nm2_result has sas_state per document.

Authoritative spec: GED_PHASE_B_SPECS_V1_2.md (NM4, NM5 sections)
Plan: GED_COWORK_IMPLEMENTATION_PLAN_V1_2.md (Milestone 3 section)

If this plan and the spec conflict, follow the spec and report the conflict.
If anything is ambiguous, STOP and report instead of guessing.

NM4 runs before NM5.

Your task — create these files only:
1. jansa/adapters/ged/nm4_assignment.py — classify_assignments()
2. jansa/adapters/ged/nm5_revisions.py — compute_active_dataset()
3. jansa/tests/ged/test_nm4_assignment.py
4. jansa/tests/ged/test_nm5_revisions.py
5. jansa/tests/ged/test_integration.py — test_full_pipeline_nm1_to_nm5()
6. Update constants.py — add KEYWORD_ACTIVATION

Rules:
- NM4 fallback when no matrix: UNKNOWN_REQUIRED + UNKNOWN_ASSIGNMENT flag (NOT direct REQUIRED_VISA)
- HM reviewers: excluded from relevant_reviewers, not counted as missing or blocking
- NM5 uniqueness key: (famille_key, lot, batiment) — not (famille_key, lot)
- Validate input contracts — raise ContractError if required columns missing
- FORBIDDEN: iterrows(), itertuples(), row-level for loops
- FORBIDDEN: print() — use log_event()
- circuit_matrix=None is valid V1: all REVIEWER rows → UNKNOWN_REQUIRED

STOP after Milestone 3 validation passes AND test_integration.py passes. Report results. Wait for approval.
Do not implement NM7.
```

### Prompt: Start Milestone 4

```
You are implementing Milestone 4 of the JANSA VISASIST GED pipeline.
Milestones 1–3 are complete and validated.
active_dataset, nm2_result, nm4_summary, nm5_enriched are ready.

Authoritative spec: GED_PHASE_B_SPECS_V1_2.md (NM7 section)
Plan: GED_COWORK_IMPLEMENTATION_PLAN_V1_2.md (Milestone 4 section)

If this plan and the spec conflict, follow the spec and report the conflict.
If anything is ambiguous, STOP and report instead of guessing.

Your task — create these files only:
1. jansa/pipeline/nm7_lifecycle.py — run_nm7()
2. jansa/tests/ged/test_nm7_lifecycle.py
3. Update constants.py — add LIFECYCLE_WEIGHT, BLOCKER_WEIGHT, CONFIDENCE_DEDUCTIONS (full, including UNKNOWN_ASSIGNMENT: -0.15)

Rules:
- Validate input contract — raise ContractError if required columns missing
- Compute is_late in NM7 (vectorized, from ecart_reponse) — not expected from upstream
- get_moex_verdict() returns extra_flags — MUST merge into inference_flags
- ALL_APPROVE + no MOEX row → READY_TO_ISSUE + MISSING_MOEX_ASSIGNMENT flag
- All weights from constants — no magic numbers in nm7_lifecycle.py
- Decision tree: exact order from spec, first match wins
- Output must be deterministic: same input → same output
- FORBIDDEN: iterrows(), itertuples(), row-level for loops
- FORBIDDEN: print() — use log_event()
- Never modify NM1–NM5 data

After implementation: run full Milestone 4 validation. Report all results.
```

### Prompt: Run Full Validation Against Real File

```
Run the complete pipeline validation against the real GED file.

File: data/17_CO_Tranche_2_du_23_mars_2026_07_45.xlsx
Plan: GED_COWORK_IMPLEMENTATION_PLAN_V1_2.md

Run milestone validation checkpoints in order: M1, M2, M3, M4.
Stop at the first failing milestone. Do not proceed past a failure.

Report format:
MILESTONE 1: [pass/fail]
  - [check]: [result]

MILESTONE 2: [pass/fail]
  - [check]: [result]

MILESTONE 3: [pass/fail]
  - [check]: [result]

MILESTONE 4: [pass/fail]
  - [check]: [result]

DEFINITION OF DONE (14 criteria): [N/14 pass]
  - [criterion]: [pass/fail]

For any failure: describe the discrepancy and proposed fix.
Do not auto-fix. Report first, wait for approval.
```

---

*Document status: GED Cowork Implementation Plan **V1.2** — Operational. Supersedes V1.1.*  
*Authority: **GED_PHASE_B_SPECS_V1_2.md** + GED_DIAGNOSTIC_V1_1.md*  
*V1.2 patches: spec authority rule, 4-milestone restructure (M1=NM1, M2=NM3+NM2, M3=NM4+NM5, M4=NM7), STOP rules, contract validation per module, no-loop global rule, logging global rule, NM1→NM5 integration checkpoint (M3), NM1→NM7 determinism test (M4), simplified validation metrics.*
