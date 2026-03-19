# JANSA VISASIST — Module 2: Technical Implementation Plan V2

**Data Model & Revision Linking**

Based on: V2.2 Production-Ready Specification

Revision: V2.1 — incorporates 8 review corrections (V2) + 5 final cleanup corrections (V2.1)

---

## Notation Convention

Throughout this document, every rule and decision is labelled with its origin:

- **[SPEC]** — Behavior defined by the V2.2 specification. Authoritative, non-negotiable. Implementation must match exactly.
- **[SAFEGUARD]** — Defensive engineering added for robustness. Not in the spec. Clearly separated so it can be reviewed, adjusted, or removed without affecting spec compliance.
- **[IMPLEMENTATION — deterministic by GP1]** — Implementation decision not explicitly defined in V2.2 but required for deterministic output per GP1. The chosen rule is one valid option; alternatives exist.

---

## 0. Scope & Constraints

This plan covers **Module 2 only**. It assumes Module 1 is fully implemented and produces the master dataset as specified (all columns, all quality grades, all approver blocks, `assigned_approvers`, `document`/`document_raw` pairs, `row_quality`, import log).

**Hard constraints:**

- **[SPEC]** Strictly follows V2.2 specification. No invented behavior.
- **[SPEC]** Deterministic: same input always produces same output.
- **[SPEC]** Simple and robust (MVP mindset). No UI, no external API.
- **[SPEC]** Respects GP1–GP5 global policies.
- **[SPEC]** The 88 naming-format pairs must resolve correctly via `doc_family_key` flattening.
- **[SPEC]** UNPARSEABLE rows must never group with valid documents or with each other.
- **[SPEC]** Duplicate detection (UNIQUE / DUPLICATE / SUSPECT) must be implemented before chain linking.
- **[SPEC]** Anomaly types are exactly: REVISION_GAP, LATE_FIRST_APPEARANCE, DATE_REGRESSION, DUPLICATE_EXACT, DUPLICATE_SUSPECT, MISSING_IND, UNPARSEABLE_DOCUMENT.

**Unchanged from V1.**

---

## 1. Architecture

### 1.1 Language & Libraries

Aligned with Module 1 choices:

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Python 3.11+ | Same as M1 |
| Data manipulation | `pandas >= 2.0` | Same as M1. Vectorized groupby operations for family grouping, chain linking, cross-lot detection. |
| Hashing | `hashlib` (stdlib) | Deterministic SHA-256 for UNPARSEABLE fallback keys. No external dependency. |
| Testing | `pytest >= 7.0` | Same as M1 |
| Serialization | `json` (stdlib), `csv` (stdlib), `openpyxl >= 3.1` | Same as M1 for JSON/CSV/Excel output per GP4 |

**No new dependencies required.** Module 2 adds zero entries to `requirements.txt`.

### 1.2 Design Pattern

Module 2 follows the same pattern as Module 1:

- A `Module2Context` dataclass accumulates state (linking anomalies log, family index).
- A top-level `run_module2(master_df, output_dir)` function orchestrates Steps 1–6 sequentially.
- Each step is a pure-ish function in its own file under `pipeline/m2/`.
- The pipeline receives the Module 1 master DataFrame and returns enriched DataFrame + side outputs.

### 1.3 Execution Model

**[SPEC]** Module 2 runs as a **single-pass pipeline** over the Module 1 master dataset:

```
master_df (from M1)
  → Step 1: doc_family_key construction      (row-level, vectorizable)
  → Step 2: ind_sort_order computation        (row-level, vectorizable)
  → Step 5: duplicate detection               (group-level, BEFORE chain linking)
  → Step 3: family grouping & chain linking   (group-level, after duplicates resolved)
  → Step 4: cross-lot detection               (group-level, across sheets)
  → Step 6: anomaly detection                 (group-level, final pass)
  → Output assembly
```

**[SPEC]** Note on ordering: Step 5 (duplicate detection) is executed **before** Step 3 (chain linking) per spec requirement: "Resolve duplicates in Step 5 before linking." Steps 1 and 2 are independent row-level transforms. Steps 3, 4, 6 are group-level operations that depend on 1, 2, 5.

**Unchanged from V1.**

---

## 2. Module 1 → Module 2 Interface Contract

### 2.1 What Module 2 Expects from Module 1

**[SPEC]** Module 2 consumes the Module 1 master dataset as a `pandas.DataFrame`. The following columns are **required** (consumed directly by Module 2 logic):

| Column | Type | Used In | Nullable? |
|--------|------|---------|-----------|
| `document` | str or None | Step 1 (family key) | Yes — null triggers UNPARSEABLE fallback |
| `document_raw` | str or None | Step 1 (fallback hash input) | Yes — preserved for traceability |
| `ind` | str or None | Step 2 (sort order), Step 3 (chain), Step 5 (dupes), Step 6 (anomalies) | Yes — null IND → sort_order = 0 |
| `lot` | str or None | Output (family index) | Yes |
| `source_sheet` | str | Step 1 (fallback hash), Step 3 (chain scope), Step 4 (cross-lot), Step 5 (dupe scope) | No — always populated |
| `source_row` | int | Step 1 (fallback hash) | No — always populated |
| `row_id` | str | Row-level unique identifier; traceability | No — always populated |
| `date_diffusion` | str (ISO) or None | Step 6 (DATE_REGRESSION) | Yes |
| `visa_global` | str or None | Output (family index: latest visa) | Yes |
| `row_quality` | str | Passthrough; participates in duplicate comparison (per Step 5 comparison rule) | No |
| `row_quality_details` | list or None | Passthrough; participates in duplicate comparison (per Step 5 comparison rule) | Yes |
| `assigned_approvers` | list[str] | Passthrough to M3; participates in duplicate comparison (per Step 5 comparison rule) | No |
| All `{APPROVER}_statut` columns | str or None | Passthrough to M3; participates in duplicate comparison (per Step 5 comparison rule) | Yes |
| All `*_raw` columns | str or None | Passthrough; participates in duplicate comparison (per Step 5 comparison rule) | Yes |

**[SPEC]** Passthrough columns: All Module 1 columns not listed above are carried through unchanged. Module 2 never modifies existing M1 columns — it only **adds** new columns.

**Note:** Which columns participate in duplicate comparison is defined authoritatively in §3 Step 5. All M1 columns participate except `row_id` and `source_row` (unique by definition) and any M2-derived columns present at Step 5 execution time.

### 2.2 Input Validation

**[SAFEGUARD]** Before processing, Module 2 performs a lightweight contract check. This is defensive engineering — the V2.2 spec does not define M2 entry validation behavior. These checks exist to fail fast with clear error messages if M1 output is malformed.

| Check | Action on Failure |
|-------|-------------------|
| `document` column exists | FATAL: raise `Module2InputError`. Cannot proceed without document field. |
| `ind` column exists | FATAL: raise `Module2InputError`. |
| `source_sheet` column exists | FATAL: raise `Module2InputError`. |
| `source_row` column exists | FATAL: raise `Module2InputError`. |
| `row_id` column exists | FATAL: raise `Module2InputError`. |
| `date_diffusion` column exists | WARNING logged. Step 6 DATE_REGRESSION skipped. Column created as all-null. |
| `visa_global` column exists | WARNING logged. Family index `latest_visa` will be null. Column created as all-null. |
| DataFrame is empty (0 rows) | Return immediately with empty outputs. No error. |

**[SAFEGUARD]** Rationale: `document`, `ind`, `source_sheet`, `source_row`, `row_id` are structurally essential — without them Module 2 cannot construct keys, scope groups, or trace rows. `date_diffusion` and `visa_global` degrade gracefully because they only affect anomaly detection and summary fields.

**V2 change from V1:** This entire section is now explicitly labelled [SAFEGUARD] to distinguish it from spec-defined behavior.

### 2.3 How Null Document Values Trigger the UNPARSEABLE Fallback

**[SPEC]** The contract between M1 and M2 for `document = null` is:

1. **M1 is solely responsible** for deciding if a document reference is valid. M2 trusts that decision completely.
2. **If `document` is not null:** M2 treats it as a valid, normalized reference and flattens it into `doc_family_key` by stripping underscores.
3. **If `document` is null:** M2 executes the fallback path. This null arises from exactly two M1 scenarios:
   - `document_raw` was blank/whitespace (GP2 null policy, M1 Step 3)
   - `document_raw` was structurally invalid (M1 Step 7a validation rules R1–R4)
4. **M2 performs no additional validation** on the `document` field. No regex checks, no length checks, no noise-ratio checks. That was M1's job.

**[SPEC]** Fallback key construction:

```
input_string = str(document_raw) + "::" + str(source_sheet) + "::" + str(source_row)
hash_hex = sha256(input_string.encode('utf-8')).hexdigest()[:16]
doc_family_key = "UNPARSEABLE::" + hash_hex
```

**[SAFEGUARD]** The 16-character hex truncation gives 64 bits of collision space — more than sufficient for datasets under 100K rows. The hash is deterministic: same `(document_raw, source_sheet, source_row)` always produces the same key.

**Unchanged from V1.**

---

## 3. Data Pipeline Steps — Detailed Design

### Step 1: doc_family_key Construction

**[SPEC]** Spec reference: V2.2 §2.4 Step 1.

**Input:** Full master DataFrame.

**Output:** New column `doc_family_key` added to every row.

**Algorithm:**

**[SPEC]** Primary path (document is not null):

```
doc_family_key = document.replace("_", "")
# Already uppercase from M1 normalization
# Result: P17T2INEXEVTPTERI001MTDTZFD026001
```

**[SPEC]** Fallback path (document is null):

```
raw_str = "" if document_raw is None else str(document_raw)
hash_input = raw_str + "::" + source_sheet + "::" + str(source_row)
hash_hex = sha256(hash_input.encode('utf-8')).hexdigest()[:16]
doc_family_key = UNPARSEABLE_PREFIX + hash_hex
Log UNPARSEABLE_DOCUMENT anomaly
```

**[SPEC]** Key properties:

- The 88 naming-format pairs (e.g., `P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001` and `P17T2INEXEVTPTERI001MTDTZFD026001`) produce **identical** `doc_family_key` values because both flatten to the same string after underscore removal.
- The `document` column is **never modified** — it stays human-readable with underscores. `doc_family_key` is a derived grouping key.
- Each UNPARSEABLE row gets a **unique** key (hash includes sheet + row). Two different unparseable documents never group together.

**Implementation:** Vectorized with `pandas.Series.where()` or `np.where()` for the primary path, then a loop over the (very few) null-document rows for the fallback path.

**Performance:** O(n) single pass. The fallback path (SHA-256 per row) only fires for null-document rows (currently 1 row).

**Unchanged from V1.**

### Step 2: Revision Index Normalization

**[SPEC]** Spec reference: V2.2 §2.4 Step 2.

**Input:** `ind` column from master DataFrame.

**Output:** New column `ind_sort_order` (integer).

**[SPEC]** The V2.2 spec defines exactly three cases:

```
Case 1: ind IS null
  → ind_sort_order = 0
  → Log MISSING_IND anomaly

Case 2: ind is purely alphabetic (single or multi-letter, e.g., "A", "B", "AA")
  → Compute column-style index: A=1, B=2, ..., Z=26, AA=27, AB=28, ...
  → ind_sort_order = computed value

Case 3: ind is purely numeric (e.g., "1", "02", "10")
  → ind_sort_order = int(ind)
```

**[SPEC]** Alpha-to-number function:

```python
def alpha_to_sort_order(s: str) -> int:
    """Convert A=1, B=2, ..., Z=26, AA=27, AB=28, ..."""
    result = 0
    for char in s.upper():
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result
```

**[SAFEGUARD]** Unexpected format fallback — not spec-defined:

The V2.2 spec does not define behavior for IND values that are neither null, nor purely alphabetic, nor purely numeric (e.g., mixed alphanumeric like "1A", "B2", or containing special characters). If such a value is encountered:

```
ind_sort_order = 0
Log WARNING: "Unexpected IND format — not defined in V2.2 spec. Treated as sort_order 0."
```

This safeguard ensures the pipeline never crashes on unexpected data. It is **not** V2.2 business logic. If the spec is later extended to define mixed-format behavior, this fallback must be replaced with the spec rule.

**Implementation:** Vectorized for the common alpha-only case (the vast majority of IND values are single letters A–Z). Numeric INDs parsed with `pd.to_numeric(..., errors='coerce')` first pass, then alpha conversion on the remainder.

**[SPEC]** Edge case — null IND: The spec says 13 rows have empty IND. These get `ind_sort_order = 0`, which places them **before** revision A in sort order. They still participate in chain linking and duplicate detection. Each null-IND row is logged as MISSING_IND.

**V2 change from V1:** The three spec-defined cases are now clearly separated from the unexpected-format fallback, which is explicitly labelled [SAFEGUARD]. V1 presented the fallback as part of the algorithm without distinction.

### Step 5: Duplicate Detection (Executed Before Step 3)

**[SPEC]** Spec reference: V2.2 §2.4 Step 5.

> **This section is the authoritative definition of the duplicate comparison rule.** All other references to duplicate comparison (§2.1, §7 config_m2.py) cross-reference this section. If wording conflicts, this section governs.

**Scope:** Group by `(doc_family_key, ind, source_sheet)`.

**Input:** Master DataFrame with `doc_family_key` and `ind` columns.

**Output:** New column `duplicate_flag` on every row, with values: `UNIQUE`, `DUPLICATE`, or `SUSPECT`.

**[SPEC]** Comparison rule: Compare all columns present in the DataFrame at Step 5 execution time, excluding `row_id` and `source_row` (unique by definition) and any Module 2 derived columns already present (`doc_family_key`, `ind_sort_order`). Everything else from Module 1 participates — including `row_quality`, `row_quality_details`, all `_raw` fields, and all approver columns.

**[SPEC]** Algorithm:

```
For each group (doc_family_key, ind, source_sheet):
  IF group has exactly 1 row:
    duplicate_flag = "UNIQUE"
  ELSE (2+ rows):
    Compare ALL Module 1 columns EXCEPT:
      - row_id (unique by definition — M1 assigns {sheet_index}_{source_row})
      - source_row (unique by definition — original Excel row number)

    NOTE: Module 2 derived columns do not exist at this point
    in the pipeline, so they are not present in the DataFrame
    and do not need explicit exclusion.

    Pick first row (by source_row ascending) as reference.
    Compare all other rows against reference on all remaining columns.

    IF all rows are identical on all comparison columns:
      First row (by source_row ASC): duplicate_flag = "UNIQUE"
      Subsequent rows: duplicate_flag = "DUPLICATE"
      Log DUPLICATE_EXACT for each subsequent row
    ELSE:
      ALL rows in group: duplicate_flag = "SUSPECT"
      Log DUPLICATE_SUSPECT for each row, with list of differing columns
```

**[SPEC]** Comparison column exclusion list — minimal:

The only columns excluded from comparison are those that are unique by definition:

```python
DUPLICATE_EXCLUDE_COLS = {
    "row_id",       # Unique by definition: {sheet_index}_{source_row}
    "source_row",   # Unique by definition: original Excel row number
}
```

**[SPEC]** Everything else from Module 1 participates in comparison. This includes:

- `row_quality` and `row_quality_details` — these are M1 output fields. If M1 assigned different quality grades to two rows with the same (family, ind, sheet), the rows differ and are SUSPECT.
- All `_raw` fields (`document_raw`, `visa_global_raw`, `date_diffusion_raw`, `ind_raw`, all `{APPROVER}_date_raw`, `{APPROVER}_statut_raw`, etc.) — these are M1 output fields. If `visa_global_raw` differs between two rows (e.g., "vao" vs "VAO") but `visa_global` is identical ("VAO" after normalization), the rows are still SUSPECT because the raw value differs. This is the conservative, spec-correct behavior: the spec says "any field differs" → SUSPECT.
- `source_sheet` — same by definition within the group (it is part of the grouping key).
- `document`, `ind` — same by definition within the group (family key derives from document, and ind is part of the grouping key).

**Implementation note on M2 columns:** Step 5 runs before Steps 3/4/6, so the M2-derived columns (`doc_version_key`, `previous_version_key`, `is_latest`, `revision_count`, `is_cross_lot`, `cross_lot_sheets`, `duplicate_flag`) do not yet exist in the DataFrame. They do not need explicit exclusion — they are simply not present. However, `doc_family_key` and `ind_sort_order` **do** exist (computed in Steps 1 and 2). Since `doc_family_key` is deterministically derived from `document` (which is already in the comparison set) and `ind_sort_order` is deterministically derived from `ind` (also already in the comparison set), including them in comparison is harmless — they will always agree when their source fields agree. For clarity and minimal coupling, the implementation should dynamically exclude any column present in the DataFrame that is known to be M2-derived:

```python
# At runtime, exclude M2-derived columns that exist at Step 5 execution time
M2_DERIVED_COLS_STEP5 = {"doc_family_key", "ind_sort_order"}
# Full exclusion = DUPLICATE_EXCLUDE_COLS ∪ M2_DERIVED_COLS_STEP5
```

**"Subsequent" ordering:** Within a group of identical rows, the first row by `source_row` ascending is UNIQUE; the rest are DUPLICATE. This ensures deterministic assignment.

**[SPEC]** Null comparison: Two null values in the same column are considered equal. If both rows have no `date_diffusion`, that is not a difference. This is consistent with the spec's intent: "all fields identical" means matching values including matching nulls.

**[SPEC]** DUPLICATE rows are flagged but NEVER deleted.

**V2 change from V1:** The exclusion list is now minimal — only `row_id` and `source_row`. V1 incorrectly excluded `row_quality` and `row_quality_details`, which are M1 output fields and must participate per the spec's "any field differs" rule. Raw fields are explicitly noted as participants. M2-derived columns present at Step 5 time are handled via a runtime set rather than a static exclusion list.

### Step 3: Family Grouping & Chain Linking

**[SPEC]** Spec reference: V2.2 §2.4 Step 3.

**Scope:** Group by `(doc_family_key, source_sheet)`.

**Precondition:** `duplicate_flag` already assigned (Step 5).

**Input:** Master DataFrame with `doc_family_key`, `ind`, `ind_sort_order`, `duplicate_flag`.

**Output:** New columns: `doc_version_key`, `previous_version_key`, `is_latest`, `revision_count`.

**[SPEC]** Algorithm:

```
For each group (doc_family_key, source_sheet):

  1. Sort rows by ind_sort_order ASC, then source_row ASC (tie-break)

  2. Build doc_version_key for each row:
     [SPEC] Format: {doc_family_key}::{ind}::{source_sheet}
     IF ind IS NOT null:
       doc_version_key = doc_family_key + "::" + ind + "::" + source_sheet
     ELSE:
       [SAFEGUARD] The spec does not define a string representation for null
       IND in key construction. NULL_IND_LABEL ("NULL") is chosen to produce
       unambiguous, readable keys and avoid empty segments ("::::").
       doc_version_key = doc_family_key + "::" + NULL_IND_LABEL + "::" + source_sheet

     IMPORTANT: doc_version_key identifies the VERSION, not the ROW.
     When duplicate rows exist (same family, same ind, same sheet),
     they share the SAME doc_version_key. This is correct per spec:
     they represent the same version. The row_id (from Module 1)
     remains the row-level unique identifier.

     Do NOT add disambiguation suffixes (e.g., row index) to
     doc_version_key. The spec defines the format as
     {doc_family_key}::{ind}::{source_sheet} — nothing more.

  3. Build revision chain (linked list):
     Walk sorted distinct ind_sort_order values within the group:
       - First distinct ind_sort_order: previous_version_key = null
       - Each subsequent: previous_version_key = doc_version_key
         of the previous distinct ind_sort_order

     All rows sharing an ind_sort_order get the same
     previous_version_key (they are the same version).

  4. Determine is_latest:
     The row(s) with the HIGHEST ind_sort_order in this group
     get is_latest = true.
     All others: is_latest = false.

     If multiple rows share the highest ind_sort_order (duplicates):
       ALL of them get is_latest = true (they all represent
       the latest revision).

  5. Compute revision_count:
     Count of DISTINCT ind values in this (family, sheet) group.
     Same value for every row in the group.
     Null IND counts as one distinct value (represented by sort_order = 0).
```

**[SPEC]** doc_version_key format: `{doc_family_key}::{ind}::{source_sheet}`

- Example: `P17T2INEXEVTPTERI001MTDTZFD026001::A::LOT 06-02-MET`
- For null IND: `P17T2INEXEVTPTERI001MTDTZFD026001::NULL::LOT 06-02-MET` — **[SAFEGUARD]** uses `NULL_IND_LABEL` constant; the spec does not define a string representation for null IND in keys
- For UNPARSEABLE: `UNPARSEABLE::a1b2c3d4e5f6g7h8::NULL::LOT 42-PLB-UTB`

**Implementation:** `groupby(['doc_family_key', 'source_sheet'])` then sorted transform. Within each group, identify distinct `ind_sort_order` values, map each to its predecessor's `doc_version_key`, then broadcast back to all rows. Use `transform('max')` for `is_latest`. Use `nunique()` for `revision_count`.

**V2 changes from V1:**
1. `doc_version_key` is no longer described as "globally unique." It identifies the version, not the row. `row_id` is the row-level unique identifier. (Correction #1)
2. Null IND uses the `NULL_IND_LABEL` constant instead of a magic string `"NULL"`. (Correction #8)
3. Chain linking logic no longer distinguishes between UNIQUE/SUSPECT/DUPLICATE for `previous_version_key` assignment — all rows at a given `ind_sort_order` get the same `previous_version_key` because they share the same `doc_version_key`. The V1 language about "DUPLICATE rows inheriting from their UNIQUE counterpart" was correct in effect but misleadingly complex. (Simplification)

### Step 4: Cross-Lot Detection

**[SPEC]** Spec reference: V2.2 §2.4 Step 4.

**Scope:** Group by `doc_family_key` across all sheets.

**Input:** Master DataFrame with `doc_family_key`, `source_sheet`.

**Output:** New columns: `is_cross_lot`, `cross_lot_sheets`.

**[SPEC]** Algorithm:

```
For each doc_family_key:
  sheets = distinct source_sheet values for this family

  IF len(sheets) > 1:
    is_cross_lot = true
    cross_lot_sheets = sorted list of sheet names
  ELSE:
    is_cross_lot = false
    cross_lot_sheets = null
```

**[SPEC] GP2 enforcement on `cross_lot_sheets`:** When `is_cross_lot = false`, `cross_lot_sheets` MUST be `null`. Never an empty list `[]`. Never an empty string `""`. This applies in all output formats:

- JSON: `null` (not `[]`)
- CSV: empty cell (not `[]` or `""`)
- Excel: empty cell

**[SPEC]** Key property: Cross-lot detection is informational only. Revision chains remain **independent per sheet**. A document appearing in LOT 06 and LOT 42 has two separate chains with independent `is_latest`, `previous_version_key`, and `revision_count`.

**[SPEC]** UNPARSEABLE rows: Each has a unique `doc_family_key`, so they will always have `is_cross_lot = false` and `cross_lot_sheets = null`. This is correct by design.

**Implementation:** `groupby('doc_family_key')['source_sheet'].transform(lambda x: x.nunique() > 1)` for `is_cross_lot`. A separate grouped aggregation builds the `cross_lot_sheets` lists, then merged back. A final assertion verifies: `df.loc[~df['is_cross_lot'], 'cross_lot_sheets'].isna().all()` — no non-cross-lot row has a non-null `cross_lot_sheets`.

**V2 change from V1:** Added explicit GP2 enforcement language and serialization rules for the `cross_lot_sheets = null` case. V1 stated this correctly in the algorithm but the enforcement details are now explicit. (Correction #7)

### Step 6: Anomaly Detection

**[SPEC]** Spec reference: V2.2 §2.4 Step 6.

**Scope:** Group by `(doc_family_key, source_sheet)`. Operates on the fully enriched DataFrame.

**Input:** All M2-enriched columns plus `date_diffusion` from M1.

**Output:** Entries appended to the linking anomalies log.

**[SPEC]** Anomaly types and detection logic:

| Anomaly | Detection | Logged Fields |
|---------|-----------|---------------|
| **REVISION_GAP** | Within a (family, sheet) group, consecutive `ind_sort_order` values jump by more than 1. E.g., A (1) then C (3). | family_key, sheet, gap between (e.g., "A→C"), missing sort orders |
| **LATE_FIRST_APPEARANCE** | First `ind_sort_order` in the (family, sheet) group is > 1. E.g., first IND is D (sort_order=4). | family_key, sheet, first_ind, first_sort_order |
| **DATE_REGRESSION** | Within a (family, sheet) group sorted by `ind_sort_order`, a later revision has a strictly earlier `date_diffusion` than a previous revision. Only compared between non-null dates. | family_key, sheet, earlier_ind, earlier_date, later_ind, later_date |
| **DUPLICATE_EXACT** | (Logged in Step 5.) Two+ rows with same (family, ind, sheet) and identical data. | family_key, sheet, ind, row_ids |
| **DUPLICATE_SUSPECT** | (Logged in Step 5.) Two+ rows with same (family, ind, sheet) but differing data. | family_key, sheet, ind, row_ids, differing_columns |
| **MISSING_IND** | (Logged in Step 2.) Row has null IND. | family_key, sheet, row_id, source_row |
| **UNPARSEABLE_DOCUMENT** | (Logged in Step 1.) Row has null document. | row_id, source_sheet, source_row, document_raw |

**[SPEC]** REVISION_GAP detection algorithm:

```
For each (family, sheet) group:
  Get sorted distinct ind_sort_order values (excluding 0 / null-IND rows)
  For each consecutive pair (prev_order, curr_order):
    IF curr_order - prev_order > 1:
      Log REVISION_GAP
      Include: prev_order, curr_order, the jump size
```

One simple rule. The sort order is already normalized in Step 2 — the detection does not need to know whether the original indices were alpha, numeric, single-letter, or multi-letter. It just compares consecutive integer sort orders.

**[SPEC]** Chain linking behavior on gaps: When a gap is detected (e.g., A at 1, C at 3, B missing), the chain links A→C directly. The gap is logged but does not break the chain.

**[SPEC]** LATE_FIRST_APPEARANCE detection:

```
For each (family, sheet) group:
  first_sort_order = min(ind_sort_order values, excluding 0 / null-IND)
  IF first_sort_order > 1:
    Log LATE_FIRST_APPEARANCE with first_ind and first_sort_order
```

**[SPEC]** DATE_REGRESSION detection:

```
For each (family, sheet) group:
  Sort by ind_sort_order ASC
  Keep only rows where date_diffusion IS NOT null
  For each consecutive pair (row_i, row_j) where j immediately follows i:
    IF row_j.date_diffusion < row_i.date_diffusion:
      Log DATE_REGRESSION
```

Compare consecutive non-null dates only, not all pairs.

**[SPEC]** REVISION_GAP and LATE_FIRST_APPEARANCE only fire for families with valid documents (not UNPARSEABLE). UNPARSEABLE families are single-row by design and cannot have revision sequences.

**V2 change from V1:** REVISION_GAP detection is simplified to one universal rule: consecutive `ind_sort_order` jump > 1. V1 had branching logic for alpha-derived vs numeric vs multi-letter indices, which was unnecessary complexity — the sort order is already normalized to integers. (Correction #4)

---

## 4. Internal Data Structures

### 4.1 doc_family_key Construction

```python
# Stored as: str column in DataFrame
# Primary path: document.str.replace("_", "")
# Fallback path: UNPARSEABLE_PREFIX + sha256_hex_16

# No separate data structure needed. The key is a derived column
# computed once and used for all subsequent groupby operations.
```

**Unchanged from V1.**

### 4.2 Revision Chain (Linked List per Family-Sheet)

The chain is represented **implicitly** via the `previous_version_key` column — each row points to its predecessor's version. There is no separate linked-list object. The chain can be reconstructed by following `previous_version_key` pointers.

```python
# For a family with revisions A, B, D in LOT 06:
# Row A: previous_version_key = null,            is_latest = false
# Row B: previous_version_key = "...::A::LOT 06", is_latest = false
# Row D: previous_version_key = "...::B::LOT 06", is_latest = true
# (gap at C logged as REVISION_GAP, but chain links B→D directly)

# For duplicates — two rows at IND B:
# Row B (source_row 50): previous_version_key = "...::A::LOT 06", duplicate_flag = UNIQUE
# Row B (source_row 72): previous_version_key = "...::A::LOT 06", duplicate_flag = DUPLICATE
# Both share doc_version_key = "...::B::LOT 06" — same version, different rows.
# row_id distinguishes them.
```

**V2 change from V1:** Added duplicate example to clarify that `doc_version_key` is shared and `row_id` distinguishes rows. (Correction #1)

### 4.3 Duplicate Detection Groups

```python
@dataclass
class DuplicateGroup:
    doc_family_key: str
    ind: Optional[str]
    source_sheet: str
    row_ids: List[str]           # All row_ids in this group
    flag: str                    # "UNIQUE" | "DUPLICATE" | "SUSPECT"
    differing_columns: Optional[List[str]]  # Only for SUSPECT
```

This is a transient structure used during Step 5 processing. Not persisted — results are written to the `duplicate_flag` column and the anomaly log.

**Unchanged from V1.**

### 4.4 Cross-Lot Map

```python
# Transient dict built during Step 4:
# { doc_family_key: sorted_list_of_sheets }
# Used to populate is_cross_lot and cross_lot_sheets columns.
# Discarded after column assignment.
#
# When len(sheets) == 1: cross_lot_sheets = None (null), NOT empty list.
```

**V2 change from V1:** Added explicit comment about null (not empty list) for single-sheet families. (Correction #7)

### 4.5 Anomaly Log Entry

```python
@dataclass
class LinkingAnomalyEntry:
    anomaly_id: str              # Auto-incremented
    anomaly_type: str            # One of the 7 types
    doc_family_key: str
    source_sheet: Optional[str]
    row_id: Optional[str]        # Specific row (for MISSING_IND, UNPARSEABLE)
    ind: Optional[str]           # Relevant IND value
    severity: str                # WARNING (all M2 anomalies are WARNING)
    details: dict                # Type-specific payload (see below)
```

**Details payload by anomaly type:**

| Type | Details Keys |
|------|-------------|
| REVISION_GAP | `prev_sort_order`, `curr_sort_order`, `jump_size` |
| LATE_FIRST_APPEARANCE | `first_ind`, `first_sort_order` |
| DATE_REGRESSION | `earlier_ind`, `earlier_date`, `later_ind`, `later_date` |
| DUPLICATE_EXACT | `row_ids`, `duplicate_count` |
| DUPLICATE_SUSPECT | `row_ids`, `differing_columns` |
| MISSING_IND | `source_row`, `document_raw` |
| UNPARSEABLE_DOCUMENT | `source_row`, `document_raw` |

**V2 change from V1:** REVISION_GAP details payload simplified — now uses `prev_sort_order`, `curr_sort_order`, `jump_size` instead of trying to reconstruct missing index letters. (Correction #4)

### 4.6 Module2Context

```python
@dataclass
class Module2Context:
    output_dir: str
    anomaly_log: List[LinkingAnomalyEntry] = field(default_factory=list)
    family_count: int = 0
    cross_lot_count: int = 0
    unparseable_count: int = 0
    duplicate_exact_count: int = 0
    duplicate_suspect_count: int = 0

    def log_anomaly(self, entry: LinkingAnomalyEntry) -> None:
        self.anomaly_log.append(entry)

    def next_anomaly_id(self) -> str:
        return str(len(self.anomaly_log) + 1)
```

**Unchanged from V1.**

---

## 5. Output Schemas

### 5.1 Enriched Master Dataset

**[SPEC]** All Module 1 columns are preserved unchanged, plus these new columns added by Module 2:

| Column | Type | Nullable? | Description |
|--------|------|-----------|-------------|
| `doc_family_key` | str | No | Stable family identifier. Flat uppercase string or `UNPARSEABLE::{hash}` |
| `doc_version_key` | str | No | **[SPEC]** Version identifier: `{doc_family_key}::{ind}::{source_sheet}`. **Not unique per row** — duplicate rows share the same `doc_version_key`. Use `row_id` for row-level identification. **[SAFEGUARD]** When `ind` is null, `NULL_IND_LABEL` ("NULL") is substituted. |
| `ind_sort_order` | int | No | Numeric sort position. A=1, B=2, ..., Z=26, AA=27. Null IND = 0. |
| `previous_version_key` | str or null | Yes | Previous revision's `doc_version_key` in same (family, sheet). Null for first. |
| `is_latest` | bool | No | True if highest `ind_sort_order` for this (family, sheet). |
| `revision_count` | int | No | Distinct IND values for this (family, sheet). |
| `is_cross_lot` | bool | No | True if family appears in multiple sheets. |
| `cross_lot_sheets` | list[str] or null | Yes | Sorted sheet names if `is_cross_lot = true`. Strictly `null` when `is_cross_lot = false` — never `[]` or `""` (GP2). |
| `duplicate_flag` | str | No | `UNIQUE` / `DUPLICATE` / `SUSPECT` |

**Serialization (per GP4):**

- JSON: `cross_lot_sheets` as JSON array or `null` (never `[]` when not cross-lot). Booleans as true/false. Nulls preserved.
- CSV: `cross_lot_sheets` as semicolon-separated string or empty cell (never `[]` or `""` string literal). Booleans as `true`/`false`. Nulls as empty cells.
- Excel: `cross_lot_sheets` as semicolon-separated string or empty cell. Booleans as `TRUE`/`FALSE`.

**V2 changes from V1:**
1. `doc_version_key` description no longer says "globally unique" — explicitly notes it is shared by duplicate rows. (Correction #1)
2. `cross_lot_sheets` description explicitly states "strictly null when not cross-lot" with GP2 reference. Serialization rules reinforce this. (Correction #7)

### 5.2 Document Family Index

**[SPEC]** One row per `(doc_family_key, source_sheet)` pair.

| Column | Type | Description | Sourcing Rule |
|--------|------|-------------|---------------|
| `doc_family_key` | str | Family identifier | Group key |
| `source_sheet` | str | Sheet (lot) | Group key |
| `lot` | str or null | Lot code | **[IMPLEMENTATION — deterministic by GP1]** From the row with the highest `ind_sort_order` in the (family, sheet) group. If multiple rows share that sort order (duplicates), take the row with the lowest `source_row`. |
| `first_ind` | str or null | Earliest IND | **[IMPLEMENTATION — deterministic by GP1]** `ind` value of the row with the lowest `ind_sort_order` in the group. Tie-break: lowest `source_row`. |
| `latest_ind` | str or null | Latest IND | **[IMPLEMENTATION — deterministic by GP1]** `ind` value of the row with the highest `ind_sort_order` in the group. Tie-break: lowest `source_row`. |
| `revision_count` | int | Distinct IND values | **[SPEC]** Same as `revision_count` on the enriched rows (count of distinct `ind` values in group). |
| `latest_visa` | str or null | VISA status of latest revision | **[IMPLEMENTATION — deterministic by GP1]** Collect `visa_global` from all rows where `is_latest = true` in this group. If all `is_latest` rows have the same `visa_global` value (including all null), use that value. If they have different `visa_global` values, use `null`. |
| `has_revision_gap` | bool | Gap detected | **[SPEC]** `true` if any REVISION_GAP anomaly was logged for this (family, sheet). |
| `is_cross_lot` | bool | Multi-sheet family | **[SPEC]** Same as `is_cross_lot` on the enriched rows. |

These tie-breaking rules are not explicitly defined in V2.2 §2.3. They are implementation decisions to ensure deterministic output per GP1. The spec defines the columns and their meaning but does not prescribe how to resolve ambiguity when multiple rows compete for a single summary value.

**Observability note on `latest_visa`:** When `latest_visa` is null, the cause is not distinguishable from the family index alone. Null can mean either "no visa issued yet" (all `is_latest` rows have `visa_global = null`) or "conflicting visa values across duplicate/suspect rows." To determine which, check the enriched master dataset for the `is_latest = true` rows of that family-sheet group. No additional column is needed — this is a diagnostic path, not a data model change.

**Row count estimate:** ~2,518 unique documents across ~25 sheets, but many documents appear in only 1 sheet. With 993 cross-lot documents, expect roughly 3,000–4,000 rows in the family index.

**V2 change from V1:** All columns now have explicit deterministic sourcing rules. V1 used vague language ("null if ambiguous/SUSPECT") for `lot` and `latest_visa`. The `latest_visa` rule is now precise: same value → use it; different values → null. The `lot` and `first_ind`/`latest_ind` rules now specify tie-breaking by `source_row` ascending. (Correction #5)

**V2.1 change from V2:** Sourcing rules for `lot`, `first_ind`, `latest_ind`, `latest_visa` relabelled from [SPEC] to [IMPLEMENTATION — deterministic by GP1]. These are implementation decisions, not explicit spec rules. Added observability note for `latest_visa` null ambiguity. (Corrections V2.1 #2, #4)

### 5.3 Linking Anomalies Log

| Column | Type | Description |
|--------|------|-------------|
| `anomaly_id` | str | Auto-incremented |
| `anomaly_type` | str | One of the 7 defined types |
| `doc_family_key` | str | Affected family |
| `source_sheet` | str or null | Sheet context (null for cross-lot anomalies if applicable) |
| `row_id` | str or null | Specific row (for row-level anomalies) |
| `ind` | str or null | Relevant IND |
| `severity` | str | Always `WARNING` for M2 anomalies |
| `details` | dict (JSON) / str (CSV) | Type-specific payload |

**Unchanged from V1.**

---

## 6. Folder & File Structure

Extension of the existing Module 1 project layout:

```
jansa_visasist/
├── __init__.py
├── main.py                          # M1 entry point (unchanged)
├── main_m2.py                       # M2 entry point (NEW)
├── context.py                       # M1 context (unchanged)
├── context_m2.py                    # M2 context (NEW)
├── config.py                        # M1 config (unchanged)
├── config_m2.py                     # M2 config — constants, exclude lists (NEW)
├── models/
│   ├── __init__.py                  # Add new exports
│   ├── enums.py                     # Add DuplicateFlag, AnomalyType enums (EXTEND)
│   ├── log_entry.py                 # M1 import log entry (unchanged)
│   ├── header_report.py             # M1 header report (unchanged)
│   └── linking_anomaly.py           # LinkingAnomalyEntry dataclass (NEW)
├── pipeline/
│   ├── __init__.py
│   ├── [M1 pipeline files]          # All unchanged
│   └── m2/                          # Module 2 pipeline (NEW directory)
│       ├── __init__.py
│       ├── family_key.py            # Step 1: doc_family_key construction
│       ├── ind_normalization.py     # Step 2: ind_sort_order computation
│       ├── duplicate_detection.py   # Step 5: UNIQUE/DUPLICATE/SUSPECT
│       ├── chain_linking.py         # Step 3: family grouping, chain, is_latest
│       ├── cross_lot.py             # Step 4: cross-lot detection
│       └── anomaly_detection.py     # Step 6: revision gaps, date regression, etc.
├── outputs/
│   ├── __init__.py
│   ├── [M1 output writers]          # All unchanged
│   ├── m2_json_writer.py            # M2 JSON output (NEW)
│   ├── m2_csv_writer.py             # M2 CSV output (NEW)
│   └── m2_excel_writer.py           # M2 Excel output (NEW)
├── tests/
│   ├── __init__.py
│   ├── [M1 test files]              # All unchanged
│   ├── test_family_key.py           # (NEW)
│   ├── test_ind_normalization.py    # (NEW)
│   ├── test_duplicate_detection.py  # (NEW)
│   ├── test_chain_linking.py        # (NEW)
│   ├── test_cross_lot.py            # (NEW)
│   ├── test_anomaly_detection.py    # (NEW)
│   ├── test_m2_integration.py       # (NEW)
│   └── golden/
│       ├── [M1 golden files]
│       └── golden_m2_snapshot.json   # (NEW)
└── requirements.txt                  # No changes needed
```

**Design decisions:**

- M2 pipeline steps live in `pipeline/m2/` subdirectory to avoid polluting M1's namespace.
- M2 output writers are prefixed `m2_` and live alongside M1 writers for consistency.
- M2 tests follow the same naming pattern as M1 (one test file per pipeline step).
- `config_m2.py` keeps M2-specific constants separate from M1 config.
- `main_m2.py` is a separate entry point that loads M1 output and runs M2. This allows running M2 independently or chaining M1 → M2 in a combined pipeline.

### 6.1 Output Directory Structure

```
output/
├── [M1 outputs]                      # Unchanged
│   ├── master_dataset.json
│   ├── master_dataset.csv
│   ├── master_dataset.xlsx
│   ├── import_log.json
│   ├── import_log.csv
│   ├── header_mapping_report.json
│   └── validation_report.json
├── enriched_master_dataset.json      # M2 Output 1 (NEW)
├── enriched_master_dataset.csv       # M2 Output 1 (NEW)
├── enriched_master_dataset.xlsx      # M2 Output 1 (NEW — optional audit)
├── document_family_index.json        # M2 Output 2 (NEW)
├── document_family_index.csv         # M2 Output 2 (NEW)
├── linking_anomalies.json            # M2 Output 3 (NEW)
└── linking_anomalies.csv             # M2 Output 3 (NEW)
```

**Unchanged from V1.**

---

## 7. config_m2.py — Constants

```python
# ──────────────────────────────────────────────────
# Duplicate comparison exclusions
# ──────────────────────────────────────────────────

# Duplicate comparison rule: see §3 Step 5 for the authoritative definition.
# Only columns that are unique by definition are excluded.
# Everything else from M1 participates (per Step 5 comparison rule).
DUPLICATE_EXCLUDE_COLS = {
    "row_id",       # Unique by definition: {sheet_index}_{source_row}
    "source_row",   # Unique by definition: original Excel row number
}

# [SAFEGUARD] M2-derived columns present at Step 5 execution time.
# Excluded at runtime because they are deterministically derived from
# columns already in the comparison set. See §3 Step 5 for rationale.
M2_DERIVED_COLS_STEP5 = {
    "doc_family_key",   # Derived from document
    "ind_sort_order",   # Derived from ind
}

# ──────────────────────────────────────────────────
# Anomaly type constants
# ──────────────────────────────────────────────────

ANOMALY_REVISION_GAP = "REVISION_GAP"
ANOMALY_LATE_FIRST = "LATE_FIRST_APPEARANCE"
ANOMALY_DATE_REGRESSION = "DATE_REGRESSION"
ANOMALY_DUPLICATE_EXACT = "DUPLICATE_EXACT"
ANOMALY_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"
ANOMALY_MISSING_IND = "MISSING_IND"
ANOMALY_UNPARSEABLE = "UNPARSEABLE_DOCUMENT"

# ──────────────────────────────────────────────────
# Key construction constants
# ──────────────────────────────────────────────────

# [SPEC] Prefix for fallback keys when document is null
UNPARSEABLE_PREFIX = "UNPARSEABLE::"

# [SAFEGUARD] Hash truncation length (hex chars) for UNPARSEABLE keys
HASH_TRUNCATE_LENGTH = 16

# [SAFEGUARD] Label used in doc_version_key when ind is null.
# The spec does not define a string representation for null IND in key
# construction. "NULL" is chosen to produce unambiguous, readable keys
# and avoid empty segments ("::::").
NULL_IND_LABEL = "NULL"

# ──────────────────────────────────────────────────
# Input contract
# ──────────────────────────────────────────────────

# [SAFEGUARD] Required M1 columns for M2 to proceed
M2_REQUIRED_COLUMNS = {"document", "ind", "source_sheet", "source_row", "row_id"}

# [SAFEGUARD] Optional M1 columns (degrade gracefully if missing)
M2_OPTIONAL_COLUMNS = {"date_diffusion", "visa_global"}
```

**V2 changes from V1:**
1. `DUPLICATE_EXCLUDE_COLS` reduced to minimal set: only `row_id` and `source_row`. `row_quality` and `row_quality_details` removed — they are M1 fields and participate in comparison. (Correction #2)
2. `M2_DERIVED_COLS_STEP5` added as separate set for runtime exclusion of M2 columns present at Step 5. (Correction #2)
3. `NULL_IND_LABEL = "NULL"` added. (Correction #8)
4. Every constant labelled [SPEC] or [SAFEGUARD]. (Correction #6)

**V2.1 changes from V2:**
5. `NULL_IND_LABEL` relabelled from [SPEC] to [SAFEGUARD]. (Correction V2.1 #1)
6. `DUPLICATE_EXCLUDE_COLS` comment now cross-references §3 Step 5 as authoritative source. (Correction V2.1 #3)

---

## 8. main_m2.py — Entry Point Design

```
CLI interface:
  --input-master   Path to M1 master dataset (JSON or CSV)
  --output-dir     Directory for M2 outputs
  --log-level      DEBUG | INFO | WARNING | ERROR

Execution flow:
  1. Parse args
  2. Load M1 master dataset into DataFrame
  3. [SAFEGUARD] Validate contract (required columns check)
  4. Build Module2Context
  5. [SPEC] Execute pipeline:
     a. Step 1: family_key.build_family_keys(df, ctx)
     b. Step 2: ind_normalization.compute_sort_orders(df, ctx)
     c. Step 5: duplicate_detection.detect_duplicates(df, ctx)
     d. Step 3: chain_linking.link_chains(df, ctx)
     e. Step 4: cross_lot.detect_cross_lot(df, ctx)
     f. Step 6: anomaly_detection.detect_anomalies(df, ctx)
  6. Build document family index (deterministic sourcing rules)
  7. Write outputs (JSON, CSV, optional Excel)
  8. Print summary
```

**Combined pipeline option:** A future `main_combined.py` can chain M1 → M2 (and later → M3) without intermediate serialization, passing the DataFrame directly. This avoids JSON round-trip overhead. But for now, M2 reads M1 output from disk for clean separation and debuggability.

**V2 change from V1:** Steps labelled [SPEC] vs [SAFEGUARD]. (Correction #6)

---

## 9. Performance Considerations (GP5)

**[SPEC]** Target: Module 2 must contribute to the combined M1+M2+M3 budget of under 3 seconds for 5,000 rows.

**Expected M2 budget:** ~0.5–1.0 seconds for 5,000 rows (M1 takes ~1.5–2.0s, leaving room for M3).

**Strategy:**

- Steps 1 and 2 are row-level vectorized operations: O(n), negligible cost.
- Step 5 uses `groupby` + comparison: O(n) amortized for typical data (few duplicates). The comparison within groups is O(k * m) where k = group size and m = column count, but groups of size > 1 are rare.
- Step 3 uses `groupby` + sorted transform: O(n log n) for the sort, O(n) for the chain linking.
- Step 4 uses `groupby` + `nunique`: O(n).
- Step 6 uses `groupby` + sequential scan: O(n).

**No O(n^2) operations.** All steps are groupby-based with at most O(n log n) complexity.

**Unchanged from V1.**

---

## 10. Testing Strategy

### 10.1 Unit Tests (per step)

| Test File | Key Test Cases |
|-----------|---------------|
| `test_family_key.py` | Underscore format → correct flat key; compressed format → same flat key; both formats → identical key (the 88-pair test); null document → UNPARSEABLE key; two null documents → different keys; blank document_raw → UNPARSEABLE with empty string hash |
| `test_ind_normalization.py` | **[SPEC]** A→1, B→2, Z→26, AA→27; numeric "1"→1, "02"→2; null→0 + MISSING_IND log. **[SAFEGUARD]** mixed/unexpected format (e.g., "1A") → 0 + WARNING |
| `test_duplicate_detection.py` | Single row → UNIQUE; identical rows → first UNIQUE + rest DUPLICATE; differing rows → all SUSPECT; null-field comparison (both null = equal); DUPLICATE_EXACT and DUPLICATE_SUSPECT logged; **[V2]** rows differing only in `_raw` field → SUSPECT; rows differing only in `row_quality` → SUSPECT |
| `test_chain_linking.py` | A→B→C chain; gap A→C (link correctly, gap logged); single revision (is_latest=true, no previous); null IND at position 0 (uses NULL_IND_LABEL in key); **[V2]** duplicate rows share same doc_version_key; row_id distinguishes them |
| `test_cross_lot.py` | Single-sheet family → is_cross_lot=false, cross_lot_sheets=null (not []); multi-sheet family → true + sorted sheets; UNPARSEABLE → always false + null sheets |
| `test_anomaly_detection.py` | REVISION_GAP: sort_order jump > 1 between any consecutive pair; LATE_FIRST_APPEARANCE for first_sort_order > 1; DATE_REGRESSION for later revision with earlier date; no false positives on clean data |

**V2 changes from V1:**
1. `test_ind_normalization.py` separates spec cases from safeguard fallback test. (Correction #3, #6)
2. `test_duplicate_detection.py` adds cases for `_raw` field differences and `row_quality` differences producing SUSPECT. (Correction #2)
3. `test_chain_linking.py` adds test that duplicate rows share `doc_version_key` and that `NULL_IND_LABEL` is used. (Corrections #1, #8)
4. `test_cross_lot.py` explicitly tests `cross_lot_sheets = null` (not `[]`) when not cross-lot. (Correction #7)
5. `test_anomaly_detection.py` uses simplified gap detection (sort_order jump > 1, no branching on index type). (Correction #4)

### 10.2 Integration Test

`test_m2_integration.py`:

- Load the actual M1 output (`output/master_dataset.json`)
- Run full M2 pipeline
- Verify: all 4,108 rows have all M2 columns populated
- Verify: 88 naming-format pairs resolve to same `doc_family_key`
- Verify: exactly 1 UNPARSEABLE row (LOT 42, row 244)
- Verify: UNPARSEABLE row has `is_cross_lot = false` and `cross_lot_sheets` is null (not `[]`)
- Verify: `doc_version_key` format matches `{family}::{ind}::{sheet}` — NOT expected to be unique per row
- Verify: `row_id` is unique per row (from M1, unchanged by M2)
- Verify: `is_latest` has exactly one `true` per non-duplicate (family, sheet) group at the highest `ind_sort_order`
- Verify: all `cross_lot_sheets` values are null when `is_cross_lot = false`
- Verify: anomaly log contains no unexpected types
- Verify: determinism (run twice, compare outputs byte-for-byte)
- Verify: Document Family Index `latest_visa` is deterministic — identical across repeated runs
- Verify: review SUSPECT count against expectations. If SUSPECT count is unexpectedly high, inspect whether differences are driven by `row_quality`/`row_quality_details` or `_raw` field variations. This is not a bug — it reflects real data variation per the Step 5 comparison rule — but should be reviewed during first integration run to calibrate expectations.

**V2.1 change from V2:** Added SUSPECT count validation item. (Correction V2.1 #5)

**V2 changes from V1:**
1. Removed assertion that `doc_version_key` is globally unique — it is not. (Correction #1)
2. Added `row_id` uniqueness check instead. (Correction #1)
3. Added `cross_lot_sheets` null enforcement check. (Correction #7)
4. Added `latest_visa` determinism check. (Correction #5)

### 10.3 Golden Snapshot

Generate a golden snapshot of M2 outputs from the current GrandFichier for regression testing.

**Unchanged from V1.**

---

## 11. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| M1 output format changes | Medium | High | **[SAFEGUARD]** Contract validation at M2 entry. Explicit required-column check. |
| New IND formats (e.g., "1A", "B2") | Low | Medium | **[SAFEGUARD]** Fallback to sort_order = 0 + WARNING log. Never crash. Clearly labelled as non-spec behavior. |
| Large duplicate groups (>10 rows same family/ind/sheet) | Low | Low | Comparison is column-count bounded, not row-count quadratic. |
| UNPARSEABLE hash collision | Negligible | Low | **[SAFEGUARD]** 16 hex chars = 64 bits. For <100K rows, collision probability < 10^-14. |
| Cross-lot families with conflicting data | Medium | Low | **[SPEC]** M2 does not merge cross-lot data. Each sheet is independent. Flag only. |
| New M1 columns affect duplicate comparison | Medium | Low | **[SPEC]** Minimal exclusion list means new M1 columns automatically participate in comparison (conservative default — correct per spec's "any field differs" rule). |
| `_raw` field differences inflate SUSPECT count | Medium | Low | **[SPEC]** Correct behavior — raw fields are M1 output and must participate. If the SUSPECT count is higher than expected, it reflects real data variation, not a bug. |

**V2 changes from V1:**
1. All mitigations labelled [SPEC] or [SAFEGUARD]. (Correction #6)
2. "Future M1 columns" risk reframed: minimal exclusion list is now the design — new columns participate by default, which is correct. (Correction #2)
3. New risk added: `_raw` field differences inflating SUSPECT count. (Correction #2)

---

## 12. Success Criteria Checklist

Directly from V2.2 spec section 2.7, with implementation verification method:

| Criterion | Origin | Verification |
|-----------|--------|-------------|
| Every row has `doc_family_key` | **[SPEC]** | Assert no nulls in column |
| Every row has `doc_version_key` | **[SPEC]** | Assert no nulls in column. Assert format matches `{family}::{ind_or_NULL}::{sheet}`. Do NOT assert uniqueness — duplicates share keys. |
| Every row has `duplicate_flag` | **[SPEC]** | Assert all values in {UNIQUE, DUPLICATE, SUSPECT} |
| 88 naming pairs resolve to same family | **[SPEC]** | Test: collect all `doc_family_key` values for known pairs, assert set size = 1 per pair |
| Chains correctly ordered | **[SPEC]** | Test: for each (family, sheet), verify `ind_sort_order` is strictly non-decreasing along chain |
| `is_latest` correct per (family, sheet) | **[SPEC]** | Test: for each group, all rows at the maximum `ind_sort_order` have `is_latest = true`, all others have `false` |
| UNPARSEABLE rows produce unique keys | **[SPEC]** | Test: all keys starting with "UNPARSEABLE::" are distinct |
| UNPARSEABLE rows never group with valid documents | **[SPEC]** | Test: no `doc_family_key` starting with "UNPARSEABLE::" appears without that prefix elsewhere |
| UNPARSEABLE rows never group with each other | **[SPEC]** | Test: each UNPARSEABLE key appears in exactly 1 row |
| `cross_lot_sheets` null when not cross-lot | **[SPEC] GP2** | Test: `df.loc[~df['is_cross_lot'], 'cross_lot_sheets'].isna().all()` — never `[]` or `""` |
| All anomalies logged | **[SPEC]** | Test: anomaly log is non-empty; types are subset of defined 7 |
| Deterministic output | **[SPEC] GP1** | Test: run twice on same input, diff outputs byte-for-byte |
| Document Family Index `latest_visa` deterministic | **[IMPL GP1]** | Test: same value across repeated runs; verify sourcing rule (same value → use it, different → null) |
| `NULL_IND_LABEL` used consistently | **[SAFEGUARD]** | Test: all `doc_version_key` values with null IND contain "::NULL::", not "::None::" or "::::" |

**V2 changes from V1:**
1. `doc_version_key` criterion no longer asserts uniqueness. (Correction #1)
2. Added `cross_lot_sheets` null enforcement criterion. (Correction #7)
3. Added `latest_visa` determinism criterion. (Correction #5)
4. Added `NULL_IND_LABEL` consistency criterion. (Correction #8)
5. Every criterion labelled with origin. (Correction #6)

---

## Appendix A: V2 Correction Traceability Matrix

| Correction # | Description | Sections Modified |
|--------------|-------------|-------------------|
| 1. `doc_version_key` not globally unique | Clarified as version identifier, not row identifier. `row_id` is the row-level unique key. | §3 Step 3, §4.2, §5.1, §10.1, §10.2, §12 |
| 2. Minimal duplicate exclusion list | Only `row_id` and `source_row` excluded. `row_quality`, `row_quality_details`, all `_raw` fields participate. | §2.1, §3 Step 5, §7, §10.1, §11, §12 |
| 3. IND format — spec vs safeguard | Three spec cases separated from unexpected-format fallback. | §3 Step 2, §10.1 |
| 4. Simplified REVISION_GAP detection | One rule: consecutive `ind_sort_order` jump > 1. No branching on index type. | §3 Step 6, §4.5, §10.1 |
| 5. Deterministic Family Index sourcing | Exact rules for `lot`, `first_ind`, `latest_ind`, `latest_visa` with tie-breaking. | §5.2, §10.2, §12 |
| 6. Spec vs safeguard labelling | [SPEC] and [SAFEGUARD] tags throughout. | All sections |
| 7. `cross_lot_sheets` strictly null | Enforcement in algorithm, serialization, data structures, tests. | §3 Step 4, §4.4, §5.1, §10.1, §10.2, §12 |
| 8. `NULL_IND_LABEL` constant | Defined in config. Used in key construction. Tested in integration. | §3 Step 3, §7, §10.1, §12 |

## Appendix B: V2.1 Correction Traceability Matrix

| Correction # | Description | Sections Modified |
|--------------|-------------|-------------------|
| V2.1-1. `NULL_IND_LABEL` relabelled [SAFEGUARD] | The spec does not define a string literal for null IND in key construction. "NULL" is an implementation choice. | Notation, §3 Step 3, §5.1, §7, §12 |
| V2.1-2. Family Index tie-break rules relabelled [IMPLEMENTATION — deterministic by GP1] | V2.2 §2.3 defines columns and meaning but not tie-breaking. Rules are implementation decisions. | Notation, §5.2, §12 |
| V2.1-3. Duplicate comparison rule unified | §3 Step 5 is now the single authoritative statement. §2.1 and §7 cross-reference it. | §2.1, §3 Step 5, §7 |
| V2.1-4. `latest_visa` null observability note | Documents two distinct causes of null (no visa vs conflicting values) and diagnostic path. | §5.2 |
| V2.1-5. SUSPECT count validation | Integration test now includes review of SUSPECT count to calibrate expectations on first run. | §10.2 |
