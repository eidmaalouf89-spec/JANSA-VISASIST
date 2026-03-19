# JANSA VISASIST — Module 2: Technical Implementation Plan

**Data Model & Revision Linking**

Based on: V2.2 Production-Ready Specification

---

## 0. Scope & Constraints

This plan covers **Module 2 only**. It assumes Module 1 is fully implemented and produces the master dataset as specified (all columns, all quality grades, all approver blocks, `assigned_approvers`, `document`/`document_raw` pairs, `row_quality`, import log).

**Hard constraints:**

- Strictly follows V2.2 specification. No invented behavior.
- Deterministic: same input always produces same output.
- Simple and robust (MVP mindset). No UI, no external API.
- Respects GP1–GP5 global policies.
- The 88 naming-format pairs must resolve correctly via `doc_family_key` flattening.
- UNPARSEABLE rows must never group with valid documents or with each other.
- Duplicate detection (UNIQUE / DUPLICATE / SUSPECT) must be implemented before chain linking.
- Anomaly types are exactly: REVISION_GAP, LATE_FIRST_APPEARANCE, DATE_REGRESSION, DUPLICATE_EXACT, DUPLICATE_SUSPECT, MISSING_IND, UNPARSEABLE_DOCUMENT.

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

Module 2 runs as a **single-pass pipeline** over the Module 1 master dataset:

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

**Note on ordering:** Step 5 (duplicate detection) is executed **before** Step 3 (chain linking) per spec requirement: "Resolve duplicates in Step 5 before linking." Steps 1 and 2 are independent row-level transforms. Steps 3, 4, 6 are group-level operations that depend on 1, 2, 5.

---

## 2. Module 1 → Module 2 Interface Contract

### 2.1 What Module 2 Expects from Module 1

Module 2 consumes the Module 1 master dataset as a `pandas.DataFrame`. The following columns are **required** (consumed directly by Module 2 logic):

| Column | Type | Used In | Nullable? |
|--------|------|---------|-----------|
| `document` | str or None | Step 1 (family key) | Yes — null triggers UNPARSEABLE fallback |
| `document_raw` | str or None | Step 1 (fallback hash input) | Yes — preserved for traceability |
| `ind` | str or None | Step 2 (sort order), Step 3 (chain), Step 5 (dupes), Step 6 (anomalies) | Yes — null IND → sort_order = 0 |
| `lot` | str or None | Output (family index) | Yes |
| `source_sheet` | str | Step 1 (fallback hash), Step 3 (chain scope), Step 4 (cross-lot), Step 5 (dupe scope) | No — always populated |
| `source_row` | int | Step 1 (fallback hash) | No — always populated |
| `row_id` | str | Traceability | No — always populated |
| `date_diffusion` | str (ISO) or None | Step 6 (DATE_REGRESSION) | Yes |
| `visa_global` | str or None | Output (family index: latest visa) | Yes |
| `row_quality` | str | Passthrough (not modified by M2) | No |
| `assigned_approvers` | list[str] | Passthrough | No |
| All `{APPROVER}_statut` columns | str or None | Passthrough (consumed by M3) | Yes |

**Passthrough columns:** All Module 1 columns not listed above are carried through unchanged. Module 2 never modifies existing M1 columns — it only **adds** new columns.

### 2.2 Input Validation (Defensive)

Before processing, Module 2 performs a lightweight contract check:

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

**Rationale:** `document`, `ind`, `source_sheet`, `source_row`, `row_id` are structurally essential — without them Module 2 cannot construct keys, scope groups, or trace rows. `date_diffusion` and `visa_global` degrade gracefully because they only affect anomaly detection and summary fields.

### 2.3 How Null Document Values Trigger the UNPARSEABLE Fallback

The contract between M1 and M2 for `document = null` is:

1. **M1 is solely responsible** for deciding if a document reference is valid. M2 trusts that decision completely.
2. **If `document` is not null:** M2 treats it as a valid, normalized reference and flattens it into `doc_family_key` by stripping underscores.
3. **If `document` is null:** M2 executes the fallback path. This null arises from exactly two M1 scenarios:
   - `document_raw` was blank/whitespace (GP2 null policy, M1 Step 3)
   - `document_raw` was structurally invalid (M1 Step 7a validation rules R1–R4)
4. **M2 performs no additional validation** on the `document` field. No regex checks, no length checks, no noise-ratio checks. That was M1's job.

Fallback key construction:

```
input_string = str(document_raw) + "::" + str(source_sheet) + "::" + str(source_row)
hash_hex = sha256(input_string.encode('utf-8')).hexdigest()[:16]
doc_family_key = "UNPARSEABLE::" + hash_hex
```

The 16-character hex truncation gives 64 bits of collision space — more than sufficient for datasets under 100K rows. The hash is deterministic: same `(document_raw, source_sheet, source_row)` always produces the same key.

---

## 3. Data Pipeline Steps — Detailed Design

### Step 1: doc_family_key Construction

**Input:** Full master DataFrame.

**Output:** New column `doc_family_key` added to every row.

**Algorithm:**

```
For each row:
  IF document IS NOT null:
    doc_family_key = document.replace("_", "")
    # Already uppercase from M1 normalization
    # Result: P17T2INEXEVTPTERI001MTDTZFD026001
  ELSE:
    raw_str = "" if document_raw is None else str(document_raw)
    hash_input = raw_str + "::" + source_sheet + "::" + str(source_row)
    hash_hex = sha256(hash_input.encode('utf-8')).hexdigest()[:16]
    doc_family_key = "UNPARSEABLE::" + hash_hex
    Log UNPARSEABLE_DOCUMENT anomaly
```

**Key properties:**

- The 88 naming-format pairs (e.g., `P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001` and `P17T2INEXEVTPTERI001MTDTZFD026001`) produce **identical** `doc_family_key` values because both flatten to the same string after underscore removal.
- The `document` column is **never modified** — it stays human-readable with underscores. `doc_family_key` is a derived grouping key.
- Each UNPARSEABLE row gets a **unique** key (hash includes sheet + row). Two different unparseable documents never group together.

**Implementation:** Vectorized with `pandas.Series.where()` or `np.where()` for the primary path, then a loop over the (very few) null-document rows for the fallback path.

**Performance:** O(n) single pass. The fallback path (SHA-256 per row) only fires for null-document rows (currently 1 row).

### Step 2: Revision Index Normalization

**Input:** `ind` column from master DataFrame.

**Output:** New column `ind_sort_order` (integer).

**Algorithm:**

```
For each row:
  IF ind IS null:
    ind_sort_order = 0
    Log MISSING_IND anomaly (with row details)
  ELSE IF ind is purely numeric (e.g., "1", "02", "10"):
    ind_sort_order = int(ind)
  ELSE IF ind is alphabetic (single or multi-letter):
    Compute column-style index:
      A=1, B=2, ..., Z=26, AA=27, AB=28, ...
    ind_sort_order = computed value
  ELSE:
    # Unexpected format (e.g., mixed alphanumeric)
    ind_sort_order = 0
    Log WARNING with raw value
```

**Alpha-to-number function:**

```python
def alpha_to_sort_order(s: str) -> int:
    """Convert A=1, B=2, ..., Z=26, AA=27, AB=28, ..."""
    result = 0
    for char in s.upper():
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result
```

**Implementation:** Vectorized for the common alpha-only case (the vast majority of IND values are single letters A–Z). Numeric INDs parsed with `pd.to_numeric(..., errors='coerce')` first pass, then alpha conversion on the remainder.

**Edge case — null IND:** The spec says 13 rows have empty IND. These get `ind_sort_order = 0`, which places them **before** revision A in sort order. They still participate in chain linking and duplicate detection. Each null-IND row is logged as MISSING_IND.

### Step 5: Duplicate Detection (Executed Before Step 3)

**Scope:** Group by `(doc_family_key, ind, source_sheet)`.

**Input:** Master DataFrame with `doc_family_key` and `ind` columns.

**Output:** New column `duplicate_flag` on every row, with values: `UNIQUE`, `DUPLICATE`, or `SUSPECT`.

**Algorithm:**

```
For each group (doc_family_key, ind, source_sheet):
  IF group has exactly 1 row:
    duplicate_flag = "UNIQUE"
  ELSE (2+ rows):
    Define comparison columns = ALL columns EXCEPT:
      - row_id, source_row (unique by definition)
      - row_quality, row_quality_details (derived, may differ)
      - doc_family_key, doc_version_key, ind_sort_order (derived by M2)

    Pick first row as reference.
    Compare all other rows against reference on comparison columns.

    IF all rows are identical on comparison columns:
      First row: duplicate_flag = "UNIQUE"
      Subsequent rows: duplicate_flag = "DUPLICATE"
      Log DUPLICATE_EXACT for each subsequent row
    ELSE:
      ALL rows in group: duplicate_flag = "SUSPECT"
      Log DUPLICATE_SUSPECT for each row, with list of differing columns
```

**"Subsequent" ordering:** Within a group of identical rows, the first row by `source_row` ascending is UNIQUE; the rest are DUPLICATE. This ensures deterministic assignment.

**Comparison column exclusion list (explicit):**

```python
DUPLICATE_EXCLUDE_COLS = {
    "row_id", "source_row", "row_quality", "row_quality_details",
    "doc_family_key", "doc_version_key", "ind_sort_order",
    "previous_version_key", "is_latest", "revision_count",
    "is_cross_lot", "cross_lot_sheets", "duplicate_flag",
}
```

These are either unique-by-definition (row_id, source_row) or M2-derived columns that don't exist yet at comparison time. Everything else — including `document`, `document_raw`, `titre`, all dates, all approver statuses, `visa_global`, `lot`, `observations` — participates in comparison.

**Null comparison:** Two null values in the same column are considered equal (consistent with pandas `equals()` semantics for this purpose). This is correct: if both rows have no `date_diffusion`, that is not a difference.

**DUPLICATE rows are flagged but NEVER deleted** (per spec).

### Step 3: Family Grouping & Chain Linking

**Scope:** Group by `(doc_family_key, source_sheet)`.

**Precondition:** `duplicate_flag` already assigned (Step 5).

**Input:** Master DataFrame with `doc_family_key`, `ind`, `ind_sort_order`, `duplicate_flag`.

**Output:** New columns: `doc_version_key`, `previous_version_key`, `is_latest`, `revision_count`.

**Algorithm:**

```
For each group (doc_family_key, source_sheet):

  1. Sort rows by ind_sort_order ASC, then source_row ASC (tie-break)

  2. Build doc_version_key for each row:
     doc_version_key = doc_family_key + "::" + str(ind or "NULL") + "::" + source_sheet

     NOTE: If multiple rows share (family, ind, sheet) — i.e., duplicates —
     they get the SAME doc_version_key. This is correct: they represent
     the same version. The duplicate_flag distinguishes them.

  3. Build revision chain (linked list):
     Consider only UNIQUE and SUSPECT rows for chain ordering.
     DUPLICATE rows get previous_version_key = same as their UNIQUE counterpart.

     Walk sorted distinct ind_sort_order values:
       - First distinct ind: previous_version_key = null
       - Each subsequent: previous_version_key = doc_version_key of previous distinct ind

     DUPLICATE rows inherit previous_version_key from their UNIQUE/first row.

  4. Determine is_latest:
     The row(s) with the HIGHEST ind_sort_order in this group get is_latest = true.
     All others: is_latest = false.

     If multiple rows share the highest ind_sort_order (duplicates):
       ALL of them get is_latest = true (they all represent the latest revision).

  5. Compute revision_count:
     Count of DISTINCT ind values in this (family, sheet) group.
     Same value for every row in the group.
     Null IND counts as one distinct value (represented by sort_order = 0).
```

**doc_version_key format:** `{doc_family_key}::{ind}::{source_sheet}`

- Example: `P17T2INEXEVTPTERI001MTDTZFD026001::A::LOT 06-02-MET`
- For null IND: `P17T2INEXEVTPTERI001MTDTZFD026001::NULL::LOT 06-02-MET`
- For UNPARSEABLE: `UNPARSEABLE::a1b2c3d4e5f6g7h8::NULL::LOT 42-PLB-UTB`

**Implementation:** `groupby(['doc_family_key', 'source_sheet'])` then sorted transform. Use `shift()` within sorted groups to compute `previous_version_key`. Use `transform('max')` for `is_latest`. Use `nunique()` for `revision_count`.

### Step 4: Cross-Lot Detection

**Scope:** Group by `doc_family_key` across all sheets.

**Input:** Master DataFrame with `doc_family_key`, `source_sheet`.

**Output:** New columns: `is_cross_lot`, `cross_lot_sheets`.

**Algorithm:**

```
For each doc_family_key:
  sheets = distinct source_sheet values for this family

  IF len(sheets) > 1:
    is_cross_lot = true
    cross_lot_sheets = sorted list of sheet names
  ELSE:
    is_cross_lot = false
    cross_lot_sheets = null (per GP2: null, not empty list)
```

**Key property:** Cross-lot detection is informational only. Revision chains remain **independent per sheet**. A document appearing in LOT 06 and LOT 42 has two separate chains with independent `is_latest`, `previous_version_key`, and `revision_count`.

**UNPARSEABLE rows:** Each has a unique `doc_family_key`, so they will always have `is_cross_lot = false`. This is correct by design.

**Implementation:** `groupby('doc_family_key')['source_sheet'].transform(lambda x: x.nunique() > 1)` for `is_cross_lot`. A separate grouped aggregation builds the `cross_lot_sheets` lists, then merged back.

### Step 6: Anomaly Detection

**Scope:** Group by `(doc_family_key, source_sheet)`. Operates on the fully enriched DataFrame.

**Input:** All M2-enriched columns plus `date_diffusion` from M1.

**Output:** Entries appended to the linking anomalies log.

**Anomaly types and detection logic:**

| Anomaly | Detection | Logged Fields |
|---------|-----------|---------------|
| **REVISION_GAP** | Within a (family, sheet) group, consecutive ind_sort_order values differ by more than 1 for alpha indices. E.g., A (1) then C (3) — gap at B (2). | family_key, sheet, missing_ind (e.g., "B"), between (e.g., "A→C") |
| **LATE_FIRST_APPEARANCE** | First ind_sort_order in the (family, sheet) group is > 1 for alpha indices, or > 0 for numeric. E.g., first IND is D (sort_order=4). | family_key, sheet, first_ind, first_sort_order |
| **DATE_REGRESSION** | Within a (family, sheet) group sorted by ind_sort_order, a later revision has a strictly earlier `date_diffusion` than a previous revision. Only compared between non-null dates. | family_key, sheet, earlier_ind, earlier_date, later_ind, later_date |
| **DUPLICATE_EXACT** | (Logged in Step 5.) Two+ rows with same (family, ind, sheet) and identical data. | family_key, sheet, ind, row_ids |
| **DUPLICATE_SUSPECT** | (Logged in Step 5.) Two+ rows with same (family, ind, sheet) but differing data. | family_key, sheet, ind, row_ids, differing_columns |
| **MISSING_IND** | (Logged in Step 2.) Row has null IND. | family_key, sheet, row_id, source_row |
| **UNPARSEABLE_DOCUMENT** | (Logged in Step 1.) Row has null document. | row_id, source_sheet, source_row, document_raw |

**REVISION_GAP detection algorithm (detailed):**

```
For each (family, sheet) group:
  Get sorted distinct ind_sort_order values (excluding 0 / null-IND)
  For consecutive pairs (prev_order, curr_order):
    IF both are alpha-derived (1–26 single-letter range) AND curr_order - prev_order > 1:
      For each missing value in (prev_order+1 ... curr_order-1):
        Log REVISION_GAP with the missing index letter

    # For numeric indices or multi-letter indices:
    # Only flag if curr_order - prev_order > 1
    # (We cannot enumerate "missing" numeric indices reliably)
```

**DATE_REGRESSION detection algorithm:**

```
For each (family, sheet) group:
  Sort by ind_sort_order ASC
  Keep only rows where date_diffusion IS NOT null
  Walk pairs (row_i, row_j) where j > i:
    IF row_j.date_diffusion < row_i.date_diffusion:
      Log DATE_REGRESSION
      Break (one log per pair, not combinatorial)

  # Compare consecutive non-null dates only, not all pairs.
```

**Implementation note:** REVISION_GAP and LATE_FIRST_APPEARANCE only fire for families with valid documents (not UNPARSEABLE). UNPARSEABLE families are single-row by design and cannot have revision sequences.

---

## 4. Internal Data Structures

### 4.1 doc_family_key Construction

```python
# Stored as: str column in DataFrame
# Primary path: document.str.replace("_", "")
# Fallback path: "UNPARSEABLE::" + sha256_hex_16

# No separate data structure needed. The key is a derived column
# computed once and used for all subsequent groupby operations.
```

### 4.2 Revision Chain (Linked List per Family-Sheet)

The chain is represented **implicitly** via the `previous_version_key` column — each row points to its predecessor. There is no separate linked-list object. The chain can be reconstructed by following `previous_version_key` pointers.

```python
# For a family with revisions A, B, D in LOT 06:
# Row A: previous_version_key = null,            is_latest = false
# Row B: previous_version_key = "...::A::LOT 06", is_latest = false
# Row D: previous_version_key = "...::B::LOT 06", is_latest = true
# (gap at C logged as REVISION_GAP, but link is B→D)
```

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

### 4.4 Cross-Lot Map

```python
# Transient dict built during Step 4:
# { doc_family_key: sorted_list_of_sheets }
# Used to populate is_cross_lot and cross_lot_sheets columns.
# Discarded after column assignment.
```

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
| REVISION_GAP | `missing_ind`, `between_prev`, `between_next` |
| LATE_FIRST_APPEARANCE | `first_ind`, `expected_start` |
| DATE_REGRESSION | `earlier_ind`, `earlier_date`, `later_ind`, `later_date` |
| DUPLICATE_EXACT | `row_ids`, `duplicate_count` |
| DUPLICATE_SUSPECT | `row_ids`, `differing_columns` |
| MISSING_IND | `source_row`, `document_raw` |
| UNPARSEABLE_DOCUMENT | `source_row`, `document_raw` |

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

---

## 5. Output Schemas

### 5.1 Enriched Master Dataset

All Module 1 columns are preserved unchanged, plus these new columns added by Module 2:

| Column | Type | Nullable? | Description |
|--------|------|-----------|-------------|
| `doc_family_key` | str | No | Stable family identifier. Flat uppercase string or `UNPARSEABLE::{hash}` |
| `doc_version_key` | str | No | Globally unique: `{doc_family_key}::{ind or "NULL"}::{source_sheet}` |
| `ind_sort_order` | int | No | Numeric sort position. A=1, B=2, ..., Z=26, AA=27. Null IND = 0. |
| `previous_version_key` | str or null | Yes | Previous revision's `doc_version_key` in same (family, sheet). Null for first. |
| `is_latest` | bool | No | True if highest `ind_sort_order` for this (family, sheet). |
| `revision_count` | int | No | Distinct IND values for this (family, sheet). |
| `is_cross_lot` | bool | No | True if family appears in multiple sheets. |
| `cross_lot_sheets` | list[str] or null | Yes | Sorted sheet names if `is_cross_lot`, else null per GP2. |
| `duplicate_flag` | str | No | `UNIQUE` / `DUPLICATE` / `SUSPECT` |

**Serialization (per GP4):**

- JSON: `cross_lot_sheets` as JSON array or null. Booleans as true/false. Nulls preserved.
- CSV: `cross_lot_sheets` as semicolon-separated string. Booleans as `true`/`false`. Nulls as empty cells.
- Excel: `cross_lot_sheets` as semicolon-separated string. Booleans as `TRUE`/`FALSE`.

### 5.2 Document Family Index

One row per `(doc_family_key, source_sheet)` pair.

| Column | Type | Description |
|--------|------|-------------|
| `doc_family_key` | str | Family identifier |
| `source_sheet` | str | Sheet (lot) |
| `lot` | str or null | Lot code from first row of this family-sheet group |
| `first_ind` | str or null | Lowest IND (by sort_order) in this family-sheet |
| `latest_ind` | str or null | Highest IND (by sort_order) in this family-sheet |
| `revision_count` | int | Distinct IND values |
| `latest_visa` | str or null | `visa_global` of the `is_latest` row (if unique; null if ambiguous/SUSPECT) |
| `has_revision_gap` | bool | True if any REVISION_GAP anomaly exists for this family-sheet |
| `is_cross_lot` | bool | True if family appears in multiple sheets |

**Row count estimate:** ~2,518 unique documents across ~25 sheets, but many documents appear in only 1 sheet. With 993 cross-lot documents, expect roughly 3,000–4,000 rows in the family index.

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

---

## 7. config_m2.py — Constants

```python
# Columns excluded from duplicate comparison
DUPLICATE_EXCLUDE_COLS = {
    "row_id", "source_row",
    "row_quality", "row_quality_details",
    # M2-derived columns (excluded because they don't exist at comparison time
    # or are derived from the data being compared):
    "doc_family_key", "doc_version_key", "ind_sort_order",
    "previous_version_key", "is_latest", "revision_count",
    "is_cross_lot", "cross_lot_sheets", "duplicate_flag",
}

# Anomaly type constants
ANOMALY_REVISION_GAP = "REVISION_GAP"
ANOMALY_LATE_FIRST = "LATE_FIRST_APPEARANCE"
ANOMALY_DATE_REGRESSION = "DATE_REGRESSION"
ANOMALY_DUPLICATE_EXACT = "DUPLICATE_EXACT"
ANOMALY_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"
ANOMALY_MISSING_IND = "MISSING_IND"
ANOMALY_UNPARSEABLE = "UNPARSEABLE_DOCUMENT"

# UNPARSEABLE key prefix
UNPARSEABLE_PREFIX = "UNPARSEABLE::"

# Hash truncation length (hex chars) for UNPARSEABLE keys
HASH_TRUNCATE_LENGTH = 16

# Required M1 columns for M2 to proceed
M2_REQUIRED_COLUMNS = {"document", "ind", "source_sheet", "source_row", "row_id"}

# Optional M1 columns (degrade gracefully)
M2_OPTIONAL_COLUMNS = {"date_diffusion", "visa_global"}
```

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
  3. Validate contract (required columns check)
  4. Build Module2Context
  5. Execute pipeline:
     a. Step 1: family_key.build_family_keys(df, ctx)
     b. Step 2: ind_normalization.compute_sort_orders(df, ctx)
     c. Step 5: duplicate_detection.detect_duplicates(df, ctx)
     d. Step 3: chain_linking.link_chains(df, ctx)
     e. Step 4: cross_lot.detect_cross_lot(df, ctx)
     f. Step 6: anomaly_detection.detect_anomalies(df, ctx)
  6. Build document family index
  7. Write outputs (JSON, CSV, optional Excel)
  8. Print summary
```

**Combined pipeline option:** A future `main_combined.py` can chain M1 → M2 (and later → M3) without intermediate serialization, passing the DataFrame directly. This avoids JSON round-trip overhead. But for now, M2 reads M1 output from disk for clean separation and debuggability.

---

## 9. Performance Considerations (GP5)

**Target:** Module 2 must contribute to the combined M1+M2+M3 budget of under 3 seconds for 5,000 rows.

**Expected M2 budget:** ~0.5–1.0 seconds for 5,000 rows (M1 takes ~1.5–2.0s, leaving room for M3).

**Strategy:**

- Steps 1 and 2 are row-level vectorized operations: O(n), negligible cost.
- Step 5 uses `groupby` + comparison: O(n) amortized for typical data (few duplicates). The comparison within groups is O(k * m) where k = group size and m = column count, but groups of size > 1 are rare.
- Step 3 uses `groupby` + sorted transform: O(n log n) for the sort, O(n) for the chain linking.
- Step 4 uses `groupby` + `nunique`: O(n).
- Step 6 uses `groupby` + sequential scan: O(n).

**No O(n^2) operations.** All steps are groupby-based with at most O(n log n) complexity.

---

## 10. Testing Strategy

### 10.1 Unit Tests (per step)

| Test File | Key Test Cases |
|-----------|---------------|
| `test_family_key.py` | Underscore format → correct flat key; compressed format → same flat key; both formats → identical key (the 88-pair test); null document → UNPARSEABLE key; two null documents → different keys; blank document_raw → UNPARSEABLE with empty string hash |
| `test_ind_normalization.py` | A→1, B→2, Z→26, AA→27; numeric "1"→1, "02"→2; null→0 + MISSING_IND log; mixed/unexpected→0 + WARNING |
| `test_duplicate_detection.py` | Single row → UNIQUE; identical rows → first UNIQUE + rest DUPLICATE; differing rows → all SUSPECT; null-field comparison (both null = equal); DUPLICATE_EXACT and DUPLICATE_SUSPECT logged |
| `test_chain_linking.py` | A→B→C chain; gap A→C (link correctly, gap logged); single revision (is_latest=true, no previous); null IND at position 0; duplicates get correct previous_version_key |
| `test_cross_lot.py` | Single-sheet family → false; multi-sheet family → true + sorted sheets; UNPARSEABLE → always false |
| `test_anomaly_detection.py` | REVISION_GAP for A,C; LATE_FIRST_APPEARANCE for first=D; DATE_REGRESSION for later revision with earlier date; no false positives on clean data |

### 10.2 Integration Test

`test_m2_integration.py`:

- Load the actual M1 output (`output/master_dataset.json`)
- Run full M2 pipeline
- Verify: all 4,108 rows have all M2 columns populated
- Verify: 88 naming-format pairs resolve to same `doc_family_key`
- Verify: exactly 1 UNPARSEABLE row (LOT 42, row 244)
- Verify: UNPARSEABLE row has `is_cross_lot = false`
- Verify: `doc_version_key` is globally unique
- Verify: `is_latest` has exactly one `true` per non-duplicate (family, sheet) group
- Verify: anomaly log contains no unexpected types
- Verify: determinism (run twice, compare outputs byte-for-byte)

### 10.3 Golden Snapshot

Generate a golden snapshot of M2 outputs from the current GrandFichier for regression testing.

---

## 11. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| M1 output format changes | Medium | High | Contract validation at M2 entry. Explicit required-column check. |
| New IND formats (e.g., "1A", "B2") | Low | Medium | Fallback to sort_order = 0 + WARNING log. Never crash. |
| Large duplicate groups (>10 rows same family/ind/sheet) | Low | Low | Comparison is column-count bounded, not row-count quadratic. |
| UNPARSEABLE hash collision | Negligible | Low | 16 hex chars = 64 bits. For <100K rows, collision probability < 10^-14. |
| Cross-lot families with conflicting data | Medium | Low | M2 does not merge cross-lot data. Each sheet is independent. Flag only. |
| Future M1 columns not in exclude list | Medium | Medium | DUPLICATE_EXCLUDE_COLS is explicit. New M1 columns automatically participate in comparison (conservative default). |

---

## 12. Success Criteria Checklist

Directly from V2.2 spec section 2.7, with implementation verification method:

| Criterion | Verification |
|-----------|-------------|
| Every row has `doc_family_key` | Assert no nulls in column |
| Every row has `doc_version_key` | Assert no nulls in column |
| Every row has `duplicate_flag` | Assert all values in {UNIQUE, DUPLICATE, SUSPECT} |
| 88 naming pairs resolve to same family | Test: collect all doc_family_key values for known pairs, assert set size = 1 per pair |
| Chains correctly ordered | Test: for each (family, sheet), verify ind_sort_order is strictly non-decreasing along chain |
| `is_latest` correct per (family, sheet) | Test: for each group, exactly one distinct ind_sort_order has is_latest=true, and it is the maximum |
| UNPARSEABLE rows produce unique keys | Test: all keys starting with "UNPARSEABLE::" are distinct |
| UNPARSEABLE rows never group with valid documents | Test: no doc_family_key starting with "UNPARSEABLE::" appears without that prefix anywhere |
| UNPARSEABLE rows never group with each other | Test: each UNPARSEABLE key appears in exactly 1 row |
| All anomalies logged | Test: anomaly log is non-empty; types are subset of defined 7 |
| Deterministic output | Test: run twice on same input, diff outputs |
