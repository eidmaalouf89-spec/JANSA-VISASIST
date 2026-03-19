# JANSA VISASIST — Module 1: Technical Implementation Plan

**Version**: Based on Specification V2.2 — Revised
**Scope**: Data Ingestion & Normalization Engine only
**Date**: 2026-03-19

---

## 1. Architecture

### 1.1 Language & Runtime

**Python 3.10+** — chosen for pandas maturity, openpyxl support, and construction-industry familiarity.

### 1.2 Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| `pandas` | ≥ 2.0 | DataFrame operations, merge, schema union |
| `openpyxl` | ≥ 3.1 | Read .xlsx with multi-row header access |
| `unicodedata` | stdlib | NFKD accent removal for header normalization |
| `rapidfuzz` | ≥ 3.0 | Levenshtein fuzzy matching (Step 4, Level 3) |
| `re` | stdlib | Regex for document normalization, validation |
| `json` | stdlib | JSON serialization (GP4) |
| `datetime` | stdlib | Date parsing and ISO formatting |
| `logging` | stdlib | Structured pipeline logging |
| `argparse` | stdlib | CLI argument parsing |
| `dataclasses` | stdlib | Structured data models |

No web frameworks. No database. No external APIs. No Module 2/3 dependencies.

### 1.3 Design Principles

- **Correctness first**: every step must be provably correct before considering speed
- **Deterministic**: identical input always produces identical output
- **Fail-safe**: bad data never crashes the pipeline (GP3)
- **Traceable**: every transformation logged, every field has raw counterpart
- **Performance-aware**: keep GP5 targets in mind, but do not optimize prematurely; prefer clear row-by-row logic over premature vectorization when correctness is at stake

---

## 2. Folder Structure

```
jansa_visasist/
├── main.py                     # Entry point: parse args → build context → run pipeline → write outputs
├── config.py                   # Constants, canonical schema, approver dictionary, status maps
├── context.py                  # PipelineContext: centralized logs, reports, counters, config, output paths
├── pipeline/
│   ├── __init__.py
│   ├── sheet_discovery.py      # Step 1: enumerate and validate sheets
│   ├── header_detection.py     # Step 2: find header row, anchor R, R+1, R+2
│   ├── column_mapping.py       # Step 4: normalize headers, 4-level matching
│   ├── approver_detection.py   # Step 5: detect approver blocks from R+1
│   ├── row_extraction.py       # Step 6: extract data rows from R+3 (permissive)
│   ├── doc_normalization.py    # Step 3: document reference pipeline (8 sub-steps)
│   ├── doc_validation.py       # Step 7a: structural validation (R1–R4)
│   ├── date_cleaning.py        # Step 7b: serial → ISO, corruption detection
│   ├── status_cleaning.py      # Step 7c + 7d: visa_global + approver statut
│   ├── text_cleaning.py        # Step 7e: trim, control chars, collapse spaces
│   ├── quality_scoring.py      # Step 8: row-level OK / WARNING / ERROR from accumulated logs
│   ├── schema_merge.py         # Step 9: union schema, merge all sheets
│   └── validation_checks.py    # Step 10: row count, schema, date sanity
├── models/
│   ├── __init__.py
│   ├── log_entry.py            # ImportLogEntry dataclass
│   ├── header_report.py        # HeaderMappingReport dataclass
│   └── enums.py                # Severity, RowQuality, MappingConfidence enums
├── outputs/
│   ├── __init__.py
│   ├── json_writer.py          # Master dataset + log → JSON (GP4)
│   ├── csv_writer.py           # Master dataset + log → CSV (GP4)
│   └── excel_writer.py         # Optional .xlsx audit output (GP4)
├── tests/
│   ├── test_doc_normalization.py
│   ├── test_doc_validation.py
│   ├── test_date_cleaning.py
│   ├── test_status_cleaning.py
│   ├── test_column_mapping.py
│   ├── test_row_extraction.py
│   ├── test_quality_scoring.py
│   ├── test_integration.py     # End-to-end with known GrandFichier
│   └── golden/                 # Golden-file snapshots for regression testing
│       ├── expected_master_dataset.json
│       ├── expected_import_log.json
│       └── expected_header_mapping_report.json
└── requirements.txt
```

---

## 3. PipelineContext (`context.py`)

A single shared object passed through every pipeline step. Centralizes all mutable state so individual functions remain pure (receive context, return results, append to context).

```python
@dataclass
class PipelineContext:
    # --- Configuration (immutable after init) ---
    input_path: str
    output_dir: str

    # --- Accumulated state (mutated during pipeline) ---
    import_log: List[ImportLogEntry]         # All log entries across all sheets
    header_reports: List[HeaderMappingReport] # One per sheet
    sheet_row_counts: Dict[str, int]         # sheet_name → extracted row count
    sheets_processed: int = 0
    sheets_skipped: int = 0
    total_rows: int = 0

    # --- Per-row log accumulator (reset per row) ---
    # Used internally during row processing; flushed to import_log after each row
    _current_row_logs: List[ImportLogEntry] = field(default_factory=list)

    def log(self, entry: ImportLogEntry) -> None:
        """Append a log entry to both current-row buffer and global log."""
        self._current_row_logs.append(entry)
        self.import_log.append(entry)

    def log_sheet_level(self, entry: ImportLogEntry) -> None:
        """Append a sheet-level log entry (no row context)."""
        self.import_log.append(entry)

    def begin_row(self) -> None:
        """Reset per-row accumulator before processing a new row."""
        self._current_row_logs = []

    def end_row(self) -> List[ImportLogEntry]:
        """Return accumulated logs for the current row (for quality scoring)."""
        return list(self._current_row_logs)

    def next_log_id(self) -> str:
        """Auto-incrementing log ID."""
        return str(len(self.import_log) + 1)
```

**Rationale**: Without a shared context, log entries, counters, and config must be threaded through every function signature or returned as tuples. PipelineContext eliminates this while keeping individual step functions testable — they receive a context and operate on it.

---

## 4. Execution Architecture

The pipeline is organized into four distinct phases, each with clear boundaries:

```
Phase A: Sheet-Level Preprocessing
  For each sheet:
    Step 1: Sheet Discovery — enumerate and load sheet
    Step 2: Header Detection — find anchor rows R, R+1, R+2
    Step 4: Column Mapping — map raw headers to canonical schema
    Step 5: Approver Detection — detect approver blocks from R+1
    Step 6: Row Extraction — extract raw rows from R+3 (permissive)
    → Output: list of raw row dicts + column mapping + approver list

Phase B: Row-Level Normalization & Cleaning
  For each row in each sheet:
    ctx.begin_row()
    Step 3:  Normalize document reference
    Step 7a: Validate document structure → logs only, no quality assignment
    Step 7b: Clean all date fields
    Step 7c: Normalize visa_global
    Step 7d: Normalize all approver statuts
    Step 7e: Clean text fields
    Assign row_id, source_sheet, source_row
    Step 8:  Score row quality from ctx.end_row() accumulated logs
    → Output: one cleaned row dict with quality grade

Phase C: Sheet Assembly
  For each sheet:
    Collect all cleaned rows into a per-sheet DataFrame
    Generate header mapping report entry
    Record sheet row count in context

Phase D: Global Merge & Validation
  Step 9:  Schema union + merge all sheet DataFrames
  Step 10: Run validation checks (row count, schema, dates, orphans)
  Write all outputs (JSON, CSV, optional XLSX)
  Print summary, return exit code
```

---

## 5. Data Pipeline — Step-by-Step

### Step 1: Sheet Discovery (`sheet_discovery.py`)

**Input**: File path to .xlsx (from PipelineContext)
**Process**:
1. Open workbook with `openpyxl` in `read_only=True, data_only=True`
2. Enumerate all sheet names (expect 25)
3. For each sheet, attempt to load. On exception → log ERROR via `ctx.log_sheet_level()`, set `mapping_confidence = FAILED`, skip sheet, increment `ctx.sheets_skipped`

**Output**: List of `(sheet_name, worksheet_object)` tuples

### Step 2: Header Row Detection (`header_detection.py`)

**Input**: One worksheet object
**Process**:
1. Scan rows 1–15, column A
2. Find first row where cell value (stripped, lowercased) contains `"document"`
3. That row index = **R** (anchor row)
4. **R+1** = approver names row
5. **R+2** = sub-labels row (DATE / N° / STATUT)
6. Data starts at **R+3**
7. If no match in rows 1–15 → log ERROR, skip sheet, `mapping_confidence = FAILED`

**Output**: `HeaderAnchor(header_row=R, approver_row=R+1, sublabel_row=R+2, data_start=R+3)`

### Step 3: Document Reference Normalization (`doc_normalization.py`)

**Input**: Raw document string from column A
**Process** (strictly sequential, per spec):
1. `strip()` leading/trailing whitespace
2. `upper()`
3. Replace spaces and hyphens with underscores: `re.sub(r'[\s\-]+', '_', val)`
4. Collapse repeated underscores: `re.sub(r'_+', '_', val)`
5. Strip trailing punctuation (`.` `,`): `re.sub(r'[.,]+$', '', val)` → if changed, log INFO `trailing_punctuation`
6. Strip leading/trailing underscores: `strip('_')`
7. If starts with `17_T2` or `17T2` (missing P): prepend `P` → log INFO `missing_p_prefix`
8. If result is empty: return `None` → log ERROR `missing_field`

**Output**: Normalized string or `None`

### Step 4: Column Mapping (`column_mapping.py`)

**Input**: Raw header values from row R
**Process**:

Header normalization function:
```
def normalize_header(raw: str) -> str:
    val = raw.strip()
    val = val.replace('\n', ' ').replace('\r', ' ')
    val = val.lower()
    val = unicodedata.normalize('NFKD', val)
    val = ''.join(c for c in val if not unicodedata.combining(c))
    val = re.sub(r'\s+', ' ', val).strip()
    val = re.sub(r'\(.*?\)', '', val).strip()
    val = re.sub(r'[.,;:]+$', '', val).strip()
    return val
```

4-level matching (first match wins):
- **Level 1** — Exact: normalized header == canonical key → confidence 1.0
- **Level 2** — Keyword: canonical key has keyword set; all keywords present in header → confidence 0.85
- **Level 3** — Fuzzy: `rapidfuzz.fuzz.ratio(header, canonical) / 100` → if ≥ 0.80, use match with highest score
- **Level 4** — Unmatched: store column as `unmapped_{col_index}`, log WARNING

**Output**: `Dict[int, ColumnMapping]` mapping column index → canonical name + confidence

### Step 5: Approver Block Detection (`approver_detection.py`)

**Input**: Row R+1 (approver names), column mapping result
**Process**:
1. Find the column index of `visa_global` from Step 4
2. Scan columns right of `visa_global` in row R+1
3. Each non-empty cell = approver name (raw)
4. Normalize approver name → match against 14 canonical keys (exact or fuzzy)
5. Each approver owns exactly **3 consecutive source columns** in the Excel file: DATE, N°, STATUT
6. Stop when `observations` column is reached or row ends
7. Record `assigned_approvers` = list of canonical keys found (structural, applies to all rows on this sheet)

**Output**: `List[ApproverBlock(canonical_key, date_col, n_col, statut_col)]` + `assigned_approvers`

**Important — source vs. output distinction**: Each approver has 3 source columns in Excel (DATE, N°, STATUT) but generates **5 output columns** in the master dataset: `{KEY}_date_raw`, `{KEY}_date`, `{KEY}_n`, `{KEY}_statut_raw`, `{KEY}_statut`. The raw/normalized split for date and statut is created during Phase B cleaning, not during detection.

### Step 6: Data Row Extraction (`row_extraction.py`)

**Input**: Worksheet, data_start row index, column mapping
**Process**:
1. Iterate from `data_start` to last row
2. Read column A value
3. **Permissive inclusion rule**: a row is extracted if column A is **non-empty** (any non-blank value)
4. Skip only truly empty rows (column A is None, empty string, or whitespace-only)
5. For each included row: extract all mapped columns into a raw dict, recording `source_row` (1-based Excel row number)

**Rationale for permissive extraction**: The spec requires that rows with malformed document references (e.g., `²S`) are preserved because their other fields (titre, dates, lot, approver statuses) remain meaningful. If row extraction filters on document format, such rows would be silently dropped before Step 7a ever sees them. The structural document validation in Step 7a is the correct place to assess document quality — not here. Decoration/summary rows are still excluded because they have empty column A.

**Output**: `List[Dict]` of raw row data with `source_row`

### Step 7a: Structural Document Validation (`doc_validation.py`)

**Input**: Normalized document string (from Step 3; may be `None` if Step 3 returned empty)
**Process** — if document is `None` (already flagged by Step 3), skip validation. Otherwise, apply rules sequentially, fail on first:

| Rule | Test | Failure |
|------|------|---------|
| R1 | `len(doc) < 10` | Too short |
| R2 | No digit `[0-9]` found | No numeric segment |
| R3 | No letter `[A-Z]` found | No alphabetic segment |
| R4 | Non-alnum ratio (excl. underscores) > 30% | High noise |

If ANY rule fails:
- Set `document = None` (preserve `document_raw` untouched)
- Log via `ctx.log()`: severity=WARNING, category=`unparseable_document`, action=`Set document to null — failed structural rule R{n}`

**What this step does NOT do**: It does not assign `row_quality` directly. It only emits log entries. Row quality is computed later in Step 8 by examining accumulated logs for the entire row. This ensures a single, consistent quality-scoring path.

**Output**: Validated document string or `None` + log entries appended to context

### Step 7b: Date Cleaning (`date_cleaning.py`)

**Input**: Raw date value (could be Excel serial, string, datetime, or blank)
**Process**:
1. Store raw in `{field}_raw`
2. If blank/None → `None` (GP2)
3. If numeric: check range `[1, 2958465]`
   - Valid → convert to ISO date string: `datetime(1899, 12, 30) + timedelta(days=serial)`
   - Out of range → `None` + log ERROR `corrupted_date` via `ctx.log()`
4. If datetime object → format to ISO `YYYY-MM-DD`
5. If string → attempt parse with common French/ISO formats
6. All other → `None` + log ERROR via `ctx.log()`

**Output**: ISO date string or `None`

### Step 7c: VISA GLOBAL Normalization (`status_cleaning.py`)

**Input**: Raw visa_global value
**Process**:
1. Store in `visa_global_raw`
2. Trim, uppercase
3. Exact match against: `VSO, VAO, REF, HM, SUS, DEF, FAV`
4. Case-insensitive match (e.g., `vao` → `VAO`)
5. Unknown value (e.g., `C`) → `None` + log WARNING `unknown_status` via `ctx.log()`
6. Blank → `None` (GP2)

**Output**: One of `{VSO, VAO, REF, HM, SUS, DEF, FAV, None}`

### Step 7d: Approver STATUT Normalization (`status_cleaning.py`)

**Input**: Raw approver status value
**Process** (sequential):
1. Trim
2. Strip leading `.,;-` and parentheses
3. Remove internal spaces
4. Uppercase
5. Map synonyms via canonical dictionary (defined in `config.py`)
6. Multi-value (contains newline) → split, take **last** value, log both via `ctx.log()`
7. Ambiguous typos (e.g., `VSA`) → `None` + log WARNING via `ctx.log()`
8. Validate against vocabulary
9. Blank → `None` (GP2)

**Output**: Normalized status or `None`

### Step 7e: Text Cleaning (`text_cleaning.py`)

**Input**: Raw text (titre, observations, etc.)
**Process**:
1. Strip leading/trailing whitespace
2. Remove control characters: `re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', val)`
3. Collapse multiple spaces: `re.sub(r' +', ' ', val)`
4. If result is empty → `None` (GP2)

**Output**: Clean string or `None`

### Step 8: Row Quality Scoring (`quality_scoring.py`)

**Input**: `ctx.end_row()` — the list of all log entries accumulated for the current row during Phase B
**Process**:

| Grade | Condition |
|-------|-----------|
| `OK` | Zero WARNING, zero ERROR in the row's log entries |
| `WARNING` | ≥1 WARNING, zero ERROR |
| `ERROR` | ≥1 ERROR |

- `row_quality_details` = deduplicated list of all issue category codes from the row's log entries

**This is the single authority for row quality**. No other step assigns `row_quality` directly. Steps 3, 7a–7e only emit log entries; Step 8 derives the grade from them. This guarantees consistency: a row's quality always reflects the totality of its issues, never a partial snapshot.

**Output**: `(row_quality: str, row_quality_details: List[str])`

### Step 9: Schema Union & Merge (`schema_merge.py`)

**Input**: List of per-sheet DataFrames + per-sheet approver lists
**Process**:
1. Compute union of all column names across sheets
2. All 14 canonical approver keys × 5 output columns = always present in final schema
3. Missing approver columns for a given sheet → `None` (structural, tracked by `assigned_approvers`)
4. `pd.concat()` all sheet DataFrames with `ignore_index=True`
5. Verify: no column uses empty string (GP2 enforcement)

**Output**: Single merged `pd.DataFrame`

### Step 10: Validation Checks (`validation_checks.py`)

**Input**: Merged DataFrame + PipelineContext (for sheet row counts)
**Process**:
1. **Row count**: `sum(ctx.sheet_row_counts.values()) == len(master_df)` — log ERROR if mismatch
2. **Schema completeness**: all core columns + all approver groups present
3. **No orphaned rows**: every row has `source_sheet`, `source_row`, `row_id` non-null
4. **Date sanity**: no normalized date < `2020-01-01` or > `2030-12-31`
5. Report pass/fail for each check

**Output**: Validation report (pass/fail per check + details)

---

## 6. Internal Data Structures

### 6.1 Canonical Column Schema (defined in `config.py`)

```python
CORE_COLUMNS = [
    "source_sheet", "source_row", "row_id", "row_quality", "row_quality_details",
    "document_raw", "document",
    "titre",
    "date_diffusion_raw", "date_diffusion",
    "lot", "type_doc", "niv", "zone", "n_doc",
    "ind_raw", "ind",
    "type_format", "ancien", "n_bdx",
    "date_reception_raw", "date_reception",
    "non_recu_papier",
    "date_contractuelle_visa_raw", "date_contractuelle_visa",
    "visa_global_raw", "visa_global",
    "observations",
    "assigned_approvers",
]
```

### 6.2 Canonical Approver Keys & Column Expansion

```python
CANONICAL_APPROVERS = [
    "MOEX_GEMO", "ARCHI_MOX", "BET_STR_TERRELL", "BET_GEOLIA_G4",
    "ACOUSTICIEN_AVLS", "AMO_HQE_LE_SOMMER", "BET_POLLUTION_DIE",
    "SOCOTEC", "BET_ELIOTH", "BET_EGIS", "BET_ASCAUDIT",
    "BET_ASCENSEUR", "BET_SPK", "PAYSAGISTE_MUGO",
]

# Source vs. Output column distinction:
#
# In the Excel file, each approver occupies 3 consecutive columns:
#   DATE | N° | STATUT
#
# In the master dataset output, each approver generates 5 columns:
#   {KEY}_date_raw    ← raw Excel value (preserved for traceability)
#   {KEY}_date        ← normalized ISO date (or null)
#   {KEY}_n           ← bordereau number (text, cleaned)
#   {KEY}_statut_raw  ← raw Excel value (preserved for traceability)
#   {KEY}_statut      ← normalized status (or null)
#
# Total approver output columns: 14 approvers × 5 columns = 70
```

### 6.3 VISA Status Vocabulary

```python
VISA_GLOBAL_VALUES = {"VSO", "VAO", "REF", "HM", "SUS", "DEF", "FAV"}
```

### 6.4 Import Log Entry

```python
@dataclass
class ImportLogEntry:
    log_id: str          # Auto-incremented via ctx.next_log_id()
    sheet: str           # Source sheet name
    row: Optional[int]   # Source row (None for sheet-level entries)
    column: Optional[str] # Column name (None if not applicable)
    severity: str        # ERROR | WARNING | INFO
    category: str        # corrupted_date, unknown_status, missing_field, fuzzy_match,
                         # unparseable_document, trailing_punctuation, missing_p_prefix, ...
    raw_value: Optional[str]
    action_taken: str    # What the pipeline did
    confidence: Optional[float]  # For fuzzy matches (None otherwise)
```

### 6.5 Header Mapping Report Entry

```python
@dataclass
class HeaderMappingReport:
    sheet: str
    header_row_detected: int
    core_columns_mapped: int
    core_columns_missing: List[str]
    approvers_detected: List[str]
    mapping_confidence: str   # HIGH | MEDIUM | LOW | FAILED
    unmapped_columns: List[str]
```

### 6.6 Mapping Confidence Thresholds

```python
# Per-sheet confidence based on worst column match:
# HIGH:   all columns matched at Level 1 or Level 2
# MEDIUM: at least one Level 3 fuzzy match (≥0.80)
# LOW:    at least one unmapped column (Level 4)
# FAILED: header row not found → sheet skipped
```

---

## 7. Input / Output Formats

### 7.1 Input

- **File**: Single `.xlsx` file (GrandFichier)
- **Invocation**: `python main.py --input path/to/GrandFichier.xlsx --output-dir path/to/output/`
- No configuration files. No environment variables. No user prompts.

### 7.2 Outputs (per GP4)

All written to `--output-dir`:

| File | Format | Content |
|------|--------|---------|
| `master_dataset.json` | JSON | Full master DataFrame. Nulls preserved. Dates as ISO strings. Lists as arrays. |
| `master_dataset.csv` | CSV | Same data. Nulls as empty cells. Lists as semicolon-separated. |
| `master_dataset.xlsx` | Excel | Optional audit format. Single sheet. |
| `import_log.json` | JSON | All ImportLogEntry records. |
| `import_log.csv` | CSV | Same log, flat format. |
| `header_mapping_report.json` | JSON | Per-sheet HeaderMappingReport. |
| `validation_report.json` | JSON | Step 10 pass/fail results. |

### 7.3 Return Code

- `0` — pipeline completed (may have warnings)
- `1` — pipeline failed (system-level error)

---

## 8. Execution Flow (`main.py`)

```
1.  Parse CLI args (input path, output dir)
2.  Initialize PipelineContext

--- Phase A: Sheet-Level Preprocessing ---
3.  STEP 1: Sheet Discovery → list of (sheet_name, worksheet)
4.  For each sheet:
      a. STEP 2: Detect header row → HeaderAnchor (or skip sheet)
      b. STEP 4: Map columns → column mapping dict
      c. STEP 5: Detect approver blocks → approver list + assigned_approvers
      d. STEP 6: Extract data rows (permissive) → raw row dicts

--- Phase B: Row-Level Normalization & Cleaning ---
5.  For each sheet's raw rows:
      For each row:
        a. ctx.begin_row()
        b. STEP 3:  Normalize document reference → logs emitted to ctx
        c. STEP 7a: Validate document structure → logs emitted to ctx
        d. STEP 7b: Clean all date fields → logs emitted to ctx
        e. STEP 7c: Normalize visa_global → logs emitted to ctx
        f. STEP 7d: Normalize all approver statuts → logs emitted to ctx
        g. STEP 7e: Clean text fields
        h. Assign row_id = {sheet_index}_{source_row}
        i. STEP 8: Score row quality from ctx.end_row()

--- Phase C: Sheet Assembly ---
6.  For each sheet:
      a. Collect cleaned rows into per-sheet DataFrame
      b. Generate header mapping report entry → ctx.header_reports
      c. Record row count → ctx.sheet_row_counts

--- Phase D: Global Merge & Validation ---
7.  STEP 9: Schema union + merge all DataFrames
8.  STEP 10: Run validation checks
9.  Write all outputs (JSON, CSV, optional XLSX)
10. Print summary to stdout
11. Return exit code
```

---

## 9. Error Handling Strategy

| Scope | Failure | Action | Severity |
|-------|---------|--------|----------|
| System | File not found / unreadable | Abort, exit 1 | FATAL |
| Sheet | No header row detected | Skip sheet, continue | ERROR |
| Sheet | openpyxl exception | Skip sheet, continue | ERROR |
| Row | Corrupted cell read | Keep row, null the field | ERROR |
| Field | Corrupted date serial | Null + raw preserved | ERROR |
| Field | Unknown status value | Null + raw preserved | WARNING |
| Field | Fuzzy match < 0.80 | Unmapped column | WARNING |
| Field | Unparseable document (Step 7a) | document = null, row preserved | WARNING |
| Field | Trailing punctuation | Strip + log | INFO |
| Field | Missing P prefix | Prepend P + log | INFO |

**Invariant**: Bad data never crashes the pipeline. Every anomaly is logged. Every raw value is preserved. Row quality is always derived from accumulated logs, never assigned ad hoc.

---

## 10. Performance Considerations (GP5)

| Scale | Target |
|-------|--------|
| ≤ 5K rows | < 3s (M1 + M2 + M3 combined per spec) |
| ≤ 10K rows | < 8s |
| ≤ 25K rows | < 20s |

**Approach**: Write correct, clear row-by-row logic first. Profile only if targets are missed. Likely optimizations if needed (not pre-applied):

- Header fuzzy matching: cache results per unique normalized header string (only 34–47 per sheet)
- Status normalization: dict lookup, O(1)
- Date conversion: batch via `pd.to_datetime` after initial row-level cleaning
- Schema merge: single `pd.concat` call
- Avoid O(n²) patterns in any loop

**Principle**: The current dataset is ~4K rows across 25 sheets. Row-by-row Python with dict lookups will comfortably meet GP5 targets. Do not introduce vectorized complexity unless measurements prove otherwise.

---

## 11. Testing Strategy

| Test Type | Scope | Tool |
|-----------|-------|------|
| Unit | Each pipeline step in isolation | `pytest` |
| Integration | Full pipeline on known GrandFichier | `pytest` |
| Regression | V2.2 edge cases (²S, trailing dot, missing P) | `pytest` with fixtures |
| Golden-file | Full output comparison against approved snapshots | `pytest` (see below) |
| Validation | Output row count, schema, null policy | Automated in Step 10 |

### Golden-File Regression Testing

**Purpose**: Catch unintended changes to pipeline output across code changes. A known GrandFichier input produces a known output; any deviation fails the test.

**Setup**:
1. Run the pipeline once on the reference GrandFichier with a verified-correct implementation
2. Save outputs as golden files in `tests/golden/`:
   - `expected_master_dataset.json` — full master dataset
   - `expected_import_log.json` — all log entries
   - `expected_header_mapping_report.json` — per-sheet reports
3. These files are committed to version control

**Test execution** (`test_integration.py`):
1. Run the pipeline on the same reference GrandFichier
2. Load actual outputs and golden files
3. Compare field-by-field with tolerance for:
   - Log ID ordering (if non-deterministic — but should be deterministic)
   - Float precision on confidence scores (round to 2 decimal places before comparison)
4. Any structural difference = test failure with diff output

**Maintenance**:
- When the spec changes (e.g., V2.2 → V2.3), regenerate golden files deliberately
- Golden file updates require explicit human review and approval
- Never auto-update golden files in CI

### Key Unit Test Cases (from spec)

- `²S` → document = null, WARNING log with category `unparseable_document`, rule R1
- `17_T2_GE_EXE_...` → P prepended, INFO log with category `missing_p_prefix`
- `P17_T2_..._A.` → trailing dot stripped, INFO log with category `trailing_punctuation`
- Corrupted date serial > 2.9M → null, ERROR log with category `corrupted_date`
- `C` visa status → null, WARNING log with category `unknown_status`
- `VSA` approver status → null, WARNING log
- Multi-value statut with newline → last value kept, both logged
- Row with bad document but valid titre/dates → row preserved, document = null, row_quality = WARNING (not dropped)
- Row with corrupted date AND unknown status → row_quality = ERROR (ERROR takes precedence)
- Completely clean row → row_quality = OK, empty row_quality_details

---

## 12. Implementation Order

| Phase | Steps | Deliverable | Est. Effort |
|-------|-------|-------------|-------------|
| **Phase 1** | config.py, context.py, models/, enums | Constants, PipelineContext, data structures | 0.5 day |
| **Phase 2** | Steps 1–2 (discovery, header) | Sheet loading, header anchor | 0.5 day |
| **Phase 3** | Steps 4–5 (col map, approvers) | Column mapping + approver detection | 1 day |
| **Phase 4** | Step 6 (row extraction, permissive) | Raw data extraction | 0.5 day |
| **Phase 5** | Step 3 + Steps 7a–7e (normalization + cleaning) | Full data cleaning pipeline | 1.5 days |
| **Phase 6** | Steps 8–10 (quality scoring, merge, validate) | Merged dataset + validation | 1 day |
| **Phase 7** | Output writers (JSON, CSV, XLSX) | All output files | 0.5 day |
| **Phase 8** | Unit tests + integration + golden files | Test suite passing | 1.5 days |
| **Total** | | | **~7 days** |

---

## 13. Dependencies Between Steps

```
Phase A ─ Sheet-Level Preprocessing
  Step 1 (Sheet Discovery)
    └── Step 2 (Header Detection)
          ├── Step 4 (Column Mapping)
          │     └── Step 5 (Approver Detection)
          └── Step 6 (Row Extraction — permissive)

Phase B ─ Row-Level Normalization & Cleaning
  For each row from Step 6:
    ├── Step 3 (Doc Normalization)
    │     └── Step 7a (Doc Validation — logs only)
    ├── Step 7b (Date Cleaning)
    ├── Step 7c (VISA Global Norm)
    ├── Step 7d (Approver Statut Norm)
    ├── Step 7e (Text Cleaning)
    └── Step 8 (Quality Scoring — from accumulated logs)

Phase C ─ Sheet Assembly
  Per-sheet DataFrame construction

Phase D ─ Global Merge & Validation
  Step 9 (Schema Merge)
    └── Step 10 (Validation Checks)
```

---

## 14. Corrections Applied (V1 → V2 of this plan)

For traceability, the following corrections were applied to the original implementation plan:

1. **Permissive row extraction (Step 6)**: Removed format-based filtering from row extraction. Any row with non-empty column A is extracted. Document quality assessment happens in Step 7a, not at extraction time. This prevents silent drops of rows like `²S` that have valid non-document fields.

2. **No direct row_quality assignment in Step 7a**: Step 7a now only emits log entries via `ctx.log()`. Row quality is computed exclusively in Step 8 from accumulated logs. This ensures a single, consistent quality-scoring authority.

3. **Approver source vs. output columns clarified**: Each approver has 3 source columns in Excel (DATE, N°, STATUT) but 5 output columns in the master dataset ({KEY}_date_raw, {KEY}_date, {KEY}_n, {KEY}_statut_raw, {KEY}_statut). The distinction is now explicit in Step 5 and Section 6.2.

4. **Module 2 dependency removed**: Removed `hashlib` from the library list. It was present only for M2's `UNPARSEABLE::{hash}` fallback, which is not Module 1's responsibility. Module 1's contract is: if document is unparseable, set it to `None`. Module 2 decides what to do with that.

5. **Execution architecture restructured into 4 phases**: Phase A (sheet preprocessing), Phase B (row normalization/cleaning), Phase C (sheet assembly), Phase D (global merge/validation). This replaces the flat loop structure and makes the pipeline's flow explicit.

6. **Correctness over premature optimization**: Removed "single-pass per sheet" as a design principle. Replaced with "correctness first" and "performance-aware." Performance section now recommends writing clear row-by-row logic first and optimizing only if GP5 targets are missed after measurement.

7. **PipelineContext added**: New `context.py` with centralized log accumulation, per-row log buffering (`begin_row`/`end_row`), sheet counters, header reports, and config. Eliminates scattered state threading.

8. **Golden-file regression testing added**: New `tests/golden/` directory with approved snapshot files. Integration test compares actual output against golden files field-by-field. Catches unintended regressions. Requires explicit human review to update.

---

## 15. Constraints Checklist

- [x] Strictly follows V2.2 specification
- [x] Deterministic: same input → same output, always
- [x] No UI, no CLI prompts, no external APIs
- [x] No AI/ML — only rules, dictionaries, and Levenshtein
- [x] No Module 2/3 dependencies — Module 1 is self-contained
- [x] GP2 enforced: null everywhere, never empty string
- [x] GP3 enforced: never crash on bad data
- [x] GP4 enforced: JSON + CSV + optional XLSX outputs
- [x] GP5 aware: targets documented, optimization deferred until measurement
- [x] All 14 approver keys × 5 output columns in schema regardless of sheet presence
- [x] Raw values always preserved alongside normalized
- [x] Every anomaly logged with category, severity, and action taken
- [x] Row quality derived exclusively from accumulated log entries (Step 8)
- [x] Row extraction is permissive — no silent drops before validation
- [x] PipelineContext centralizes all mutable pipeline state
- [x] Golden-file regression tests guard against unintended output changes
