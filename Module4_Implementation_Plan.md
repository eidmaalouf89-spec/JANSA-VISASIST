# Module 4: Analysis Engine — Implementation Plan

**Version**: V2.2.2 E1–E3 compliant — **Revision V3 (9 patches applied, coding-ready)**
**Date**: 2026-03-20
**Status**: PLANNING PHASE — No code. Plan only.

---

## 1. Executive Implementation Overview

Module 4 (M4) is the Analysis Engine — Tier 2 in the JANSA VISASIST pipeline orchestration. It consumes the outputs of M1 (master dataset), M2 (enriched dataset with revision linking), and M3 (prioritized queue), and produces deterministic, template-generated analytical outputs with zero AI or heuristic logic.

**Inputs**: M1 full master dataset (all rows), M2 full enriched dataset (all rows), M3 filtered priority queue, and a configurable `reference_date`.

**Outputs** (returned as a 5-element Tuple):

1. `List[Dict]` — Per-item `analysis_result` for every M3 queue item (nested A1–A6 blocks + `lifecycle_state`)
2. `pd.DataFrame` — G1: Systemic blocker report (14 rows, one per canonical approver)
3. `pd.DataFrame` — G2: Loop detection report (one row per family-sheet combination)
4. `pd.DataFrame` — G3: Risk score per queue item
5. `pd.DataFrame` — G4: Lot health report (one row per source_sheet)

**Authority level**: DERIVED AUTHORITATIVE — all outputs are deterministic functions of M1–M3, valid while `pipeline_run_id` matches. M4 is computed once per pipeline run, persisted, never recomputed on UI action.

**Execution strategy**: 5 sequential phases respecting a strict dependency graph:

- Phase 1: Input validation, approver column discovery, index construction
- Phase 2: G1 global precomputation (systemic blockers) — required before A4
- Phase 3: Per-item loop (A1→A2→A3→A4→A5→A6→lifecycle_state) over M3 queue
- Phase 4: G2 (loops), G3 (risk scores), G4 (lot health) — depend on Phase 2+3
- Phase 5: Output assembly, schema validation, summary logging

**Performance target**: < 2s for 5K rows, < 5s for 10K rows. No O(n²) operations. All lookups via precomputed indexes.

---

## 2. Phase-by-Phase Implementation Plan

### Phase 1: Input Contracts, Indexing & Precomputed Lookups

**Objective**: Validate all inputs, dynamically discover approver columns, and build every lookup index needed for O(1) access throughout M4.

**Inputs**: m1_master, m2_enriched, m3_queue, reference_date

**Outputs produced** (available to all downstream phases):

- `approver_keys`: set of all canonical approver keys found across assigned_approvers
- `approver_col_map`: dict mapping each approver_key to its column names (`{key}_statut`, `{key}_date`)
- `version_index`: dict[doc_version_key → M2 row (as Series or dict)]
- `queue_index`: dict[row_id → M3 queue row]
- `chain_index`: dict[(doc_family_key, source_sheet) → sorted list of M2 rows by ind_sort_order ascending]
- `sheet_index`: dict[source_sheet → list of M3 queue row_ids]
- `latest_index`: dict[(doc_family_key, source_sheet) → M2 row where is_latest=true]

**Dependency ordering**: Must execute first. All subsequent phases depend on these indexes.

**Steps**:

**1.1 Input DataFrame Validation** [SAFEGUARD]

- Verify m1_master, m2_enriched, m3_queue are non-null DataFrames
- Verify m3_queue is not empty (if empty, return early with empty outputs and log WARNING)
- Verify reference_date is a valid date object
- Verify required columns exist in each DataFrame. For M1: at minimum `row_id`, `visa_global`, `date_diffusion`, `date_contractuelle_visa`, `assigned_approvers`, `ind`, `ind_raw`, `titre`, `document`, `document_raw`, `source_sheet`, `source_row`, `lot`, `type_doc`, `row_quality`. For M2: all M1 columns plus `doc_family_key`, `doc_version_key`, `revision_count`, `is_latest`, `previous_version_key`, `is_cross_lot`, `cross_lot_sheets`, `duplicate_flag`, `ind_sort_order`. For M3: all M2 columns plus `priority_score`, `category`, `consensus_type`, `days_since_diffusion`, `days_until_deadline`, `is_overdue`, `days_overdue`, `has_deadline`, `total_assigned`, `total_replied`, `total_pending`, `approvers_vso`, `approvers_vao`, `approvers_ref`, `approvers_hm`, `relevant_approvers`, `missing_approvers`, `blocking_approvers`
- If critical columns missing → log ERROR, raise exception (M4 cannot run without valid inputs)

**1.2 Approver Column Discovery** [SPEC]

- Collect all unique approver keys from the `assigned_approvers` column across the full M2 enriched dataset (not just queue). `assigned_approvers` is a list column; union all lists.
- For each discovered key, verify the expected columns exist: `{KEY}_statut`, `{KEY}_statut_raw`, `{KEY}_date`, `{KEY}_n`. At minimum `{KEY}_statut` must exist; others are optional with graceful fallback.
- Build `approver_col_map`: dict mapping approver_key → dict of column names found.
- Validate against the 14 canonical keys. If a discovered key is not in the canonical set → log WARNING (unexpected approver). If a canonical key is never seen → log INFO (approver not assigned on any sheet).

**1.3 Build version_index** [IMPLEMENTATION]

- Purpose: O(1) lookup of any M2 row by its `doc_version_key`. Required by A4 (chain backward walk) and A5 (previous revision delta).
- Construction: iterate M2 enriched rows, map `doc_version_key` → row data (as dict for serialization-safe access). If duplicate `doc_version_key` values found → log ERROR, keep first occurrence.
- Mandatory index. Without it, A4 and A5 degrade to O(n) per lookup.

**1.4 Build queue_index** [IMPLEMENTATION]

- Purpose: O(1) access to M3 queue row by `row_id`.
- Construction: iterate M3 queue rows, map `row_id` → row data.
- Mandatory index.

**1.5 Build chain_index** [IMPLEMENTATION]

- Purpose: For any (doc_family_key, source_sheet) pair, retrieve the full revision chain sorted by `ind_sort_order` ascending. Required by A4 chain scan and G2 loop detection.
- Construction: group M2 enriched rows by (doc_family_key, source_sheet), sort each group by `ind_sort_order` ascending, store as list of row dicts.
- Mandatory index.

**1.6 Build sheet_index** [IMPLEMENTATION]

- Purpose: For any source_sheet, retrieve all M3 queue items belonging to that lot. Required by G4 lot health aggregation.
- Construction: group M3 queue row_ids by `source_sheet`.
- Mandatory index.

**1.7 Build latest_index** [IMPLEMENTATION]

- Purpose: For any (doc_family_key, source_sheet) pair, retrieve the is_latest=true M2 row. Required by G1 (systemic blocker scope).
- Construction: filter M2 where `is_latest=true`, map (doc_family_key, source_sheet) → row.
- Mandatory index.

**Failure handling**: If index construction fails for any index → log ERROR. version_index and chain_index failure is fatal (A4/A5/G2 cannot function). queue_index failure is fatal. sheet_index and latest_index failure degrades G4 and G1 respectively — return empty DataFrames for affected globals.

---

### Phase 2: Global Precomputations (G1)

**Objective**: Compute G1 (Systemic Blocker Report) and build the `blocker_index` lookup. G1 must be available before any A4 computation because A4 needs systemic blocker lookup.

**Inputs**: M2 enriched full dataset, `latest_index`, `approver_col_map`, canonical approver list, reference_date

**Outputs produced**:

- `g1_report`: pd.DataFrame (14 rows, one per canonical approver) with all Fix #20 schema fields
- `blocker_index`: dict[approver_key → G1 row data] for O(1) lookup from A4

**Dependency ordering**: Must complete before Phase 3 (per-item loop). A4 blocking logic depends on `blocker_index` to determine which rejecting approvers are systemic.

**Steps**:

**2.1 Compute G1: Systemic Blocker Report** [SPEC]

- Scope: All `is_latest=true` rows in M2 (use latest_index values or filter M2 directly).
- For each of the 14 canonical approver keys, compute:
  - `total_latest_assigned`: count of distinct (doc_family_key, source_sheet) pairs where this approver_key is in the row's `assigned_approvers` list, among is_latest rows.
  - `total_responded`: count of distinct (doc_family_key, source_sheet) pairs where `{APPROVER}_statut` is not null (including HM and non-classifiable). This is an activity metric. [SPEC]
  - `total_blocking`: count of distinct (doc_family_key, source_sheet) pairs where `{APPROVER}_statut` = REF on is_latest revision. [SPEC]
  - `blocking_rate`: total_blocking / total_latest_assigned. If total_latest_assigned = 0 → blocking_rate = 0.0. [SPEC]
  - `avg_response_days`: mean of (statut_date − date_diffusion).days across responded items. Null if no valid date pairs. [SPEC]
  - `is_systemic_blocker`: total_blocking >= SYSTEMIC_BLOCKER_THRESHOLD (3). [SPEC]
  - `blocked_families`: list of (doc_family_key, source_sheet) tuples where statut = REF on is_latest. Reflects per-sheet grain — the same family blocked on 3 sheets produces 3 entries, not 1. [SPEC] [PATCHED — V3]
  - `severity`: HIGH if blocking_rate > 0.5, MEDIUM if > 0.25, LOW otherwise. [SPEC]
  - `display_name` [PATCHED — V2]: Looked up from a hardcoded canonical display_name dictionary (sourced from M1 spec §1.3 "Approver canonical dictionary"). No heuristic title-casing. No fallback guessing. The exact mapping is: [SPEC]

    ```
    MOEX_GEMO          → "MOEX GEMO"
    ARCHI_MOX          → "ARCHI MOX"
    BET_STR_TERRELL    → "BET STR-TERRELL"
    BET_GEOLIA_G4      → "BET GEOLIA - G4"
    ACOUSTICIEN_AVLS   → "ACOUSTICIEN AVLS"
    AMO_HQE_LE_SOMMER  → "AMO HQE LE SOMMER"
    BET_POLLUTION_DIE   → "BET POLLUTION DIE"
    SOCOTEC            → "SOCOTEC"
    BET_ELIOTH         → "BET ELIOTH"
    BET_EGIS           → "BET EGIS"
    BET_ASCAUDIT       → "BET ASCAUDIT"
    BET_ASCENSEUR      → "BET ASCENSEUR"
    BET_SPK            → "BET SPK"
    PAYSAGISTE_MUGO    → "PAYSAGISTE MUGO"
    ```

    If an unknown approver key appears (not in the canonical 14 — indicates upstream anomaly) → log ERROR with the unknown key → use the raw approver_key string as display_name. [SAFEGUARD]

**2.2 Build blocker_index** [IMPLEMENTATION]

- Map approver_key → {is_systemic_blocker, total_blocking, blocked_families, severity} for O(1) lookup from A4.

**Failure handling** [SAFEGUARD]: If G1 computation throws → log ERROR, set g1_report to empty DataFrame with correct schema (14 rows, all zero/false/empty defaults), set blocker_index to empty dict. A4 will treat all approvers as non-systemic (conservative safe default — no false positives).

---

### Phase 3: Per-Item Analysis Block Computation

**Objective**: Iterate every item in the M3 queue. For each item, compute A1→A2→A3→A4→A5→A6 in strict order, derive `lifecycle_state`, assemble the composite `analysis_result`, and build the `result_index`.

**Inputs**: M3 queue (iteration source), M2 enriched full dataset (via version_index, chain_index), G1 blocker_index, approver_col_map, reference_date

**Outputs produced**:

- `per_item_results`: List[Dict] — one analysis_result per queue item
- `result_index`: dict[row_id → analysis_result] — for G3 consumption

**Dependency ordering**: Requires Phase 1 (all indexes) and Phase 2 (blocker_index). Must complete before Phase 4 (G2/G3/G4 need per-item results).

**Steps**:

**3.1 Iteration Structure** [IMPLEMENTATION]

- Iterate m3_queue rows. For each row, extract row_id and all needed fields.
- Wrap the entire per-item computation in a try/except. If ANY unhandled exception occurs for an item: apply full-item degraded fallback (all 6 blocks get safe defaults, analysis_degraded=true, all blocks in failed_blocks). Item is NEVER skipped. [SAFEGUARD per Fix #21]

**3.2 A1: Agreement Detection** [SPEC]

- Executed for every item.
- Wrap in per-block try/except. On failure: apply A1 safe defaults, block_status=FAILED. [Fix #21]
- Read the item's `assigned_approvers` list. For each approver_key in the list, read `{KEY}_statut` from the queue row.
- Partition into 5 mutually exclusive primary sets based on statut value:
  - `approve_set`: statut IN (VSO, VAO)
  - `reject_set`: statut = REF
  - `pending_set`: statut IS NULL
  - `hm_set`: statut = HM
  - `non_classifiable_response_set`: statut IS NOT NULL AND statut NOT IN (VSO, VAO, REF, HM) — covers SUS, DEF, FAV, any unknown
- Derive 2 aggregate sets:
  - `opinionated_approvers` = approve_set ∪ reject_set
  - `responded_non_hm_approvers` = approve_set ∪ reject_set ∪ non_classifiable_response_set
- Apply R1–R8 decision rules (first match wins):
  - R1: opinionated empty AND pending empty → NO_DATA
  - R2: opinionated empty AND pending not empty → AWAITING
  - R3: reject empty AND approve not empty AND pending empty → FULL_APPROVAL
  - R4: approve empty AND reject not empty AND pending empty → FULL_REJECTION
  - R5: reject empty AND approve not empty AND pending not empty → PARTIAL_APPROVAL
  - R6: approve empty AND reject not empty AND pending not empty → PARTIAL_REJECTION
  - R7: reject not empty AND approve not empty → CONFLICT
  - R8: fallback → UNKNOWN (log ERROR — should not be reachable)
- Post-check [Fix #1]: validate agreement_type↔M3.consensus_type using the 8-row normative mapping table. Set `consensus_match` = true/false. On mismatch: log ERROR with both values. M3 consensus_type remains authoritative.
- Generate `agreement_detail` template string.
- Populate all A1 output fields: agreement_type, approve_count, reject_count, pending_count, hm_count, non_classifiable_count, approve_list, reject_list, pending_list, non_classifiable_list, agreement_detail, consensus_match, block_status (OK or FAILED).

**3.3 A2: Conflict Detection** [SPEC]

- Wrap in per-block try/except. On failure: apply A2 safe defaults, block_status=FAILED. [Fix #21]
- Trigger: ONLY computed when A1.agreement_type = CONFLICT. Otherwise: conflict_detected=false, all other fields null, block_status=OK. [SPEC]
- When triggered, apply S1–S4 severity rules (first match wins):
  - S1: reject_count >= approve_count AND reject_count >= 2 → HIGH
  - S2: reject_count >= approve_count AND reject_count = 1 → MEDIUM
  - S3: approve_count > reject_count AND pending_count > 0 → MEDIUM
  - S4: approve_count > reject_count AND pending_count = 0 → LOW
- Compute `majority_position`: APPROVE if approve > reject, REJECT if reject > approve, TIED if equal.
- Compute `approvers_against_majority`: if APPROVE majority → reject_list; if REJECT majority → approve_list; if TIED → union of both (all are "against" in a tie). [IMPLEMENTATION]
- [Fix #12]: conflict_severity = operational complexity/decision burden for MOEX, not safety criticality. No code impact — labeling/documentation only.
- Generate `conflict_detail` template string.
- Populate all A2 output fields.

**3.4 A3: Missing Approver Analysis** [SPEC]

- Wrap in per-block try/except. On failure: apply A3 safe defaults, block_status=FAILED. [Fix #21]
- Input: `pending_set` from A1, date_diffusion, date_contractuelle_visa, reference_date.
- If pending_set is empty: total_missing=0, missing_approvers=[], worst_urgency=null, critical_missing=[], block_status=OK. [SAFEGUARD]
- For each approver_key in pending_set:
  - Compute `days_since_diffusion` = (reference_date − date_diffusion).days. Null if date_diffusion is null.
  - Compute `days_past_deadline` = (reference_date − date_contractuelle_visa).days. Null if no deadline. Negative = not yet due.
  - Apply U1–U5 urgency rules (first match wins):
    - U1: days_past_deadline not null AND > 14 → CRITICAL
    - U2: days_past_deadline not null AND > 0 AND <= 14 → HIGH
    - U3: days_past_deadline not null AND <= 0 AND > -3 → MEDIUM
    - U4: days_past_deadline null AND days_since_diffusion not null AND > 21 → MEDIUM
    - U5: all other → LOW
- Compute aggregates: `worst_urgency` (highest across all), `critical_missing` (keys with CRITICAL).
- Urgency ordering for "highest": CRITICAL > HIGH > MEDIUM > LOW. [IMPLEMENTATION]
- Generate `missing_summary` template string.
- Populate all A3 output fields.

**3.5 A4: Blocking Logic** [SPEC]

- Wrap in per-block try/except. On failure: apply A4 safe defaults, block_status=FAILED. [Fix #21]
- Input: A1 reject_count, approve_count, pending_count, M2 revision_count, M2 full dataset (via chain_index and version_index), blocker_index from Phase 2.
- Apply B1–B5 blocking pattern rules (first match wins):
  - B1: reject_count = 0 → NOT_BLOCKED
  - B2: reject_count > 0 AND approve_count = 0 AND pending_count = 0 AND revision_count > 1 → CHRONIC_BLOCK
  - B3: reject_count > 0 AND approve_count = 0 AND pending_count = 0 AND revision_count = 1 → FIRST_REJECTION
  - B4: reject_count > 0 AND approve_count > 0 → PARTIAL_BLOCK
  - B5: reject_count > 0 AND pending_count > 0 AND approve_count = 0 → BLOCK_WITH_PENDING
- If blocking_pattern = CHRONIC_BLOCK → invoke `scan_rejection_chain()` (the shared helper, also used by G2) [Fix #2, Fix #3]:
  - Starting from the current item, walk backward via `previous_version_key`.
  - For each revision in the backward walk, look up its row in version_index.
  - Qualification: A revision is a rejection if (a) visa_global = REF, or (b) visa_global is null AND reject_set is not empty AND approve_set is empty (for that revision's approvers). Explicit: zero opinionated approvers (both approve and reject empty) does NOT qualify — that falls to BK3.
  - Chain breaks (BK1–BK4, stop counting):
    - BK1: visa_global IN (VAO, VSO, SUS, DEF, FAV)
    - BK2: visa_global null AND at least one approver statut IN (VSO, VAO)
    - BK3: visa_global null AND all assigned approver statuts are null
    - BK4: previous_version_key is null
  - `consecutive_rejections` = count from latest backward (inclusive) before first break. Minimum 2 for CHRONIC_BLOCK (guaranteed by B2 requiring revision_count > 1, but validate). [SAFEGUARD]
- For each approver in `blocking_approvers` (reject_list from A1), check blocker_index for `is_systemic_blocker`. Populate `is_systemic_blocker` as the subset that are systemic. [SPEC]
- Generate `blocking_detail` template string (includes chronic block details when applicable).
- Populate all A4 output fields: is_blocked, blocking_pattern, blocking_approvers, is_systemic_blocker, consecutive_rejections, blocking_detail, block_status.

**3.6 A5: Revision Delta** [SPEC]

- Wrap in per-block try/except. On failure: apply A5 safe defaults, block_status=FAILED. [Fix #21]
- Trigger: Only computed when `previous_version_key` is not null. Otherwise: has_previous=false, all delta fields null/0, block_status=OK.
- Lookup previous revision's row via version_index using `previous_version_key` → match on `doc_version_key`. If not found: has_previous=false, log WARNING. [SAFEGUARD]
- Compare current vs previous:
  - `visa_global_change`: compare current and previous visa_global. Format as "X → Y" if different, null if same.
  - For each approver in assigned_approvers: compare current `{KEY}_statut` to previous `{KEY}_statut`. Record {approver_key, previous_statut, current_statut, changed: bool}.
  - `total_changed`: count where changed=true.
  - `new_responses`: count where previous=null AND current!=null.
  - `lost_responses`: count where previous!=null AND current=null.
  - `reversals`: count where previous IN (VSO,VAO) AND current=REF, OR previous=REF AND current IN (VSO,VAO). [SPEC]
- Generate `delta_summary` template string.
- Populate all A5 output fields.

**3.7 A6: Time Analysis** [SPEC]

- Wrap in per-block try/except. On failure: apply A6 safe defaults, block_status=FAILED. [Fix #21]
- Input: date_diffusion, date_contractuelle_visa, reference_date. Can reuse M3 precomputed values: days_since_diffusion, days_until_deadline, is_overdue, days_overdue, has_deadline.
- Apply D1–D8 deadline_status rules (first match wins):
  - D1: has_deadline=false → NO_DEADLINE
  - D2: days_until_deadline > 14 → COMFORTABLE
  - D3: days_until_deadline > 7 AND <= 14 → APPROACHING
  - D4: days_until_deadline > 0 AND <= 7 → URGENT
  - D5: days_until_deadline = 0 → DUE_TODAY
  - D6: days_overdue > 0 AND <= 14 → OVERDUE
  - D7: days_overdue > 14 AND <= 30 → SEVERELY_OVERDUE
  - D8: days_overdue > 30 → CRITICALLY_OVERDUE
- Apply age_bracket rules (first match wins):
  - G1 (rule label): days_since_diffusion null → UNKNOWN_AGE
  - G2 (rule label): days_since_diffusion <= 7 → FRESH
  - G3 (rule label): days_since_diffusion <= 21 → NORMAL
  - G4 (rule label): days_since_diffusion <= 60 → AGING
  - G5 (rule label): days_since_diffusion > 60 → STALE
- Generate `time_summary` template string.
- Populate all A6 output fields.

**3.7b analysis_degraded and failed_blocks Derivation** [SPEC — Fix #21] [PATCHED — V2]

- Computed per item, immediately after all 6 blocks (A1–A6) have finalized (whether successfully or via safe defaults).
- `analysis_degraded` = `true` if and only if any of the 6 blocks (A1–A6) has `block_status = FAILED`. Otherwise `false`.
- `failed_blocks` = ordered list of block IDs (from ["A1", "A2", "A3", "A4", "A5", "A6"]) where `block_status = FAILED`. Empty list `[]` if none failed.
- Both values are finalized at this point and are immutable for the remainder of the per-item pipeline. `analysis_degraded` is consumed by the immediately following lifecycle_state derivation (ON_HOLD if degraded). `failed_blocks` is included in the final analysis_result assembly.

**3.8 lifecycle_state Derivation** [SPEC — V2.2.2 E2]

- Computed per item, immediately after step 3.7b finalizes `analysis_degraded`. Part of the per-item loop, NOT a separate global step.
- Derive from M3 `consensus_type`, M2 `revision_count`, and `analysis_degraded`:
  - consensus_type = NOT_STARTED → NOT_STARTED
  - consensus_type = INCOMPLETE → WAITING_RESPONSES
  - consensus_type = ALL_APPROVE → READY_TO_ISSUE
  - consensus_type = ALL_REJECT AND revision_count = 1 → READY_TO_REJECT
  - consensus_type = MIXED → NEEDS_ARBITRATION
  - consensus_type = ALL_REJECT AND revision_count > 1 → CHRONIC_BLOCKED
  - analysis_degraded = true → ON_HOLD (this takes priority check — see ordering below)
- Evaluation order [IMPLEMENTATION]: Check `analysis_degraded` first. If true → ON_HOLD regardless of consensus_type. Otherwise apply the consensus_type mapping. This ensures degraded items are always ON_HOLD.
- [SAFEGUARD] ALL_HM consensus_type guard: ALL_HM items should never appear in the M3 queue (M3 Step 1 excludes all-HM items). If an item with consensus_type = ALL_HM is encountered → log ERROR ("ALL_HM item found in M3 queue — should have been excluded by M3 filtering"), default lifecycle_state to ON_HOLD.
- Validation [SPEC — V2.2.2 E2, Phase 2 spec §3.2–3.3] [PATCHED — V2] [PATCHED — V3]: lifecycle_state must never contradict M3 category. M3 category is authoritative. Define the following compatibility map derived from the Phase 2 spec §3.3 mapping table:

  | lifecycle_state | Compatible M3 category |
  |---|---|
  | NOT_STARTED | NOT_STARTED |
  | WAITING_RESPONSES | WAITING |
  | READY_TO_ISSUE | EASY_WIN_APPROVE |
  | READY_TO_REJECT | FAST_REJECT |
  | NEEDS_ARBITRATION | CONFLICT |
  | CHRONIC_BLOCKED | BLOCKED |
  | ON_HOLD | (any — ON_HOLD is always compatible) |

  If the computed lifecycle_state contradicts the M3 category per this map → log ERROR with both values (computed lifecycle_state and M3 category) → **override** lifecycle_state to the value implied by the M3 category using the reverse mapping:

  | M3 category | Implied lifecycle_state |
  |---|---|
  | EASY_WIN_APPROVE | READY_TO_ISSUE |
  | FAST_REJECT | READY_TO_REJECT |
  | BLOCKED | CHRONIC_BLOCKED |
  | CONFLICT | NEEDS_ARBITRATION |
  | WAITING | WAITING_RESPONSES |
  | NOT_STARTED | NOT_STARTED |

  The conflicting computed value is **discarded**, not preserved. M3 category is the authoritative source for resolving contradictions.

**3.9 Per-Item Result Assembly** [SPEC]

- Assemble the composite `analysis_result` dict:
  - row_id, agreement (A1 output), conflict (A2), missing (A3), blocking (A4), delta (A5), time (A6), lifecycle_state (from step 3.8), analysis_degraded (from step 3.7b), failed_blocks (from step 3.7b).
- Append to `per_item_results` list.
- Add to `result_index`: row_id → analysis_result.

**Failure handling**: As stated in 3.1, the entire per-item block is wrapped in try/except. Individual block failures produce safe defaults for that block only (Fix #21). Item-level failure produces all-block safe defaults.

---

### Phase 4: Global Post-Item Analyses (G2, G3, G4)

**Objective**: Compute G2 (loop detection), G3 (risk scores), G4 (lot health). These depend on Phase 2 (G1) and Phase 3 (per-item results).

**Inputs**: chain_index, version_index, blocker_index (G1), result_index (per-item), M2 enriched, M3 queue

**Outputs produced**:

- `g2_report`: pd.DataFrame — loop detection report
- `loop_index`: dict[(doc_family_key, source_sheet) → G2 row] — consumed by G3
- `g3_report`: pd.DataFrame — risk score per queue item
- `g4_report`: pd.DataFrame — lot health report

**Dependency ordering**: G2 can run as soon as Phase 3 completes (it reuses the chain helper but not per-item results). G3 depends on G1, G2, and per-item results. G4 depends on G3. Canonical order: G2 → G3 → G4.

**Steps**:

**4.1 G2: Loop Detection Report** [SPEC]

- Wrap in try/except. On failure: log ERROR, return empty DataFrame with correct schema. [SAFEGUARD]
- Scope: All (doc_family_key, source_sheet) combinations where is_latest=true in M2.
- For each combination, invoke `scan_rejection_chain()` — the EXACT SAME function used by A4. [Fix #3] This is a hard spec requirement. Two separate implementations are a spec violation.
- Populate one row per family-sheet:
  - doc_family_key, source_sheet, document (from latest row), titre (from latest row)
  - `is_looping`: consecutive_rejections >= 2
  - `loop_length`: = consecutive_rejections. 0 if not looping.
  - `loop_start_ind`: IND of the first revision in the rejection sequence (earliest in chain). Null if not looping.
  - `loop_end_ind`: IND of the latest revision. Null if not looping.
  - `persistent_blockers`: approvers with statut=REF on ALL revisions within the loop sequence. Empty if not looping.
  - `latest_visa_global`: visa_global of the latest revision
- Build `loop_index`: (doc_family_key, source_sheet) → G2 row data. [IMPLEMENTATION]

**4.2 G3: Risk Score Per Item** [SPEC]

- Wrap in try/except. On failure: log ERROR, return empty DataFrame with correct schema. [SAFEGUARD]
- [Fix #13]: Scoped to M3 queue items only.
- For each queue item, evaluate 6 risk factors:
  - F1 (weight 3): days_overdue > 14 — from A6/M3
  - F2 (weight 3): is_looping=true — lookup (doc_family_key, source_sheet) in loop_index from G2
  - F3 (weight 2): any blocking_approver is systemic — from A4.is_systemic_blocker list (non-empty)
  - F4 (weight 2): revision_count > 3 — from M2
  - F5 (weight 1): agreement_type=CONFLICT — from A1 in result_index
  - F6 (weight 1): has_deadline=false AND days_since_diffusion > 30 — from M3/A6
- `risk_score` = sum of weights for applicable factors. Range [0, 12].
- `is_high_risk` = risk_score >= HIGH_RISK_THRESHOLD (3).
- `contributing_factors`: list of factor IDs triggered (e.g. ["F1","F3"]).
- `factor_details`: list of {factor_id, label, weight, condition_met} for all 6 factors.
- Populate one row per queue item.

**4.3 G4: Lot Health Report** [SPEC]

- Wrap in try/except. On failure: log ERROR, return empty DataFrame with correct schema. [SAFEGUARD]
- Scope: One row per source_sheet (all sheets present in M2, expected ~25).
- For each source_sheet:
  - `total_documents`: count of is_latest=true rows in M2 for this sheet.
  - `total_pending`: count of queue items for this lot (from sheet_index).
  - `total_overdue`: pending items where is_overdue=true.
  - `total_high_risk`: items from G3 with risk_score >= HIGH_RISK_THRESHOLD for this sheet.
  - `category_distribution`: count per M3 category (EASY_WIN_APPROVE, BLOCKED, FAST_REJECT, CONFLICT, WAITING, NOT_STARTED).
  - `approval_rate`: count of is_latest rows where visa_global IN (VAO, VSO) / count of is_latest rows where visa_global is not null. 0.0 if denominator is 0.
  - `avg_priority_score`: mean priority_score for pending items. Null if total_pending=0.
  - `avg_days_pending`: mean days_since_diffusion for pending items. Null if no dates.
  - `is_high_risk_cluster`: total_high_risk >= HIGH_RISK_CLUSTER_THRESHOLD (5).
  - `health_score`: 100 − (overdue_pct×40 + high_risk_pct×30 + (1−approval_rate)×30), clamped [0, 100]. Where overdue_pct = total_overdue / max(total_pending, 1), high_risk_pct = total_high_risk / max(total_pending, 1).

---

### Phase 5: Output Assembly, Schema Validation & Logging

**Objective**: Assemble the final 5-element Tuple, validate all schemas, produce summary log.

**Inputs**: per_item_results, g1_report, g2_report, g3_report, g4_report

**Outputs produced**: The final Tuple return value + validation report + summary log.

**Steps**:

**5.1 Per-Item Schema Validation** [SPEC — GP9]

- For each analysis_result in per_item_results:
  - Verify all required top-level keys exist (row_id, agreement, conflict, missing, blocking, delta, time, lifecycle_state, analysis_degraded, failed_blocks).
  - For each block, verify all required sub-fields exist with correct types.
  - Validate all enum values against the centralized registry (GP8). Out-of-enum → log ERROR.
  - If validation fails for an item: do NOT remove it. Log ERROR. Mark as degraded if not already. [SAFEGUARD]

**5.2 Global DataFrame Schema Validation** [SPEC — GP9]

- For each of G1, G2, G3, G4:
  - Verify all required columns exist with correct dtypes.
  - Verify not-null constraints (per Fix #20 schema annotations).
  - Validate enum columns.
  - If validation fails: log ERROR, do NOT return invalid DataFrame. Apply column-level defaults where possible. [SAFEGUARD]

**5.3 Summary Logging** [IMPLEMENTATION]

- Total items processed, total items degraded, blocks failed (count per block type), consensus mismatches, G1/G2/G3/G4 row counts, systemic blockers found, loops detected, high-risk items found, high-risk clusters found.
- Execution timing (total M4 wall-clock, per-phase timing). [SAFEGUARD — GP5 monitoring]

**5.4 Return** [SPEC]

- Return (per_item_results, g1_report, g2_report, g3_report, g4_report).

---

## 3. Precomputed Indexes and Lookup Structures

| Index | Key → Value | Purpose | When Built | Mandatory? |
|---|---|---|---|---|
| `version_index` | doc_version_key → M2 row (dict) | A4 chain backward walk, A5 delta lookup | Phase 1 (step 1.3) | Yes — correctness. Without it, A4/A5 require O(n) scan per lookup. |
| `queue_index` | row_id → M3 row (dict) | Per-item field access during Phase 3 | Phase 1 (step 1.4) | Yes — correctness. Needed for structured access. |
| `chain_index` | (doc_family_key, source_sheet) → sorted list of M2 rows | A4 backward chain scan, G2 loop detection | Phase 1 (step 1.5) | Yes — correctness. Enables ordered chain traversal. |
| `sheet_index` | source_sheet → list of M3 queue row_ids | G4 lot health aggregation | Phase 1 (step 1.6) | Yes — performance. Without it, G4 requires repeated filtering. |
| `latest_index` | (doc_family_key, source_sheet) → M2 is_latest row | G1 scope (all is_latest rows), G4 total_documents count | Phase 1 (step 1.7) | Yes — correctness for G1. |
| `blocker_index` | approver_key → G1 metrics dict | A4 systemic blocker lookup | Phase 2 (after G1) | Yes — correctness. A4 must know if a rejecting approver is systemic. |
| `result_index` | row_id → per-item analysis_result dict | G3 risk factor lookup (A1, A4, A6 fields) | Phase 3 (built during loop, step 3.9) | Yes — correctness. G3 needs per-item analysis results. |
| `loop_index` | (doc_family_key, source_sheet) → G2 row | G3 risk factor F2 lookup | Phase 4 (after G2, step 4.1) | Yes — correctness. G3 F2 depends on loop status. |
| `approver_col_map` | approver_key → {statut_col, date_col} | All A-blocks that read approver statuts | Phase 1 (step 1.2) | Yes — correctness. Dynamic column discovery. |

---

## 4. Detailed Block-by-Block Plan

### A1: Agreement Detection

**Inputs**: item's `assigned_approvers` list, all `{APPROVER}_statut` values for those approvers (read via approver_col_map).

**Logic**:

1. [SPEC] Read assigned_approvers. For each key, read statut value.
2. [SPEC] Partition into 5 primary sets: approve_set (VSO/VAO), reject_set (REF), pending_set (null), hm_set (HM), non_classifiable_response_set (not null AND not in VSO/VAO/REF/HM).
3. [SPEC] Derive: opinionated_approvers = approve_set ∪ reject_set. responded_non_hm_approvers = approve_set ∪ reject_set ∪ non_classifiable_response_set.
4. [SPEC] Evaluate R1–R8 in order, first match wins. R1: opinionated empty AND pending empty → NO_DATA. R2: opinionated empty AND pending not empty → AWAITING. R3: reject empty AND approve not empty AND pending empty → FULL_APPROVAL. R4: approve empty AND reject not empty AND pending empty → FULL_REJECTION. R5: reject empty AND approve not empty AND pending not empty → PARTIAL_APPROVAL. R6: approve empty AND reject not empty AND pending not empty → PARTIAL_REJECTION. R7: reject not empty AND approve not empty → CONFLICT. R8: fallback → UNKNOWN.
5. [SPEC — Fix #1] Validate agreement_type ↔ consensus_type mapping. Set consensus_match accordingly. Log ERROR on mismatch.
6. [SPEC] Generate agreement_detail from template.
7. [SAFEGUARD] [PATCHED — V2] If any assigned_approvers key has no matching `{KEY}_statut` column in the DataFrame → this is an **upstream schema anomaly** (M1/M2 did not produce the expected columns), NOT an M4 computation failure. Handle as follows:
   - Log **ERROR** (not WARNING) with full context: approver_key, row_id, expected column name (`{KEY}_statut`), and the explicit note that the approver is being treated as pending due to the missing column.
   - Treat the approver's statut as null → the approver enters `pending_set`. The pipeline continues deterministically without crashing.
   - Do **NOT** set `analysis_degraded = true` for this. `analysis_degraded` has Fix #21 semantics (M4's own block computation failed). A missing upstream column is an input schema issue, not an M4 computation failure. Conflating these would pollute the degradation signal.

**Safe defaults on failure** [Fix #21]: agreement_type=UNKNOWN, all counts=0, all lists=[], agreement_detail="Analyse indisponible", consensus_match=false, block_status=FAILED.

---

### A2: Conflict Detection

**Inputs**: A1.agreement_type, A1.approve_count, A1.reject_count, A1.pending_count, A1.approve_list, A1.reject_list.

**Logic**:

1. [SPEC] If A1.agreement_type != CONFLICT → conflict_detected=false, all fields null, block_status=OK. Return early.
2. [SPEC] Apply S1–S4 first match: S1: reject>=approve AND reject>=2 → HIGH. S2: reject>=approve AND reject=1 → MEDIUM. S3: approve>reject AND pending>0 → MEDIUM. S4: approve>reject AND pending=0 → LOW.
3. [SPEC] majority_position: APPROVE if approve>reject, REJECT if reject>approve, TIED if equal.
4. [IMPLEMENTATION] approvers_against_majority: APPROVE→reject_list, REJECT→approve_list, TIED→both lists merged.
5. [SPEC] Generate conflict_detail template.

**Safe defaults on failure** [Fix #21]: conflict_detected=false, severity=null, majority_position=null, approvers_against_majority=[], conflict_detail="Analyse indisponible", block_status=FAILED.

---

### A3: Missing Approver Analysis

**Inputs**: A1.pending_set, item's date_diffusion, date_contractuelle_visa, reference_date.

**Logic**:

1. [SAFEGUARD] If pending_set empty → total_missing=0, all defaults, block_status=OK. Return early.
2. [SPEC] For each pending approver: compute days_since_diffusion (null if no date), days_past_deadline (null if no deadline, negative if not yet due).
3. [SPEC] Apply U1–U5 per approver (first match): U1: past_deadline>14 → CRITICAL. U2: past_deadline>0 AND <=14 → HIGH. U3: past_deadline<=0 AND >-3 → MEDIUM. U4: past_deadline null AND since_diffusion>21 → MEDIUM. U5: else → LOW.
4. [IMPLEMENTATION] Urgency ordering: CRITICAL(4) > HIGH(3) > MEDIUM(2) > LOW(1). worst_urgency = max across all.
5. [SPEC] critical_missing = keys with CRITICAL urgency.
6. [SPEC] Generate missing_summary template.

**Safe defaults on failure** [Fix #21]: missing_approvers=[], total_missing=0, worst_urgency=null, critical_missing=[], missing_summary="Analyse indisponible", block_status=FAILED.

---

### A4: Blocking Logic

**Inputs**: A1.reject_count/approve_count/pending_count/reject_list, M2.revision_count, chain_index, version_index, blocker_index, item's assigned_approvers + approver_col_map.

**Logic**:

1. [SPEC] Apply B1–B5 first match: B1: reject=0 → NOT_BLOCKED. B2: reject>0 AND approve=0 AND pending=0 AND revision_count>1 → CHRONIC_BLOCK. B3: reject>0 AND approve=0 AND pending=0 AND revision_count=1 → FIRST_REJECTION. B4: reject>0 AND approve>0 → PARTIAL_BLOCK. B5: reject>0 AND pending>0 AND approve=0 → BLOCK_WITH_PENDING.
2. [SPEC — Fix #2] If CHRONIC_BLOCK → call `scan_rejection_chain(item_row, version_index, chain_index, approver_col_map)`. This function walks backward from current item via previous_version_key. For each revision: (a) Check chain break conditions BK1–BK4. If any break → stop. (b) Check rejection qualification: visa_global=REF, OR (visa_global null AND reject_set non-empty AND approve_set empty). Explicit: zero opinionated (both empty) is NOT a rejection — it's BK3. Count consecutive_rejections (inclusive of current).
3. [SAFEGUARD] Validate consecutive_rejections >= 2 when CHRONIC_BLOCK (implied by revision_count>1 + all-reject, but verify).
4. [SPEC] For each approver in reject_list, lookup blocker_index → is_systemic_blocker. Build is_systemic_blocker subset list.
5. [SPEC] Generate blocking_detail template.

**Safe defaults on failure** [Fix #21]: is_blocked=false, blocking_pattern=NOT_BLOCKED, blocking_approvers=[], is_systemic_blocker=[], consecutive_rejections=null, blocking_detail="Analyse indisponible", block_status=FAILED.

---

### A5: Revision Delta

**Inputs**: item's previous_version_key, version_index, assigned_approvers, approver_col_map.

**Logic**:

1. [SPEC] If previous_version_key is null → has_previous=false, all delta fields null/0, block_status=OK. Return early.
2. [SPEC] Lookup previous revision in version_index by previous_version_key. If not found → has_previous=false, log WARNING. Return.
3. [SPEC] Compare visa_global: current vs previous. Format "X → Y" if different, null if same.
4. [SPEC] For each approver in assigned_approvers: compare current statut vs previous statut. Record change details.
5. [SPEC] Count total_changed, new_responses (null→non-null), lost_responses (non-null→null), reversals (approve↔reject flips: previous IN VSO/VAO AND current=REF, or previous=REF AND current IN VSO/VAO).
6. [SPEC] Generate delta_summary template.

**Safe defaults on failure** [Fix #21]: has_previous=false, previous_ind=null, visa_global_change=null, approver_changes=[], total_changed=0, new_responses=0, lost_responses=0, reversals=0, delta_summary="Analyse indisponible", block_status=FAILED.

---

### A6: Time Analysis

**Inputs**: days_since_diffusion, days_until_deadline, is_overdue, days_overdue, has_deadline (from M3 precomputed values), reference_date.

**Logic**:

1. [SPEC] Apply D1–D8 deadline_status (first match): D1: no deadline → NO_DEADLINE. D2: until>14 → COMFORTABLE. D3: >7 AND <=14 → APPROACHING. D4: >0 AND <=7 → URGENT. D5: =0 → DUE_TODAY. D6: overdue>0 AND <=14 → OVERDUE. D7: >14 AND <=30 → SEVERELY_OVERDUE. D8: >30 → CRITICALLY_OVERDUE.
2. [SPEC] Apply age rules (first match): null → UNKNOWN_AGE. <=7 → FRESH. <=21 → NORMAL. <=60 → AGING. >60 → STALE.
3. [SPEC] Generate time_summary template.

**Safe defaults on failure** [Fix #21]: days_since_diffusion=null, days_until_deadline=null, is_overdue=false, days_overdue=0, deadline_status=NO_DEADLINE, age_bracket=UNKNOWN_AGE, time_summary="Analyse indisponible", block_status=FAILED.

---

### lifecycle_state

**Inputs**: M3 consensus_type, M2 revision_count, analysis_degraded flag.

**Logic**:

1. [IMPLEMENTATION] Check analysis_degraded first. If true → ON_HOLD.
2. [SPEC — V2.2.2 E2] Otherwise map consensus_type: NOT_STARTED→NOT_STARTED, INCOMPLETE→WAITING_RESPONSES, ALL_APPROVE→READY_TO_ISSUE, ALL_REJECT AND revision_count=1→READY_TO_REJECT, MIXED→NEEDS_ARBITRATION, ALL_REJECT AND revision_count>1→CHRONIC_BLOCKED.
3. [SAFEGUARD] If consensus_type = ALL_HM → log ERROR ("ALL_HM in M3 queue — should have been excluded"), default to ON_HOLD.
4. [SAFEGUARD] If consensus_type matches no rule and is not ALL_HM → log ERROR, default to ON_HOLD.
5. [SPEC — Phase 2 spec §3.2–3.3] [PATCHED — V2] Validate lifecycle_state against M3 category using the compatibility map (see Section 3.8). If contradiction detected → log ERROR → **override** lifecycle_state to the value implied by the authoritative M3 category via the reverse mapping. The conflicting computed value is discarded.

---

### G1: Systemic Blocker Report

(Detailed in Phase 2 above.)

**Inputs**: M2 enriched (is_latest rows), approver_col_map, canonical approver list, reference_date.

**Logic**: [SPEC — all per section 2.1 above]

---

### G2: Loop Detection Report

(Detailed in Phase 4 step 4.1 above.)

**Inputs**: chain_index, version_index, approver_col_map, latest_index.

**Logic**:

1. [SPEC] For each (doc_family_key, source_sheet) with is_latest=true, call `scan_rejection_chain()` — same function as A4. [Fix #3]
2. [SPEC] is_looping = consecutive_rejections >= 2.
3. [SPEC] If looping: compute loop_start_ind (earliest IND in sequence), loop_end_ind (latest), persistent_blockers (REF in all loop revisions).
4. [IMPLEMENTATION] persistent_blockers computation: for each revision in the loop sequence, collect the set of approvers with statut=REF. persistent_blockers = intersection of all these sets.

---

### G3: Risk Score Per Item

(Detailed in Phase 4 step 4.2 above.)

**Inputs**: M3 queue, result_index, blocker_index (G1), loop_index (G2), M2 enriched.

**Logic**: [SPEC — all 6 factors with weights as defined in spec]

---

### G4: Lot Health Report

(Detailed in Phase 4 step 4.3 above.)

**Inputs**: M2 enriched, M3 queue, G3 report, sheet_index, latest_index.

**Logic**: [SPEC — all fields and health_score formula as defined in spec]

---

## 5. Helper Function Decomposition

| # | Function Name | Responsibility | Used By |
|---|---|---|---|
| 1 | `validate_m4_inputs` | Verify M1/M2/M3 DataFrames have required columns and types; verify reference_date is valid. Raises on fatal issues. | Phase 1 |
| 2 | `discover_approver_columns` | Scan assigned_approvers across M2, build approver_col_map, validate against canonical set. | Phase 1 |
| 3 | `build_version_index` | Construct doc_version_key → M2 row dict. Detect/log duplicate keys. | Phase 1 |
| 4 | `build_chain_index` | Group M2 rows by (doc_family_key, source_sheet), sort by ind_sort_order asc. | Phase 1 |
| 5 | `build_queue_index` | Construct row_id → M3 row dict. | Phase 1 |
| 6 | `build_sheet_index` | Group M3 queue row_ids by source_sheet. | Phase 1 |
| 7 | `build_latest_index` | Filter M2 is_latest=true, map (doc_family_key, source_sheet) → row. | Phase 1 |
| 8 | `compute_g1_blocker_report` | Iterate 14 canonical approvers over is_latest rows, compute all G1 fields per Fix #20 schema. Return DataFrame + blocker_index. | Phase 2 |
| 9 | `partition_approver_sets` | Given item row and approver_col_map, partition assigned_approvers into 5 primary sets + 2 derived aggregates. Return named structure. | A1 |
| 10 | `compute_agreement` | Apply R1–R8 on partitioned sets, validate Fix #1 mapping, generate template. Return A1 output dict. | A1 |
| 11 | `compute_conflict` | Apply S1–S4 severity, compute majority/dissenters, generate template. Return A2 output dict. | A2 |
| 12 | `compute_missing_approvers` | Apply U1–U5 per pending approver, aggregate worst_urgency/critical, generate template. Return A3 output dict. | A3 |
| 13 | `scan_rejection_chain` | Shared chain scanner: walk backward from a given row via previous_version_key, apply rejection qualification + BK1–BK4 break rules, return consecutive_rejections count and the list of qualifying revision rows. Used by BOTH A4 and G2. Single implementation — Fix #3 compliance. | A4, G2 |
| 14 | `compute_blocking` | Apply B1–B5 pattern, call scan_rejection_chain if CHRONIC_BLOCK, lookup blocker_index for systemic status, generate template. Return A4 output dict. | A4 |
| 15 | `compute_revision_delta` | Lookup previous revision in version_index, compare visa_global and per-approver statuts, count changes/new/lost/reversals, generate template. Return A5 output dict. | A5 |
| 16 | `compute_time_analysis` | Apply D1–D8 deadline_status + age bracket rules, generate template. Return A6 output dict. | A6 |
| 17 | `derive_lifecycle_state` | Map consensus_type + revision_count + analysis_degraded to lifecycle_state enum. Validate against M3 category. | Per-item (Phase 3) |
| 18 | `assemble_analysis_result` | Combine A1–A6 outputs + lifecycle_state into composite dict. Set analysis_degraded and failed_blocks. | Per-item (Phase 3) |
| 19 | `apply_block_safe_defaults` | Given a block ID (A1–A6), return the exact safe default dict per Fix #21 table. | All A-blocks (on failure) |
| 20 | `compute_g2_loop_report` | Iterate all is_latest family-sheet combos, call scan_rejection_chain for each, compute persistent_blockers/loop IND range. Return DataFrame + loop_index. | Phase 4 |
| 21 | `compute_g3_risk_scores` | Evaluate 6 risk factors per queue item using result_index, blocker_index, loop_index. Return DataFrame. | Phase 4 |
| 22 | `compute_g4_lot_health` | Aggregate per-sheet metrics (totals, rates, health_score formula). Return DataFrame. | Phase 4 |
| 23 | `validate_analysis_result_schema` | Verify a single analysis_result dict has all required keys/types/enums. Return list of validation errors. | Phase 5 |
| 24 | `validate_global_dataframe_schema` | Verify a G1/G2/G3/G4 DataFrame against its expected column schema (Fix #20). Return list of validation errors. | Phase 5 |
| 25 | `validate_enum_value` | Check a value against the allowed enum set. Log ERROR if invalid. | All blocks, Phase 5 |
| 26 | `log_m4_summary` | Produce summary statistics: items processed, degraded count, failed blocks by type, consensus mismatches, G1–G4 row counts, timing. | Phase 5 |
| 27 | `run_module4` | Top-level orchestrator: Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5. Returns the final Tuple. | Entry point |

**Notes on decomposition**:

- `scan_rejection_chain` (function 13) is the critical shared function mandated by Fix #3. It must accept a starting row (or its identifiers) and return both the consecutive_rejections count and the list of revision rows in the chain. A4 uses the count + the revision list (to identify blockers). G2 uses the count + the revision list (to identify persistent_blockers and loop IND range).
- `partition_approver_sets` (function 9) is separated from `compute_agreement` (function 10) because the partitioned sets are consumed by A2 (approve/reject counts), A3 (pending_set), A4 (reject_count/approve_count/pending_count + reject_list), and A5 (assigned_approvers iteration). The sets are computed once and passed downstream.
- `apply_block_safe_defaults` (function 19) is a lookup function keyed by block ID. It returns a frozen dict matching the Fix #21 safe defaults table exactly. No logic — pure data.

---

## 6. Validation and Logging Strategy

### Schema Validation — analysis_result (GP9)

**Approach** [IMPLEMENTATION]: Define an expected schema as a dict mapping field paths to expected types. For each analysis_result:

1. Verify top-level keys: row_id (str), agreement (dict), conflict (dict), missing (dict), blocking (dict), delta (dict), time (dict), lifecycle_state (str/enum), analysis_degraded (bool), failed_blocks (list).
2. For each block sub-dict, verify all expected keys exist and have the correct type. List fields must be lists (not null). Nullable fields are allowed to be null. Count fields must be int >= 0.
3. Validate all enum fields against the GP8 registry.
4. On validation failure: log ERROR with row_id, field path, expected type, actual value. Do NOT remove the item. Set analysis_degraded=true if not already.

### Schema Validation — G1–G4 DataFrames (GP9)

**Approach** [IMPLEMENTATION]: Define per-report column schemas (name, dtype, nullable flag) matching Fix #20 annotations.

- G1: 14 rows expected (one per canonical approver). Columns: approver_key (str, not null), display_name (str, not null), total_latest_assigned (int, not null), total_responded (int, not null), total_blocking (int, not null), blocking_rate (float, not null), avg_response_days (float, nullable), is_systemic_blocker (bool, not null), blocked_families (list, not null), severity (enum str, not null).
- G2: Variable rows. All not-null fields per Fix #20 schema.
- G3: One row per queue item. All not-null fields per Fix #20 schema.
- G4: One row per source_sheet. All not-null fields per Fix #20 schema.

Validation [PATCHED — V2] [PATCHED — V3]: check column presence, dtype compatibility, null violations. Log ERROR for each violation. Apply the following deterministic fallback rules (no discretionary judgment — every fallback is traceable to a numbered rule):

1. **Missing nullable column** (any type) → add column, fill with null. [SAFEGUARD]
2. **Missing non-nullable numeric column** (int/float) → add column, fill with defined safe default: `0` for counts, `0.0` for rates/scores. [SAFEGUARD]
3. **Missing non-nullable boolean column** → add column, fill with `false`. [SAFEGUARD]
4. **Missing non-nullable list column** → add column, fill with empty list `[]`. [SAFEGUARD]
5. **Missing non-nullable enum column** (agreement_type, blocking_pattern, deadline_status, age_bracket, severity, lifecycle_state, consensus_type, category, majority_position, conflict_severity, worst_urgency) → add column, fill with the defined safe enum fallback for that field (same defaults as Fix #21 — e.g., `UNKNOWN` for agreement_type, `NOT_BLOCKED` for blocking_pattern, `NO_DEADLINE` for deadline_status, `UNKNOWN_AGE` for age_bracket, `LOW` for severity). [SAFEGUARD]
6. **Missing required identifier/string column** (approver_key, display_name, source_sheet, doc_family_key, document, titre, row_id, doc_version_key, or any other non-enum string field) with no defined safe default → log ERROR, do NOT invent a default. Return an empty schema-valid DataFrame (correct columns, zero rows) for that entire report (G1/G2/G3/G4). [SAFEGUARD]
7. **Existing column with invalid enum value** → replace with the defined safe enum fallback for that field, log ERROR per invalid value. [SAFEGUARD]

### Enum Validation (GP8)

**Approach** [IMPLEMENTATION]: Maintain a central dict of enum_name → set of allowed values. Before setting any enum field, call `validate_enum_value(value, enum_name)`. If value not in set → log ERROR with context (block, field, value), substitute a safe default (typically the most conservative value: UNKNOWN for agreement_type, NOT_BLOCKED for blocking_pattern, etc.).

### Error Logging Format

All M4 log entries should include:

- `pipeline_run_id` (if available from context)
- `module`: "M4"
- `phase`: Phase number (1–5)
- `block`: Block ID (A1–A6, G1–G4) or null for phase-level
- `row_id`: Item identifier (null for global operations)
- `level`: ERROR / WARNING / INFO
- `message`: Human-readable description
- `details`: Structured context (field names, values, expected vs actual)

**Specific log events**:

- Per-block failure: level=ERROR, block=<id>, message="Block computation failed", details={exception, safe_defaults_applied=true}
- Consensus mismatch (Fix #1): level=ERROR, block=A1, message="consensus_type mismatch", details={agreement_type, consensus_type, expected_consensus_type}
- Missing version_index lookup: level=WARNING, block=A4/A5, message="previous_version_key not found in version_index"
- Enum violation: level=ERROR, message="Invalid enum value", details={field, value, allowed_values}
- Schema violation: level=ERROR, phase=5, message="Schema validation failed", details={field_path, expected_type, actual_value}

### Summary Statistics (Phase 5)

After M4 completes, produce and log:

- Total queue items processed
- Total items with analysis_degraded=true
- Per-block failure counts: {A1: n, A2: n, A3: n, A4: n, A5: n, A6: n}
- Consensus mismatches (Fix #1): count
- G1: systemic blockers found (count + list)
- G2: looping families found (count)
- G3: high-risk items (count), max risk_score
- G4: high-risk clusters (count + list of sheets)
- Total warnings, total errors
- Wall-clock time: total M4, per-phase breakdown

---

## 7. Open Ambiguities or Spec Risks

**7.1 A2 — Approvers Against Majority When TIED** [MINOR]

The spec defines majority_position as TIED when approve_count = reject_count. It does not explicitly define which approvers go into `approvers_against_majority` in a TIED scenario. The plan proposes merging both approve_list and reject_list (all opinionated approvers are "against" in a tie), which is a reasonable interpretation. However, an alternative reading could be: leave the list empty (no majority exists, so no one is "against" it). **Recommendation**: Use the merged approach (both lists). Flag for spec author confirmation.

**7.2 A5 — Assigned Approvers Mismatch Between Revisions** [MINOR]

The spec says "for each approver in assigned_approvers, compare current statut to previous statut." If the current revision's `assigned_approvers` differs from the previous revision's (e.g., an approver was added or removed between revisions), the comparison is asymmetric. The plan assumes we iterate over the CURRENT revision's assigned_approvers only and compare their statuts. Approvers present in the previous but not in the current are not reported (they were removed). Approvers in the current but not in the previous will show previous_statut=null. **Recommendation**: This behavior seems correct for a delta (what changed for the current approver set). Flag for spec confirmation.

**7.3 G4 — Sheets With Zero Queue Items** [MINOR]

The spec says "one row per source_sheet (all 25 sheets)." Some sheets may have zero items in the M3 queue (all their documents may already have visa_global set, or be duplicates, etc.). The plan handles this: total_pending=0, avg_priority_score=null, avg_days_pending=null. The health_score formula uses max(total_pending, 1) in denominators, so division by zero is avoided. Approval_rate uses is_latest rows regardless of queue membership. No ambiguity in the formula, but worth noting that sheets with zero pending items will have health_score driven entirely by (1−approval_rate)×30. **Recommendation**: Confirm this is intended behavior.

**7.4 G1 — avg_response_days Date Source** [MINOR]

The spec says avg_response_days = mean of (statut_date − date_diffusion) across responded items. The "statut_date" likely refers to `{APPROVER}_date`. However, `{APPROVER}_date` is per-approver while `date_diffusion` is per-item. For a given approver across multiple doc families, each computation uses that family's date_diffusion and that approver's date on that family's latest revision. The plan computes per-(approver, family) pair and averages. If `{APPROVER}_date` is null for a responded item, that pair is excluded from the average. **Recommendation**: Confirm this interpretation.

**7.5 lifecycle_state ON_HOLD Priority** [SPEC INTERPRETATION]

The spec table lists ON_HOLD with condition "analysis_degraded = true" but does not explicitly state it overrides all other conditions. The plan proposes checking analysis_degraded FIRST (before consensus_type mapping), so a degraded item is always ON_HOLD regardless of consensus. This is the most conservative interpretation. If the spec intends ON_HOLD as a fallback (only when no other state matches), the ordering would differ. **Recommendation**: The proposed priority ordering (analysis_degraded checked first) is safer and prevents exposing potentially incorrect analysis results as actionable states. Confirm with spec author.

**7.6 G1 — Counting Grain: doc_family_key vs (doc_family_key, source_sheet)** [RESOLVED] [PATCHED — V2] [PATCHED — V3]

The spec defines G1 metrics (total_latest_assigned, total_responded, total_blocking) using "distinct doc families" as the counting unit. However, the same doc_family_key can appear in multiple sheets when `is_cross_lot = true` (up to 16 sheets in the dataset). M2 spec states that "chains stay independent per sheet." An approver can have statut=REF on one sheet and VSO on another for the same family. Counting by doc_family_key alone would collapse these distinct operational situations into a single count, losing granularity.

**Option A**: Count by distinct `doc_family_key` (family-level grain). Simpler, but collapses cross-lot divergences. An approver blocking a family on 3 sheets would count as 1 blocking instance, not 3.

**Option B**: Count by distinct `(doc_family_key, source_sheet)` (row-level grain). Consistent with M2's per-sheet chain independence model. Each is_latest row is one counting unit. An approver blocking a family on 3 sheets counts as 3 blocking instances.

**Decision locked for implementation: Option B — (doc_family_key, source_sheet).** Rationale: G1 scope is is_latest rows (which are per-sheet), M2 treats chains as per-sheet independent, and blocking_rate should reflect the actual operational surface area of each approver's blocking behavior. If the spec author later specifies family-level grain, G1 computation must be updated.

---

---

## Patch Log

### V1 → V2

| Patch | Section(s) Modified | Summary |
|---|---|---|
| **Patch 1** | §3.8 (step 4), §4 lifecycle_state (steps 3–5) | lifecycle_state contradiction now **overrides** to M3-category-implied value instead of preserving conflicting computed value. Added compatibility map + reverse mapping table. Added ALL_HM guard (should never appear in queue). |
| **Patch 2** | §2.1 (Phase 2, G1 display_name) | Replaced heuristic title-casing with hardcoded canonical display_name dictionary for all 14 approvers. Unknown keys → ERROR + raw key fallback. |
| **Patch 3** | §4 A1 (step 7 safeguard) | Missing `{KEY}_statut` column upgraded from WARNING to ERROR. Classified as upstream schema anomaly, NOT M4 computation failure. Does not set analysis_degraded. Full context logged. |
| **Patch 4** | §2.1 (G1 counting metrics), §7.6 (new ambiguity) | Surfaced counting grain (doc_family_key vs (doc_family_key, source_sheet)) as formal ambiguity. All G1 metric descriptions updated with "[pending spec confirmation]". Recommended (doc_family_key, source_sheet) grain. |
| **Patch 5** | §5.2 (Phase 5 DataFrame validation) | Replaced vague "minor issues" wording with explicit 6-rule deterministic fallback table. Every fallback traceable to a numbered rule. |
| **Patch 6** | §3.7b (new step), §3.9 (reference update) | Added explicit `analysis_degraded` and `failed_blocks` derivation step between A6 and lifecycle_state. Formalized as immutable after computation. Updated §3.9 to reference pre-computed values. |

### V2 → V3 (coding-ready)

| Patch | Section(s) Modified | Summary |
|---|---|---|
| **Patch 7** | §3.8 (compatibility map table) | Fixed transcription error: NOT_STARTED compatible category changed from WAITING → NOT_STARTED. Resolves internal inconsistency with reverse mapping table. |
| **Patch 8** | §2.1 (G1 metrics), §7.6 | Locked G1 counting grain to (doc_family_key, source_sheet). Removed all "[pending spec confirmation]" caveats. Updated blocked_families to list of tuples. Changed §7.6 from open ambiguity to resolved decision. |
| **Patch 9** | §6 (G1–G4 DataFrame validation fallback table) | Split old rule 3 (string/enum conflation) into separate rules: rule 5 for enum columns (safe fallback exists) and rule 6 for identifier/string columns (no safe default → empty DataFrame). Booleans separated into rule 3. Total: 7 rules replacing 6. |

*End of Module 4 Implementation Plan.*
