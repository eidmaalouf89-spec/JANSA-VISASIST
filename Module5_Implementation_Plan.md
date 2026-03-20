# Module 5: Suggestion Engine — Implementation Plan

**Version**: V2.2.2 (8 mandatory patches + 9 hardening fixes applied) — Coding-ready
**Date**: 2026-03-20
**Status**: PLANNING PHASE — No code. Plan only.

---

## 1. Executive Implementation Overview

Module 5 (M5) is the Suggestion Engine — Tier 3 in the JANSA VISASIST pipeline orchestration [V2.2.2 E1 §2]. It consumes the outputs of M3 (prioritized queue with consensus, priority, overdue data) and M4 (per-item analysis blocks A1–A6, lifecycle_state, global reports G1–G4), and produces deterministic, actionable recommendations for MOEX operators.

**Inputs**: M3 priority queue (all queued items), M4 per-item analysis results (List[Dict]), M4 global reports (G1–G4), and a `pipeline_run_id` for cache keying.

**Outputs** (returned as a 4-element Tuple):

1. `List[Dict]` — Per-item `SuggestionResult` for every M3 queue item
2. `pd.DataFrame` — S1: Action Distribution Report
3. `pd.DataFrame` — S2: VISA Recommendation Report
4. `pd.DataFrame` — S3: Communication / Relance Report

**Authority level**: DERIVED TRANSIENT — all outputs are deterministic functions of M3+M4, valid while `pipeline_run_id` matches. M5 is computed on-demand when item/batch requested [V2.2.2 §10.1, E3]. Session-cacheable with key `(row_id, pipeline_run_id)`.

**Execution strategy**: 6-layer sequential decision engine per item, then 3 report aggregations:

- Layer 0: Scope guard (EXCLUDED/not-in-queue → SKIP)
- Layer 1: Hard overrides (ON_HOLD, analysis_degraded → HOLD)
- Layer 2: Primary action resolution (lifecycle_state → suggested_action)
- Layer 3: VISA recommendation (action × consensus → proposed_visa)
- Layer 4: Relance logic (communication targets and templates)
- Layer 5: Escalation logic (threshold-based escalation_level)
- Layer 6: Confidence scoring (deterministic formula)
- Reports: S1 (action distribution), S2 (VISA recommendations), S3 (relance/communication)

**Performance target**: Deterministic fields < 100ms per item from persisted M3/M4 [V2.2.2 §10.1]. All layer functions are O(1) table lookups or O(1) arithmetic. Reports are O(n) single-pass aggregations.

**Idempotency guarantee** [PATCH 6]: Given identical inputs (same row_id, same pipeline_run_id, same M3 and M4 output fields), M5 MUST produce bit-level identical JSON output when serialized with `json.dumps(result, sort_keys=True, ensure_ascii=False)`. No randomness, no `datetime.now()`, no `uuid4()`. Float precision: confidence rounded to 4 decimal places. action_priority is integer. Lists sorted deterministically. Dict keys sorted alphabetically.

**Consume-only guarantee** [V2.2.2 §2.4]: M5 reads M3/M4 outputs. M5 MUST NOT modify, mutate, or write back to any M1–M4 data structures or storage.

---

## 2. Phase-by-Phase Implementation Plan

### Phase 1: Input Validation & Index Construction

**Objective**: Validate all M3/M4 inputs, build O(1) lookup indexes for the decision engine.

**Inputs**: m3_queue (DataFrame), m4_results (List[Dict]), g1_report (DataFrame), pipeline_run_id (str)

**Outputs produced**:

- `m4_index`: dict[row_id → m4 analysis_result dict] — O(1) lookup of per-item M4 data
- `g1_blocker_index`: dict[approver_key → G1 row data] — O(1) systemic blocker lookup
- Validated input contract (all required fields present)

**Steps**:

**1.0 Upstream Field Alignment — Exact Source Paths** [NEW — Fix 2]

Before any processing, M5 must resolve every consumed field to its exact path in M3/M4 output structures. This table is the authoritative mapping. If the actual M3 DataFrame columns or M4 result dict structure differ from these paths, update THIS table — not the upstream module. M5 adapts to M3/M4 schemas, never the reverse.

| M5 Consumed Field | Upstream | Exact Source Path (key in dict/DataFrame) | Type | Required? | Fallback if Missing |
|---|---|---|---|---|---|
| lifecycle_state | M4 | m4_result["lifecycle_state"] | str (enum) | YES | fatal — analysis_degraded |
| analysis_degraded | M4 | m4_result["analysis_degraded"] | bool | YES | assume true (conservative) |
| score_consensus | M4 A1 | m4_result["A1"]["agreement_ratio"] | float 0–1 | YES | 0.0 + degraded flag |
| missing_count | M4 A3 | m4_result["A3"]["missing_count"] | int ≥ 0 | YES | 0 + degraded flag |
| response_rate | M4 A3 | m4_result["A3"]["response_rate"] | float 0–1 | YES | 0.0 + degraded flag |
| consecutive_rejections | M4 A4 | m4_result["A4"]["consecutive_rejections"] | int ≥ 0 | YES | 0 (conservative) |
| days_since_last_action | M4 A6 | m4_result["A6"]["days_since_last_action"] | int or null | NO | null (informational only) |
| priority_score | M3 | m3_row["priority_score"] | int 0–100 | YES | fatal — cannot compute |
| consensus_type | M3 | m3_row["consensus_type"] | str (enum) | YES | fatal — cannot compute |
| category | M3 | m3_row["category"] | str (enum) | YES | used for cross-check only |
| is_overdue | M3 | m3_row["is_overdue"] | bool | YES | false (conservative) |
| days_overdue | M3 | m3_row["days_overdue"] | int ≥ 0 | YES | 0 |
| has_deadline | M3 | m3_row["has_deadline"] | bool | YES | false |
| missing_approvers | M3 | m3_row["missing_approvers"] | list[str] | YES | [] (empty list) |
| blocking_approvers | M3 | m3_row["blocking_approvers"] | list[str] | YES | [] (empty list) |
| relevant_approvers | M3 | m3_row["relevant_approvers"] | int | YES | 1 (avoid division by zero) |
| days_since_diffusion | M3 | m3_row["days_since_diffusion"] | int or null | NO | 0 (Layer 4 NOT_STARTED) |
| source_sheet | M3 | m3_row["source_sheet"] | str | YES | "LOT_UNKNOWN" |
| document | M3 | m3_row["document"] | str or null | YES | "DOC_UNKNOWN" |
| total_assigned | M3 | m3_row["total_assigned"] | int | NO | 0 |
| replied | M3 | m3_row["replied"] | int | NO | 0 |
| pending | M3 | m3_row["pending"] | int | NO | 0 |
| assigned_approvers | M3 | m3_row["assigned_approvers"] | list[str] | YES | [] (empty list) |

Note [OBS-3]: assigned_approvers is required by Layer 4 row 4e (ARBITRATE relance_targets) and row 4f (NOT_STARTED relance_targets). See §2.6 for derivation logic.

**CRITICAL IMPLEMENTATION RULE**: Before coding, verify these exact paths against the actual M3 DataFrame columns and M4 result dict structure from the already-implemented Modules 3 and 4. If any path differs, update THIS table — not the upstream module. M5 adapts to M3/M4 schemas, never the reverse.

**1.1 Input DataFrame Validation** [SAFEGUARD]

- Verify m3_queue is a non-null, non-empty DataFrame. If empty → return early with empty outputs (empty list, empty S1/S2/S3 DataFrames with correct schemas), log WARNING.
- Verify m4_results is a non-null list with length matching m3_queue row count. If length mismatch → log ERROR. Process items that have matching M4 results; items without M4 results get analysis_degraded=true treatment (Layer 1 override).
- Verify pipeline_run_id is a non-null, non-empty string.
- Verify g1_report is a DataFrame (may be empty if G1 failed in M4 — handled gracefully).

**1.2 M3 Required Column Check** [SAFEGUARD]

Required M3 columns consumed by M5:

| Column | Type | Used By |
|---|---|---|
| row_id | str | Primary key, all layers |
| category | enum(6) | Layer 2 cross-check |
| consensus_type | enum(6) | Layers 2, 3, 6 |
| priority_score | int 0–100 | action_priority formula |
| is_overdue | bool | Layers 2, 4, 5, 6 |
| days_overdue | int ≥ 0 | Layers 4, 5, 6 |
| has_deadline | bool | Confidence component |
| missing_approvers | list[str] | Layer 4 relance targets |
| blocking_approvers | list[str] | Layers 4, 5, action_priority |
| total_assigned | int | Reason details |
| replied | int | Reason details |
| pending | int | Reason details |
| relevant_approvers | int | Confidence denominator |
| days_since_diffusion | int or null | Layer 4 (NOT_STARTED relance) |
| source_sheet | str | Template params (lot) |
| document | str | Template params |

If any critical column is missing → log ERROR, raise exception (M5 cannot run).

**1.3 M4 Required Field Check** [SAFEGUARD]

Required M4 fields per analysis_result dict:

| Field | Type | Used By |
|---|---|---|
| row_id | str | Index key |
| lifecycle_state | enum(8) | Layer 0, 1, 2 primary driver |
| analysis_degraded | bool | Layer 1 hard override |
| A1.agreement_ratio | float 0–1 | Layer 6 confidence |
| A3.missing_count | int ≥ 0 | Layers 4, 6 |
| A3.response_rate | float 0–1 | Layer 6 confidence |
| A4.consecutive_rejections | int ≥ 0 | Layers 2, 5 |
| A6.days_since_last_action | int or null | Reason details |
| G1.items_blocked_by (via g1_blocker_index) | int | Layer 5 systemic escalation |

If a field is missing from an individual M4 result → treat that item as analysis_degraded=true. Log ERROR with row_id and missing field name.

**1.4 Build m4_index** [IMPLEMENTATION]

- Iterate m4_results list. Map row_id → full analysis_result dict.
- If duplicate row_id found → log ERROR, keep first occurrence.
- Mandatory index. Without it, Layer 2+ cannot function.

**1.5 Build g1_blocker_index** [IMPLEMENTATION]

- If g1_report is non-empty: iterate rows, map approver_key → {is_systemic_blocker, total_blocking, blocked_families, severity}.
- If g1_report is empty (G1 failed in M4): set g1_blocker_index = empty dict. Layer 5 systemic escalation rules will be skipped (conservative: no false systemic blocker alerts). Log WARNING.

**Failure handling**: If index construction fails → log ERROR. m4_index failure is fatal (no suggestions possible). g1_blocker_index failure degrades Layer 5 systemic rules only.

---

### Phase 2: Per-Item Suggestion Computation (Layers 0–6)

**Objective**: Iterate every item in M3 queue. For each item, execute Layers 0→1→2→3→4→5→6 in strict order. Produce one `SuggestionResult` dict per item.

**Inputs**: m3_queue (iteration source), m4_index, g1_blocker_index, pipeline_run_id

**Outputs produced**:

- `suggestion_results`: List[Dict] — one SuggestionResult per valid queue item
- Every valid M3 queue item produces exactly one SuggestionResult. An item is valid if it is present in the M3 queue AND its lifecycle_state ≠ EXCLUDED. EXCLUDED items are an upstream anomaly (they should not appear in the M3 queue). If encountered, they are logged as WARNING and omitted — they do NOT produce a SuggestionResult. Count assertion: `len(suggestion_results) = len(m3_queue) - count(EXCLUDED encountered)`. If count(EXCLUDED encountered) > 0, log WARNING: "{n} EXCLUDED items found in M3 queue — upstream filtering anomaly."

**Steps**:

**2.1 Iteration Structure** [IMPLEMENTATION]

- Iterate m3_queue rows. For each row, extract row_id and all needed M3 fields.
- Look up M4 analysis_result from m4_index. If not found → treat as analysis_degraded=true.
- Wrap the entire per-item computation (Layers 0–6) in try/except. If ANY unhandled exception occurs: apply safe defaults (suggested_action=HOLD, proposed_visa=NONE, confidence=0.0, reason_code=DEGRADED_ANALYSIS, escalation_level=NONE, relance_required=false). Item is NEVER skipped. [SAFEGUARD]

**2.2 Layer 0: Scope Guard** [SPEC]

Decision table:

| # | Condition | Result | Fallback |
|---|---|---|---|
| 0a | lifecycle_state = EXCLUDED | SKIP — do not produce SuggestionResult for this item | — |
| 0b | Item not found in M3 queue (defensive guard) | SKIP | — |
| 0c | ALL OTHER CASES | PROCEED to Layer 1 | This IS the fallback |

Implementation: If EXCLUDED, this item is omitted from the suggestion_results list entirely. It will not appear in S1/S2/S3 reports. This is the only case where an item is not processed.

EXCLUDED items should already be filtered out by upstream modules. Their presence in the M3 queue is an upstream anomaly. M5 logs WARNING and omits them. This is the ONLY case where an item does not produce a SuggestionResult.

**2.3 Layer 1: Hard Overrides** [SPEC]

Decision table (first match wins):

| # | Condition | suggested_action | proposed_visa | confidence | reason_code | Layers 2–6 |
|---|---|---|---|---|---|---|
| 1a | lifecycle_state = ON_HOLD | HOLD | NONE | 0.0 | DEGRADED_ANALYSIS | SKIPPED |
| 1b | analysis_degraded = true | HOLD | NONE | 0.0 | DEGRADED_ANALYSIS | SKIPPED |
| 1c | ALL OTHER CASES | PROCEED to Layer 2 | — | — | — | EXECUTED |

When Layer 1 matches (rows 1a or 1b): set all output fields immediately. escalation_level=NONE, relance_required=false, relance_targets=[], relance_message=null, escalation_required=false, blocking_approvers and missing_approvers copied from M3 as-is. action_priority computed using the PATCH 5 formula with the HOLD values (escalation=NONE, no overdue boost from HOLD). Skip to output assembly.

**2.4 Layer 2: Primary Action Resolution** [SPEC] [PATCH 1]

This is the exhaustive primary decision table. lifecycle_state is the PRIMARY discriminator per [V2.2.2 E2 §3]. Every item that passes Layer 1 MUST match exactly one row below.

| # | lifecycle_state | consensus_type | A4.consec_rej | is_overdue | suggested_action | reason_code |
|---|---|---|---|---|---|---|
| 2a | NOT_STARTED | NOT_STARTED | any | false | HOLD | NOT_YET_STARTED |
| 2b | NOT_STARTED | NOT_STARTED | any | true | HOLD | NOT_YET_STARTED |
| 2c | WAITING_RESPONSES | INCOMPLETE | any | false | CHASE_APPROVERS | MISSING_RESPONSES |
| 2d | WAITING_RESPONSES | INCOMPLETE | any | true | CHASE_APPROVERS | OVERDUE |
| 2e | READY_TO_ISSUE | ALL_APPROVE | any | false | ISSUE_VISA | CONSENSUS_APPROVAL |
| 2f | READY_TO_ISSUE | ALL_APPROVE | any | true | ISSUE_VISA | CONSENSUS_APPROVAL |
| 2g | READY_TO_REJECT | ALL_REJECT | any | false | ISSUE_VISA | CONSENSUS_REJECTION |
| 2h | READY_TO_REJECT | ALL_REJECT | any | true | ISSUE_VISA | CONSENSUS_REJECTION |
| 2i | NEEDS_ARBITRATION | MIXED | any | false | ARBITRATE | MIXED_CONFLICT |
| 2j | NEEDS_ARBITRATION | MIXED | any | true | ARBITRATE | MIXED_CONFLICT |
| 2k | CHRONIC_BLOCKED | ALL_REJECT | ≥ 2 | false | ESCALATE | BLOCKING_LOOP |
| 2l | CHRONIC_BLOCKED | ALL_REJECT | ≥ 2 | true | ESCALATE | BLOCKING_LOOP |
| 2m | CHRONIC_BLOCKED | ALL_REJECT | < 2 | false | ESCALATE | BLOCKING_LOOP |
| 2n | CHRONIC_BLOCKED | ALL_REJECT | < 2 | true | ESCALATE | BLOCKING_LOOP |
| 2o | ANY (unmatched) | ANY | any | any | HOLD | DEGRADED_ANALYSIS |

**Interpretation rules**:

- Row 2o is the explicit no-fallthrough catch-all [PATCH 8]. It fires when lifecycle_state and consensus_type are inconsistent (e.g., READY_TO_ISSUE but consensus_type ≠ ALL_APPROVE). This is a lifecycle contradiction. When row 2o fires: log ERROR with lifecycle_state, consensus_type, and row_id. Set suggested_action=HOLD, reason_code=DEGRADED_ANALYSIS.
- is_overdue modifies reason_code for WAITING_RESPONSES only (row 2d: OVERDUE vs row 2c: MISSING_RESPONSES). It does NOT change the action.
- A4.consecutive_rejections is logged in reason_details but does NOT change CHRONIC_BLOCKED action (always ESCALATE, rows 2k–2n).
- [Fix 8] M5 trusts that lifecycle_state is correctly computed by M4 per [V2.2.2 E2 §3.2] conditions. In particular: READY_TO_REJECT implies M3 consensus = ALL_REJECT AND revision_count = 1; CHRONIC_BLOCKED implies M3 consensus = ALL_REJECT AND revision_count > 1. M5 does not re-validate revision_count. If M4 misassigns lifecycle_state, M5 will produce suggestions based on the (incorrect) lifecycle_state. Such contradictions are caught by catch-all row 2o only if lifecycle_state and consensus_type are inconsistent — revision_count errors within ALL_REJECT consensus are invisible to M5. This is an accepted trust boundary.

**reason_details enrichment** (populated for all rows, does NOT affect action selection):

| Input Field | Key in reason_details | Source |
|---|---|---|
| A1.agreement_ratio | consensus_strength | M4 A1 |
| A3.missing_count | missing_count | M4 A3 |
| A4.consecutive_rejections | rejection_depth | M4 A4 |
| A6.days_since_last_action | staleness_days | M4 A6 |
| G1 blocker data (if systemic) | systemic_blocker_detected | M4 G1 via g1_blocker_index |
| priority_score | priority_score | M3 |
| days_overdue | days_overdue | M3 |

reason_details dict keys MUST be sorted alphabetically [PATCH 6].

**2.5 Layer 3: VISA Recommendation** [SPEC] [PATCH 2]

Pure lookup table. No derived logic. Every suggested_action × consensus_type pair resolves to exactly one proposed_visa.

| # | suggested_action | consensus_type | proposed_visa | Rationale |
|---|---|---|---|---|
| 3a | ISSUE_VISA | ALL_APPROVE | APPROVE | Unanimous approval → issue VSO/VAO |
| 3b | ISSUE_VISA | ALL_REJECT | REJECT | Unanimous rejection → issue REF |
| 3c | CHASE_APPROVERS | INCOMPLETE | WAIT | Missing responses → cannot issue |
| 3d | CHASE_APPROVERS | NOT_STARTED | WAIT | Guard: CHASE on NOT_STARTED |
| 3e | ARBITRATE | MIXED | NONE | Split opinions → MOEX decides |
| 3f | ESCALATE | ALL_REJECT | NONE | Chronic block → escalation |
| 3g | ESCALATE | MIXED | NONE | Guard: escalation from conflict |
| 3h | ESCALATE | INCOMPLETE | NONE | Guard: escalation from stale wait |
| 3i | HOLD | NOT_STARTED | NONE | No data yet → no visa possible |
| 3j | HOLD | ANY (degraded) | NONE | Degraded → no visa |
| 3k | HOLD | ANY (on_hold) | NONE | On-hold guard → no visa |
| 3l | ANY (fallback) | ANY | NONE | No-fallthrough guarantee [PATCH 8] |

When row 3l fires: log WARNING with unexpected action/consensus pair.

**2.6 Layer 4: Relance Logic** [SPEC]

Determines whether communication is needed, who to contact, and which template to use.

| # | suggested_action | Condition | relance_required | relance_targets | Template |
|---|---|---|---|---|---|
| 4a | CHASE_APPROVERS | is_overdue = true | true | missing_approvers (from M3/A3) | T2 |
| 4b | CHASE_APPROVERS | is_overdue = false | true | missing_approvers | T1 |
| 4c | ESCALATE | A4.consec_rej ≥ ESCALATION_CONSEC_REJ_MOEX (2) | true | blocking_approvers | T3 |
| 4d | ESCALATE | G1 systemic blocker detected | true | systemic blocker approvers | T3 |
| 4e | ARBITRATE | always | true | approvers with divergent statuses | T4 |
| 4f | HOLD (NOT_STARTED) | days_since_diffusion ≥ RELANCE_NOT_STARTED_DAYS (7) | true | all assigned_approvers | T5 |
| 4g | HOLD (NOT_STARTED) | days_since_diffusion < 7 | false | [] | — |
| 4h | HOLD (degraded/on_hold) | always | false | [] | — |
| 4i | ISSUE_VISA | always | false | [] | — |
| 4j | ANY (fallback) | any | false | [] | — |

**Named constants**:

| Constant | Value | Type | Rationale |
|---|---|---|---|
| RELANCE_NOT_STARTED_DAYS | 7 | int | Wait 7 days before first contact on NOT_STARTED |

**Relance target resolution**:

- For rows 4a, 4b: `relance_targets` = M3.missing_approvers list.
- For row 4c: `relance_targets` = M3.blocking_approvers list.
- For row 4d: `relance_targets` = approver_keys from G1 where is_systemic_blocker=true AND the approver is in the current item's assigned_approvers.
- For row 4e: `relance_targets` = sorted(set(M3.assigned_approvers) - set(M3.missing_approvers)) — i.e., all approvers who have responded (regardless of their vote), since they need coordination for arbitration. [OBS-3 resolved: uses assigned_approvers and missing_approvers, both available in §1.0 alignment table. Does not depend on approvers_vso/vao/ref count fields.]
- For row 4f: `relance_targets` = full assigned_approvers list from M3 item.

**Defensive guard** [Edge Case #11]: If CHASE_APPROVERS but missing_approvers is empty → set relance_required=false, relance_targets=[], relance_message=null. Log WARNING: "CHASE_APPROVERS with empty missing_approvers for row_id={row_id}".

**relance_targets ordering** [PATCH 6]: Always sorted alphabetically by canonical approver key.

**2.7 Layer 5: Escalation Logic** [SPEC] [PATCH 4]

Determines escalation_level using explicit thresholds. First matching rule wins (priority ordering ensures DIRECTION checked before MOEX).

| Priority | Condition | escalation_level | Rationale |
|---|---|---|---|
| 1 | A4.consecutive_rejections ≥ ESCALATION_CONSEC_REJ_DIR (3) | DIRECTION | 3+ consecutive rejections → intractable |
| 2 | days_overdue ≥ ESCALATION_OVERDUE_DIR (60) | DIRECTION | 60+ days overdue → critical delay |
| 3 | G1.items_blocked_by[approver] ≥ ESCALATION_SYSTEMIC_DIR (10) for any approver in blocking_approvers | DIRECTION | Approver blocking 10+ items systemically |
| 4 | A4.consecutive_rejections ≥ ESCALATION_CONSEC_REJ_MOEX (2) | MOEX | 2+ consecutive rejections → MOEX intervention |
| 5 | days_overdue ≥ ESCALATION_OVERDUE_MOEX (30) | MOEX | 30+ days overdue → MOEX attention |
| 6 | G1.items_blocked_by[approver] ≥ ESCALATION_SYSTEMIC_MOEX (5) for any approver in blocking_approvers | MOEX | Approver blocking 5+ items |
| 7 | suggested_action = ESCALATE (no threshold met above) | MOEX | ESCALATE always produces at least MOEX |
| 8 | ALL OTHER CASES | NONE | No escalation needed |

**Named constants**:

| Constant | Value | Type | Description |
|---|---|---|---|
| ESCALATION_CONSEC_REJ_DIR | 3 | int | Consecutive rejections threshold → DIRECTION |
| ESCALATION_CONSEC_REJ_MOEX | 2 | int | Consecutive rejections threshold → MOEX |
| ESCALATION_OVERDUE_DIR | 60 | int | Days overdue threshold → DIRECTION |
| ESCALATION_OVERDUE_MOEX | 30 | int | Days overdue threshold → MOEX |
| ESCALATION_SYSTEMIC_DIR | 10 | int | G1 blocked items count → DIRECTION |
| ESCALATION_SYSTEMIC_MOEX | 5 | int | G1 blocked items count → MOEX |

**G1 blocker data handling** [Edge Case #12]: If g1_blocker_index is empty (G1 failed in M4), skip rules 3 and 6 entirely. Only A4/overdue thresholds and the rule-7 guard apply. Log WARNING: "G1 data unavailable, systemic escalation rules skipped for row_id={row_id}".

**escalation_required derivation**: escalation_required = (escalation_level ≠ NONE).

**Rule 3 and 6 detailed logic**: For each approver_key in M3.blocking_approvers, look up g1_blocker_index[approver_key].total_blocking. If ANY approver's total_blocking meets the threshold → the rule fires. For DIRECTION (rule 3), check ≥ 10. For MOEX (rule 6), check ≥ 5. The specific approver(s) meeting the threshold are logged in reason_details under `systemic_blockers` list.

**2.8 Layer 6: Confidence Scoring** [SPEC] [PATCH 3]

Deterministic formula. No ML. No heuristics. Reproducible. Bounded [0, 1].

**Hard override**: If Layer 1 matched (HOLD from degraded/on_hold) → confidence = 0.0. Skip formula entirely.

**Formula**:

```
confidence_raw =
    BASE_CONFIDENCE
  + (W_CONSENSUS × score_consensus)
  + (W_COMPLETENESS × score_completeness)
  - (W_MISSING × missing_ratio)
  - (W_CONFLICT × conflict_penalty)
  - (W_OVERDUE × overdue_penalty)
  - (W_DEGRADED × degraded_flag)

confidence = clamp(0.0, 1.0, round(confidence_raw, 4))
```

**Component definitions**:

| Component | Formula | Range | Source |
|---|---|---|---|
| score_consensus | A1.agreement_ratio | [0, 1] | M4 A1 |
| score_completeness | A3.response_rate | [0, 1] | M4 A3 |
| missing_ratio | A3.missing_count / max(relevant_approvers, 1) | [0, 1] | M4 A3 / M3 |
| conflict_penalty | 1.0 if consensus_type = MIXED, else 0.0 | {0, 1} | M3 |
| overdue_penalty | min(days_overdue / OVERDUE_PENALTY_CAP, 1.0) if is_overdue, else 0.0 | [0, 1] | M3 |
| degraded_flag | 1.0 if analysis_degraded = true, else 0.0 | {0, 1} | M4 |

**Named constants**:

| Constant | Value | Type | Rationale |
|---|---|---|---|
| BASE_CONFIDENCE | 0.30 | float | Starting point: moderate confidence before adjustments |
| W_CONSENSUS | 0.35 | float | Strong consensus is the strongest confidence signal |
| W_COMPLETENESS | 0.20 | float | Full response coverage boosts confidence |
| W_MISSING | 0.15 | float | Missing approvers reduce confidence proportionally |
| W_CONFLICT | 0.25 | float | Conflict is a major confidence penalty |
| W_OVERDUE | 0.10 | float | Overdue items have slightly less confident recommendations |
| W_DEGRADED | 0.50 | float | Degraded analysis severely reduces confidence |
| OVERDUE_PENALTY_CAP | 60 | int | Overdue penalty maxes out at 60 days |

**Normalization and clamp rules**:

- confidence_raw may exceed [0, 1] before clamping. Example: BASE + full consensus + full completeness = 0.30 + 0.35 + 0.20 = 0.85.
- Clamp: if confidence_raw < 0.0 → 0.0. If confidence_raw > 1.0 → 1.0.
- Round to 4 decimal places AFTER clamping. This ensures idempotency [PATCH 6].

**Worked examples**:

| Scenario | Components | raw | final |
|---|---|---|---|
| ALL_APPROVE, complete, no issues | 0.30 + 0.35(1.0) + 0.20(1.0) - 0 - 0 - 0 - 0 | 0.8500 | 0.8500 |
| ALL_APPROVE, 1 of 6 missing | 0.30 + 0.35(1.0) + 0.20(0.83) - 0.15(0.17) - 0 - 0 - 0 | 0.7995 | 0.7995 |
| MIXED, complete, 15d overdue | 0.30 + 0.35(0.5) + 0.20(1.0) - 0.25(1.0) - 0.10(0.25) - 0 | 0.4000 | 0.4000 |
| INCOMPLETE, 3 of 6 missing | 0.30 + 0.35(0.5) + 0.20(0.5) - 0.15(0.5) - 0 - 0 - 0 | 0.5000 | 0.5000 |
| Degraded analysis | Layer 1 override | N/A | 0.0000 |
| NOT_STARTED, no responses | 0.30 + 0.35(0.0) + 0.20(0.0) - 0 - 0 - 0 - 0 | 0.3000 | 0.3000 |

**2.9 action_priority Computation** [PATCH 5]

Computed after all layers complete (requires escalation_level from Layer 5).

**Formula**:

```
action_priority = clamp(0, 100,
    base + escalation_boost + overdue_boost + blocking_boost)
```

**Components**:

| Component | Formula | Constants |
|---|---|---|
| base | priority_score (from M3, 0–100) | Direct pass-through |
| escalation_boost | ESCALATION_BOOST_MOEX if escalation_level = MOEX, ESCALATION_BOOST_DIR if DIRECTION, else 0 | ESCALATION_BOOST_MOEX = 10, ESCALATION_BOOST_DIR = 20 |
| overdue_boost | min(days_overdue, OVERDUE_CAP) × OVERDUE_MULTIPLIER if is_overdue, else 0 | OVERDUE_CAP = 30, OVERDUE_MULTIPLIER = 0.5 |
| blocking_boost | BLOCKING_BOOST if len(blocking_approvers) > 0, else 0 | BLOCKING_BOOST = 5 |

**Named constants**:

| Constant | Value | Type | Rationale |
|---|---|---|---|
| ESCALATION_BOOST_MOEX | 10 | int | Moderate boost for MOEX-level escalation |
| ESCALATION_BOOST_DIR | 20 | int | Strong boost for DIRECTION-level escalation |
| OVERDUE_CAP | 30 | int | Cap overdue contribution at 30 days |
| OVERDUE_MULTIPLIER | 0.5 | float | Each overdue day adds 0.5 pts (max 15) |
| BLOCKING_BOOST | 5 | int | Flat boost when blockers present |

**Result**: integer (int arithmetic, no rounding needed), clamped to [0, 100].

**2.10 Output Assembly** [IMPLEMENTATION]

After all 6 layers and action_priority are computed, assemble the SuggestionResult dict for the item. All fields defined in Section 3 below. Append to suggestion_results list.

---

### Phase 3: Report Generation (S1, S2, S3)

**Objective**: Aggregate per-item suggestion_results into three global DataFrames.

**Inputs**: suggestion_results (List[Dict]) from Phase 2.

**S1 — Action Distribution Report** [Fix 5 — hardened schema]:

| Column | Type | Nullable | Owned By | Description |
|---|---|---|---|---|
| suggested_action | str (enum) | NEVER | M5 | GP8-registered suggested_action value |
| source_sheet | str | NEVER | M3 | Lot identifier (pass-through) |
| priority_band | str (enum) | NEVER | M5 | CRITICAL / HIGH / MEDIUM / LOW [V2.2.2 §9.2] |
| item_count | int ≥ 1 | NEVER | M5 | Count of items in this group |
| avg_confidence | float | NEVER | M5 | Mean confidence, rounded 4dp |
| avg_action_priority | float | NEVER | M5 | Mean action_priority, rounded 2dp |
| escalated_count | int ≥ 0 | NEVER | M5 | Count where escalation_level ≠ NONE |

Aggregation: GROUP BY (suggested_action, source_sheet, priority_band). Single pass over suggestion_results. Output rows sorted by (suggested_action ASC, source_sheet ASC, priority_band DESC — CRITICAL > HIGH > MEDIUM > LOW).

**S2 — VISA Recommendation Report** [Fix 5 — hardened schema]:

| Column | Type | Nullable | Owned By | Description |
|---|---|---|---|---|
| proposed_visa | str (enum) | NEVER | M5 | GP8-registered proposed_visa value |
| source_sheet | str | NEVER | M3 | Lot identifier (pass-through) |
| item_count | int ≥ 1 | NEVER | M5 | Count of items with this recommendation |
| avg_confidence | float | NEVER | M5 | Mean confidence, rounded 4dp |
| pct_of_lot | float | NEVER | M5 | item_count / total items in lot, 4dp |

Aggregation: GROUP BY (proposed_visa, source_sheet). Single pass. Output sorted by (source_sheet ASC, proposed_visa ASC).

**S3 — Communication / Relance Report** [Fix 5 — hardened schema]:

| Column | Type | Nullable | Owned By | Description |
|---|---|---|---|---|
| row_id | str | NEVER | M1 | Item identifier (pass-through) |
| document | str | NEVER* | M3 | Document reference (*"DOC_UNKNOWN" if null upstream) |
| source_sheet | str | NEVER | M3 | Lot identifier |
| suggested_action | str (enum) | NEVER | M5 | Action triggering relance |
| relance_required | bool | NEVER | M5 | Always true (filtered) |
| relance_targets | str | NEVER | M5 | Comma-sep approver keys, sorted alphabetically |
| relance_template_id | str (enum) | NEVER | M5 | T1–T6 |
| relance_message | str | NEVER | M5 | Full rendered message, ≤ 200 chars |
| escalation_level | str (enum) | NEVER | M5 | NONE, MOEX, or DIRECTION |
| action_priority | int | NEVER | M5 | For urgency sorting |

Aggregation: FILTER suggestion_results WHERE relance_required=true. Single pass. Output sorted by (action_priority DESC, row_id ASC). Sort aligns with [V2.2.2 §9.1] general pattern (priority DESC).

**Schema ownership note**: All S1/S2/S3 schemas are owned by M5 and are contract-locked. Downstream consumers (M6 chatbot, M7 batch, future dashboard) depend on these exact column names and types.

**Failure handling**: If report generation fails → log ERROR, return empty DataFrame with correct schema. Report failure MUST NOT affect per-item suggestion_results (already computed and returned separately).

---

## 3. Per-Item Output Schema (SuggestionResult)

Every field of the SuggestionResult dict, with complete specifications:

| # | Field | Type | Description | Allowed Values | Source | Null Policy |
|---|---|---|---|---|---|---|
| 1 | row_id | str | Primary key from M1 | Any non-empty string | M3.row_id | NEVER null |
| 2 | suggested_action | str (enum) | Primary MOEX action recommendation | ISSUE_VISA, CHASE_APPROVERS, ARBITRATE, ESCALATE, HOLD | Layer 2 decision table | NEVER null |
| 3 | action_priority | int | Composite priority for sorting/display | 0–100 inclusive | PATCH 5 formula | NEVER null |
| 4 | proposed_visa | str (enum) | Recommended visa type to issue | APPROVE, REJECT, WAIT, NONE | Layer 3 lookup table | NEVER null |
| 5 | confidence | float | Deterministic confidence score | [0.0, 1.0], 4 decimal places | Layer 6 formula | NEVER null |
| 6 | reason_code | str (enum) | Deterministic category for recommendation | CONSENSUS_APPROVAL, CONSENSUS_REJECTION, MISSING_RESPONSES, BLOCKING_LOOP, MIXED_CONFLICT, DEGRADED_ANALYSIS, OVERDUE, NOT_YET_STARTED | Layer 2 decision table | NEVER null |
| 7 | reason_details | dict | Structured explanation from A1–A6 analysis. ALWAYS contains all 7 keys, even when not relevant to the current decision path. Irrelevant keys are set to null (for optional fields like staleness_days) or 0 (for counts like missing_count) or false (for bools). This guarantees dict structural identity per PATCH 6 idempotency. Fixed key set (always present, alphabetically sorted): consensus_strength (float or null), days_overdue (int), missing_count (int), priority_score (int), rejection_depth (int), staleness_days (int or null), systemic_blocker_detected (bool). [OBS-2 note: If OP-03 is resolved by adding cross_lot_divergence (bool) from M4 A5, this becomes an 8-key dict. Update this fixed key set accordingly at implementation time.] | Keys sorted alphabetically [PATCH 6] | Layers 2–6 enrichment | NEVER null (always a 7-key dict, or 8-key if OP-03 resolved with addition) |
| 8 | blocking_approvers | list[str] | Approvers currently blocking with REF | Sorted alphabetically [PATCH 6] | M3.blocking_approvers | NEVER null (empty list [] if none) |
| 9 | missing_approvers | list[str] | Approvers who have not responded | Sorted alphabetically [PATCH 6] | M3.missing_approvers / M4 A3 | NEVER null (empty list [] if none) |
| 10 | relance_required | bool | Whether communication is needed | true / false | Layer 4 | NEVER null |
| 11 | relance_targets | list[str] | Approver keys to contact | Sorted alphabetically [PATCH 6] | Layer 4 | NEVER null (empty list [] if no relance) |
| 12 | relance_template_id | str or null | Template ID used | T1, T2, T3, T4, T5, T6 | Layer 4 | null if relance_required=false |
| 13 | relance_message | str or null | Parameterized template output | UTF-8, ≤ 200 chars [PATCH 7] | Layer 4 + template engine | null if relance_required=false |
| 14 | escalation_required | bool | Whether escalation is needed | true / false | Derived: escalation_level ≠ NONE | NEVER null |
| 15 | escalation_level | str (enum) | Escalation severity | NONE, MOEX, DIRECTION | Layer 5 threshold table | NEVER null |
| 16 | based_on_lifecycle | str | lifecycle_state used for decision | NOT_STARTED, WAITING_RESPONSES, READY_TO_ISSUE, READY_TO_REJECT, NEEDS_ARBITRATION, CHRONIC_BLOCKED, ON_HOLD, EXCLUDED | M4.lifecycle_state | NEVER null |
| 17 | analysis_degraded | bool | Whether M4 analysis was degraded | true / false | M4.analysis_degraded | NEVER null |
| 18 | pipeline_run_id | str | Cache key component | Non-empty string | Input parameter | NEVER null |

**Serialization rules** [PATCH 6]:

- All dict keys sorted alphabetically (reason_details, and the top-level SuggestionResult when serialized).
- All list values sorted alphabetically (blocking_approvers, missing_approvers, relance_targets).
- Float precision: confidence rounded to exactly 4 decimal places.
- action_priority is int (no decimal).
- json.dumps(result, sort_keys=True, ensure_ascii=False) must be bit-level identical across runs given identical inputs.

---

## 4. Relance Templates (T1–T6)

### 4.1 Template Constraints [PATCH 7]

| Constraint | Rule | Enforcement |
|---|---|---|
| Max length | 200 characters maximum per message (after parameter substitution) | Word-boundary truncation (see §4.5). Log WARNING if truncation occurs. |
| Tone | Formal, professional French. Vouvoiement only. | No informal language, no exclamation marks, no emoji. |
| No variability | Identical parameters → identical output, character-for-character | No datetime stamps, no random values, no locale-dependent formatting. |
| Fixed placeholders | Only: {document}, {approver}, {days}, {lot}, {deadline} | Any other placeholder in template = ERROR at initialization. |
| Encoding | UTF-8. French accents preserved. | No ASCII fallbacks for accented characters. |

### 4.2 Template Definitions

| ID | Trigger Condition | French Template Text | Priority |
|---|---|---|---|
| T1 | CHASE_APPROVERS + is_overdue=false | `Relance : merci de statuer sur le document {document} (lot {lot}), en attente depuis {days} jours.` | MEDIUM |
| T2 | CHASE_APPROVERS + is_overdue=true | `Relance urgente : le document {document} (lot {lot}) est en attente depuis {days} jours, délai dépassé. Merci de statuer.` | HIGH |
| T3 | ESCALATE (chronic/systemic) | `Escalade : le document {document} (lot {lot}) fait l'objet de {days} jours de blocage. Intervention requise.` | CRITICAL |
| T4 | ARBITRATE | `Arbitrage requis : avis divergents sur le document {document} (lot {lot}). Merci de vous coordonner.` | HIGH |
| T5 | HOLD (NOT_STARTED) + days_since_diffusion ≥ 7 | `Premier contact : le document {document} (lot {lot}) est en attente de visa depuis {days} jours. Merci de statuer.` | LOW |
| T6 | INCOMPLETE + partial responses | `Suivi : le document {document} (lot {lot}) attend encore votre retour depuis {days} jours. Merci de compléter.` | MEDIUM |

### 4.3 Template Selection Logic

Template selection is deterministic: trigger conditions evaluated in T1–T6 order. First match wins. If no template matches (e.g., ISSUE_VISA, HOLD without NOT_STARTED condition) → relance_message = null, relance_template_id = null.

### 4.4 Parameter Resolution

| Placeholder | Source | Fallback if null |
|---|---|---|
| {document} | M3 item `document` field | "DOC_UNKNOWN" (log WARNING) |
| {lot} | M3 item `source_sheet` field | "LOT_UNKNOWN" (log WARNING) |
| {days} | Depends on template: T1/T2/T5/T6 = M3.days_since_diffusion; T3 = M3.days_overdue; T4 = M3.days_since_diffusion | "0" (log WARNING) |
| {approver} | First entry in relance_targets (used only if template requires it — current T1–T6 do not use {approver}) | N/A for current templates |
| {deadline} | M3 `date_contractuelle_visa` formatted as YYYY-MM-DD | Not used in current templates |

### 4.5 Template Rendering Function

`generate_relance_message(template_id, params_dict)`:

1. Look up template string by template_id. If template_id not in {T1..T6} → return null, log ERROR.
2. Substitute all placeholders using Python `str.format(**params)`. If a required placeholder key is missing from params → use the fallback value from §4.4.
3. Verify result length ≤ 200 characters. If exceeds [Fix 6 — word-boundary truncation]:
   a. Find the last whitespace character at or before position 200.
   b. If found: truncate at that position (no trailing whitespace).
   c. If no whitespace found (single 200+ char token — should not happen with current templates): hard truncate at 200.
   d. Log WARNING with row_id and original length.
   e. Result is guaranteed ≤ 200 characters.
4. Return UTF-8 encoded string.

---

## 5. State Matrix (S1–S6) [Fix #26]

The ai_available / validation_failed matrix governs M5 behavior modes:

| State | ai_available | validation_failed | M5 Behavior |
|---|---|---|---|
| S1 | true | false | Full suggestion with AI explanation text appended to reason_details. Structured fields computed by decision engine as normal. AI text is SUPPLEMENTARY only — it cannot override suggested_action, proposed_visa, confidence, or any structured field [GP7]. |
| S2 | true | true | AI text discarded (validation failed). Template-based relance_message used. All structured fields (suggested_action, proposed_visa, confidence, escalation_level, action_priority) computed normally by decision engine. |
| S3 | false | false | Template-only mode. Full structured suggestion computed by decision engine. Relance messages use T1–T6 templates. No AI text. This is the GP10 baseline — fully functional without LLM [GP10]. |
| S4 | false | true | Template-only, degraded confidence. Structured fields computed normally. If validation failure affects confidence inputs → apply degraded_flag penalty (W_DEGRADED × 1.0). |
| S5 | true | (AI timeout) | AI generation timed out. Template fallback for relance_message. All structured fields kept as computed by decision engine. Functionally equivalent to S3 for the affected item. |
| S6 | false | false | GP10 guarantee mode. Identical to S3. Fully functional without any AI component. Decision engine, templates, confidence formula, escalation thresholds — all operate without LLM dependency. |

**Critical design principle**: The M5 decision engine (Layers 0–6) operates identically in ALL states S1–S6. AI text is an optional enrichment layer that NEVER affects structured output fields. This guarantees GP10 compliance: M5 is fully functional with ai_available=false.

**State determination**: ai_available is a configuration flag (injected at pipeline initialization). validation_failed is determined per-item if AI text generation is attempted and the output fails a format/safety validation. AI timeout is a per-request condition.

---

## 6. Enum Definitions [GP8]

All enums used in M5. Each value is GP8-registered. Out-of-enum values = ERROR.

### 6.0 Enum Ownership [NEW — Fix 7]

M5 defines and owns the following enums. These are GP8-registered by M5. Out-of-enum values in M5 outputs = ERROR.

**M5-OWNED ENUMS** (defined by M5, produced in M5 outputs):

- **suggested_action**: ISSUE_VISA, CHASE_APPROVERS, ARBITRATE, ESCALATE, HOLD
- **proposed_visa**: APPROVE, REJECT, WAIT, NONE
- **reason_code**: CONSENSUS_APPROVAL, CONSENSUS_REJECTION, MISSING_RESPONSES, BLOCKING_LOOP, MIXED_CONFLICT, DEGRADED_ANALYSIS, OVERDUE, NOT_YET_STARTED
- **escalation_level**: NONE, MOEX, DIRECTION
- **relance_template_id**: T1, T2, T3, T4, T5, T6

**UPSTREAM-CONSUMED ENUMS** (defined by M3/M4, validated on input by M5):

- **lifecycle_state** (M4): NOT_STARTED, WAITING_RESPONSES, READY_TO_ISSUE, READY_TO_REJECT, NEEDS_ARBITRATION, CHRONIC_BLOCKED, ON_HOLD, EXCLUDED
- **consensus_type** (M3): NOT_STARTED, INCOMPLETE, ALL_HM, MIXED, ALL_REJECT, ALL_APPROVE
- **category** (M3): EASY_WIN_APPROVE, BLOCKED, FAST_REJECT, CONFLICT, WAITING, NOT_STARTED

M5 validates upstream enum values on input. If an upstream enum value is not in the expected set → treat item as analysis_degraded=true, log ERROR with the invalid value.

### 6.1 suggested_action

| Value | Description | Triggered By |
|---|---|---|
| ISSUE_VISA | MOEX should issue visa (approval or rejection) | READY_TO_ISSUE or READY_TO_REJECT lifecycle |
| CHASE_APPROVERS | MOEX should contact missing approvers | WAITING_RESPONSES lifecycle |
| ARBITRATE | MOEX must resolve conflicting opinions | NEEDS_ARBITRATION lifecycle |
| ESCALATE | Issue requires escalation beyond MOEX | CHRONIC_BLOCKED lifecycle |
| HOLD | No action recommended at this time | NOT_STARTED, ON_HOLD, degraded, or catch-all |

### 6.2 proposed_visa

| Value | Description |
|---|---|
| APPROVE | Recommend issuing VSO or VAO |
| REJECT | Recommend issuing REF |
| WAIT | Cannot issue — awaiting responses |
| NONE | No visa recommendation possible |

### 6.3 reason_code

| Value | Description | Primary Trigger |
|---|---|---|
| CONSENSUS_APPROVAL | Unanimous or strong approval consensus | READY_TO_ISSUE + ALL_APPROVE |
| CONSENSUS_REJECTION | Unanimous or strong rejection consensus | READY_TO_REJECT + ALL_REJECT |
| MISSING_RESPONSES | Approvers have not yet responded | WAITING_RESPONSES + not overdue |
| OVERDUE | Item is past deadline and missing responses | WAITING_RESPONSES + is_overdue |
| BLOCKING_LOOP | Chronic rejection cycle detected | CHRONIC_BLOCKED |
| MIXED_CONFLICT | Divergent approver opinions require arbitration | NEEDS_ARBITRATION |
| DEGRADED_ANALYSIS | Analysis data is incomplete or contradictory | ON_HOLD, analysis_degraded, or catch-all |
| NOT_YET_STARTED | No responses received yet | NOT_STARTED lifecycle |

Note [Fix 4]: Systemic blocker detection is surfaced in reason_details (systemic_blocker_detected, systemic_blockers fields) and through escalation_level (Layer 5). It does not have a dedicated primary reason_code because the primary action is always determined by lifecycle_state via Layer 2. SYSTEMIC_BLOCKER was removed from this enum — see OP-02 (RESOLVED).

### 6.4 escalation_level

| Value | Description | Threshold Source |
|---|---|---|
| NONE | No escalation needed | Layer 5 rule 8 (default) |
| MOEX | Requires MOEX-level intervention | Layer 5 rules 4–7 |
| DIRECTION | Requires Direction-level intervention | Layer 5 rules 1–3 |

### 6.5 relance_template_id

| Value | Description |
|---|---|
| T1 | Standard chase (not overdue) |
| T2 | Urgent chase (overdue) |
| T3 | Escalation (chronic/systemic block) |
| T4 | Arbitration request |
| T5 | First contact (NOT_STARTED, 7+ days) |
| T6 | Follow-up (partial responses) |

---

## 7. Function Decomposition

Complete function-level breakdown with inputs, outputs, constraints, and dependencies.

### 7.1 Top-Level Entry Point

**`run_module5(m3_queue, m4_results, g1_report, pipeline_run_id) → Tuple[List[Dict], DataFrame, DataFrame, DataFrame]`**

- Orchestrates Phase 1 → Phase 2 → Phase 3.
- Returns (suggestion_results, s1_report, s2_report, s3_report).
- Constraint: entire M5 execution MUST NOT modify m3_queue, m4_results, or g1_report [V2.2.2 §2.4].

### 7.2 Phase 1 Functions

**`validate_m5_inputs(m3_queue, m4_results, g1_report, pipeline_run_id) → bool`**

- Inputs: all M5 inputs.
- Output: true if valid, raises exception if critical validation fails.
- Checks: non-null, non-empty, required columns/fields present.

**`build_m4_index(m4_results) → dict[str, dict]`**

- Inputs: List[Dict] of M4 analysis results.
- Output: dict mapping row_id → analysis_result.
- Constraint: O(n) single pass. Duplicate row_id → log ERROR, keep first.

**`build_g1_blocker_index(g1_report) → dict[str, dict]`**

- Inputs: G1 DataFrame.
- Output: dict mapping approver_key → {is_systemic_blocker, total_blocking, blocked_families, severity}.
- Constraint: O(n) where n = number of approver rows (max 14). Empty G1 → empty dict.

### 7.3 Phase 2 Functions (Per-Item)

**`compute_suggestion(m3_item, m4_result, g1_blocker_index, pipeline_run_id) → dict`**

- Inputs: single M3 row (as dict), corresponding M4 analysis_result (as dict), g1_blocker_index, pipeline_run_id.
- Output: complete SuggestionResult dict.
- Constraint: < 100ms. Calls Layers 0–6 sequentially, then action_priority, then assembles output.
- Failure: try/except wrapping entire function. On exception → safe defaults dict.

**`resolve_action(lifecycle_state, consensus_type, consecutive_rejections, is_overdue, analysis_degraded) → Tuple[str, str]`**

- Inputs: lifecycle_state (str), consensus_type (str), A4.consecutive_rejections (int), is_overdue (bool), analysis_degraded (bool).
- Output: (suggested_action, reason_code) tuple.
- Constraint: O(1) table lookup. Implements Layer 1 + Layer 2 combined. First checks Layer 1 overrides, then Layer 2 decision table.
- Fallback: (HOLD, DEGRADED_ANALYSIS) for any unmatched combination.

**`resolve_visa(suggested_action, consensus_type) → str`**

- Inputs: suggested_action (str), consensus_type (str).
- Output: proposed_visa enum value.
- Constraint: O(1) table lookup. Implements Layer 3.
- Fallback: NONE for any unmatched pair.

**`compute_confidence(m3_item, m4_result, suggested_action) → float`**

- Inputs: M3 fields (consensus_type, is_overdue, days_overdue, relevant_approvers), M4 fields (A1.agreement_ratio, A3.response_rate, A3.missing_count, analysis_degraded).
- Output: float [0.0, 1.0] rounded to 4 decimal places.
- Constraint: O(1) arithmetic. Implements Layer 6 formula.
- Hard override: returns 0.0 if analysis_degraded or lifecycle_state in {ON_HOLD}.

**`compute_action_priority(priority_score, escalation_level, is_overdue, days_overdue, blocking_approvers) → int`**

- Inputs: M3 priority_score (int), escalation_level (str), is_overdue (bool), days_overdue (int), blocking_approvers (list).
- Output: int [0, 100].
- Constraint: O(1) arithmetic. Implements PATCH 5 formula.

**`resolve_escalation(consecutive_rejections, days_overdue, is_overdue, blocking_approvers, g1_blocker_index, suggested_action) → str`**

- Inputs: A4.consecutive_rejections (int), days_overdue (int), is_overdue (bool), M3.blocking_approvers (list), g1_blocker_index (dict), suggested_action (str).
- Output: escalation_level enum value (NONE, MOEX, DIRECTION).
- Constraint: O(1) threshold checks. First match wins. Implements Layer 5.

**`resolve_relance(m3_item, m4_result, suggested_action, is_overdue, g1_blocker_index) → dict`**

- Inputs: M3 item fields, M4 result, suggested_action, is_overdue, g1_blocker_index.
- Output: dict with keys: relance_required (bool), relance_targets (list), relance_template_id (str or null), relance_message (str or null).
- Constraint: O(1) table lookup + template rendering. Implements Layer 4.

**`select_template(suggested_action, is_overdue, lifecycle_state, consensus_type, days_since_diffusion) → str or None`**

- Inputs: action context fields.
- Output: template ID (T1–T6) or None.
- Constraint: O(1) conditional lookup. First match wins.

**`generate_relance_message(template_id, params) → str or None`**

- Inputs: template_id (str), params dict with keys from {document, approver, days, lot, deadline}.
- Output: rendered message string (≤ 200 chars, UTF-8) or None if template_id is None.
- Constraint: O(1) string format. Word-boundary truncation at 200 chars if needed [PATCH 7, Fix 6].

### 7.4 Phase 3 Functions (Reports)

**`build_report_s1(suggestion_results) → DataFrame`**

- Inputs: List[Dict] of SuggestionResults.
- Output: S1 DataFrame (action distribution).
- Constraint: O(n) single pass aggregation. GROUP BY (suggested_action, source_sheet, priority_band).

**`build_report_s2(suggestion_results) → DataFrame`**

- Inputs: List[Dict] of SuggestionResults.
- Output: S2 DataFrame (VISA recommendations).
- Constraint: O(n) single pass aggregation. GROUP BY (proposed_visa, source_sheet).

**`build_report_s3(suggestion_results) → DataFrame`**

- Inputs: List[Dict] of SuggestionResults.
- Output: S3 DataFrame (relance/communication).
- Constraint: O(n) filter + format. FILTER WHERE relance_required=true.

---

## 8. Named Constants Registry

All named constants used in M5, consolidated for single-source-of-truth configuration.

### 8.1 Confidence Formula Constants [PATCH 3]

| Constant | Value | Type | Used In |
|---|---|---|---|
| BASE_CONFIDENCE | 0.30 | float | Layer 6 |
| W_CONSENSUS | 0.35 | float | Layer 6 |
| W_COMPLETENESS | 0.20 | float | Layer 6 |
| W_MISSING | 0.15 | float | Layer 6 |
| W_CONFLICT | 0.25 | float | Layer 6 |
| W_OVERDUE | 0.10 | float | Layer 6 |
| W_DEGRADED | 0.50 | float | Layer 6 |
| OVERDUE_PENALTY_CAP | 60 | int | Layer 6 |

### 8.2 Escalation Threshold Constants [PATCH 4]

| Constant | Value | Type | Used In |
|---|---|---|---|
| ESCALATION_CONSEC_REJ_DIR | 3 | int | Layer 5, rule 1 |
| ESCALATION_CONSEC_REJ_MOEX | 2 | int | Layer 5, rule 4 |
| ESCALATION_OVERDUE_DIR | 60 | int | Layer 5, rule 2 |
| ESCALATION_OVERDUE_MOEX | 30 | int | Layer 5, rule 5 |
| ESCALATION_SYSTEMIC_DIR | 10 | int | Layer 5, rule 3 |
| ESCALATION_SYSTEMIC_MOEX | 5 | int | Layer 5, rule 6 |

### 8.3 Action Priority Constants [PATCH 5]

| Constant | Value | Type | Used In |
|---|---|---|---|
| ESCALATION_BOOST_MOEX | 10 | int | action_priority formula |
| ESCALATION_BOOST_DIR | 20 | int | action_priority formula |
| OVERDUE_CAP | 30 | int | action_priority formula |
| OVERDUE_MULTIPLIER | 0.5 | float | action_priority formula |
| BLOCKING_BOOST | 5 | int | action_priority formula |

### 8.4 Relance Constants [PATCH 7]

| Constant | Value | Type | Used In |
|---|---|---|---|
| RELANCE_NOT_STARTED_DAYS | 7 | int | Layer 4, row 4f/4g |
| RELANCE_MAX_LENGTH | 200 | int | Template rendering |

### 8.5 Priority Band Thresholds (S1 Report) [V2.2.2 §9.2]

| Constant | Value | Type | Used In | Band |
|---|---|---|---|---|
| SCORE_BAND_CRITICAL | 80 | int | S1 report | 80–100 |
| SCORE_BAND_HIGH | 60 | int | S1 report | 60–79 |
| SCORE_BAND_MEDIUM | 40 | int | S1 report | 40–59 |
| SCORE_BAND_LOW | 0 | int | S1 report | 0–39 |

**priority_band derivation**:

```
if action_priority >= SCORE_BAND_CRITICAL: "CRITICAL"
elif action_priority >= SCORE_BAND_HIGH: "HIGH"
elif action_priority >= SCORE_BAND_MEDIUM: "MEDIUM"
else: "LOW"
```

Note: These bands are applied to action_priority (the M5-boosted value), not the raw M3 priority_score. action_priority may exceed M3 priority_score due to escalation, overdue, and blocking boosts. Band thresholds from [V2.2.2 §9.2] are applied to this boosted value.

---

## 9. Failure Handling

### 9.1 Per-Layer Failure

Every decision layer's fallback row (catch-all) produces:

| Field | Safe Default |
|---|---|
| suggested_action | HOLD |
| proposed_visa | NONE |
| confidence | 0.0 |
| reason_code | DEGRADED_ANALYSIS |
| escalation_level | NONE |
| relance_required | false |
| relance_targets | [] |
| relance_message | null |
| escalation_required | false |

### 9.2 Per-Item Exception Handling

If ANY unhandled exception occurs during compute_suggestion for a single item:

1. Catch the exception.
2. Log ERROR with: row_id, exception type, exception message, traceback.
3. Apply safe defaults (above) for ALL fields.
4. Set analysis_degraded = true.
5. Set based_on_lifecycle = lifecycle_state from M4 if available, else "UNKNOWN".
6. Continue to next item. NO ITEM IS EVER SKIPPED.

### 9.3 Report Generation Failure

If S1, S2, or S3 report generation fails:

1. Catch the exception.
2. Log ERROR with report name and exception details.
3. Return empty DataFrame with the correct schema (all columns present, zero rows).
4. Report failure MUST NOT affect per-item suggestion_results (already computed).

### 9.4 Upstream Data Corruption Guard

- M5 failure MUST NOT corrupt M1–M4 outputs [V2.2.2 §2.5].
- M5 operates on copies/views of M3/M4 data. No write-back, no in-place modification.
- If M5 throws an unrecoverable error at the top level → return (empty_list, empty_s1, empty_s2, empty_s3) with all correct schemas. Log CRITICAL.

### 9.5 Cache Invalidation

- Session cache key: (row_id, pipeline_run_id).
- Stale boundary: cached suggestions MUST NOT survive dataset_signature change [V2.2.2 §10.1].
- On pipeline_run_id change: entire M5 cache is invalidated.
- Idempotency: cache hit MUST be identical to fresh computation [PATCH 6]. This is guaranteed by the deterministic design — no need for cache validation beyond key matching.

---

## 10. Performance & Caching

### 10.1 Performance Requirements

| Operation | Target | Complexity |
|---|---|---|
| Single item suggestion | < 100ms | O(1) per layer, 6 layers |
| Full batch (5K items) | < 2s | O(n), n = queue size |
| Full batch (10K items) | < 5s | O(n) |
| S1 report generation | < 500ms | O(n) single pass |
| S2 report generation | < 500ms | O(n) single pass |
| S3 report generation | < 500ms | O(n) filter + format |

### 10.2 Caching Strategy [V2.2.2 §10.1, E3]

- M5 is computed on-demand when item/batch is requested.
- Cache key: (row_id, pipeline_run_id). Composite key ensures staleness detection.
- Cache scope: session-level. Not persisted across sessions.
- Cache invalidation: entire cache cleared when pipeline_run_id changes (new dataset imported or re-processed).
- No TTL-based expiry — staleness is determined exclusively by pipeline_run_id matching.

### 10.3 Idempotency Enforcement [PATCH 6]

- No randomness: no random(), no uuid4(), no datetime.now() in any output field.
- Float precision: confidence rounded to 4 decimal places using Python's round(value, 4).
- Integer fields: action_priority computed with int arithmetic, no rounding.
- List ordering: relance_targets, blocking_approvers, missing_approvers all sorted alphabetically.
- Dict ordering: reason_details keys sorted alphabetically. Top-level SuggestionResult keys sorted on serialization.
- Serialization: json.dumps(result, sort_keys=True, ensure_ascii=False) produces identical bytes across runs.

---

## 11. Edge Cases

### Edge Case 1: analysis_degraded = true

- **Handling**: Layer 1 hard override fires (row 1b). suggested_action=HOLD, proposed_visa=NONE, confidence=0.0, reason_code=DEGRADED_ANALYSIS. No relance, no escalation.
- **Rationale**: If M4 analysis is degraded, M5 cannot produce reliable recommendations. Conservative HOLD is safest.

### Edge Case 2: lifecycle_state = ON_HOLD

- **Handling**: Layer 1 hard override fires (row 1a). Identical to Edge Case 1.
- **Rationale**: ON_HOLD items are explicitly paused by guard logic. M5 should not recommend actions.

### Edge Case 3: All-HM items (EXCLUDED upstream)

- **Handling**: Layer 0 scope guard filters these out. If an EXCLUDED item somehow reaches M5 (defensive), Layer 0 returns SKIP — no SuggestionResult produced.
- **Guard**: If lifecycle_state=EXCLUDED is found in queue → log WARNING (should have been filtered upstream).

### Edge Case 4: Cross-lot items (same family, different sheets, different statuses)

- **Handling**: M5 processes each row independently based on its own M3/M4 data. Cross-lot divergence is reported via M4 A5 and logged in reason_details if present.
- **No special M5 logic**: Cross-lot coordination is an M4 analysis concern. M5 simply reflects the per-item lifecycle_state and analysis.

### Edge Case 5: Items with no deadline (has_deadline = false)

- **Handling**: is_overdue will be false (no deadline to exceed). days_overdue = 0. overdue_penalty in confidence formula = 0.0. overdue_boost in action_priority = 0. No escalation triggered by overdue rules (rules 2, 5 in Layer 5).
- **Confidence impact**: No overdue penalty applied. Other components (consensus, completeness, missing) still contribute normally.

### Edge Case 6: Items with revision_count = 0

- **Handling**: Should not exist in M3 queue (M2 enrichment ensures revision_count ≥ 1 for valid items). If encountered: A4.consecutive_rejections = 0, no blocking pattern detected. M5 processes normally using available data. Log WARNING.

### Edge Case 7: SUSPECT-flagged items

- **Handling**: SUSPECT items are flagged in M1/M2 quality scoring. If they reach M3 queue and M4 analysis, M5 processes them based on their lifecycle_state. The SUSPECT flag does not create a special M5 path — it may contribute to analysis_degraded if M4 set that flag.

### Edge Case 8: A4.consecutive_rejections > 0 but consensus_type ≠ ALL_REJECT

- **Handling**: This is valid (previous revisions were rejected but current revision has mixed or incomplete responses). Layer 2 uses lifecycle_state as primary discriminator, not A4 alone. If lifecycle_state = CHRONIC_BLOCKED, action = ESCALATE regardless of current consensus. If lifecycle_state = WAITING_RESPONSES (new revision in progress), action = CHASE_APPROVERS. consecutive_rejections is recorded in reason_details.rejection_depth for MOEX awareness.

### Edge Case 9: lifecycle_state contradicts M3 category

- **Handling**: Layer 2 catch-all row 2o fires. suggested_action=HOLD, reason_code=DEGRADED_ANALYSIS. Log ERROR with: row_id, lifecycle_state, category, consensus_type. This indicates a pipeline integrity issue that should be investigated.
- **M3 is authoritative** [V2.2.2 E2]: If lifecycle_state and category contradict, the catch-all ensures safe behavior while flagging the inconsistency.

### Edge Case 10: Float precision

- **Handling**: All confidence values rounded to 4 decimal places using round(value, 4) AFTER clamping. action_priority uses int arithmetic only. No floating-point comparison in decision logic — all decisions use enum/string/int comparisons.
- **Idempotency**: Ensures bit-level identical outputs [PATCH 6].

### Edge Case 11: Empty missing_approvers when CHASE_APPROVERS

- **Handling**: Defensive guard in Layer 4. If suggested_action=CHASE_APPROVERS but missing_approvers is empty → set relance_required=false, relance_targets=[], relance_message=null. Log WARNING: "CHASE_APPROVERS with empty missing_approvers for row_id={row_id}".
- **Rationale**: Cannot send relance if no targets. This is likely an upstream data anomaly.

### Edge Case 12: G1 blocker data unavailable

- **Handling**: If g1_blocker_index is empty (G1 failed in M4) → Layer 5 rules 3 and 6 (systemic blocker rules) are skipped entirely. Only A4/overdue thresholds and the rule-7 guard apply. Log WARNING.
- **Conservative default**: No systemic blocker alerts when G1 data is unavailable (avoids false positives).

### Edge Case 13: Items from UNPARSEABLE document families

- **Handling**: If a document family is marked UNPARSEABLE in upstream modules, it should not reach M3 queue. If it does (defensive), M4 likely set analysis_degraded=true → Layer 1 override fires → HOLD. If not degraded, process normally with available data.

---

## 12. Open Points / Spec Clarifications

### OP-01: T6 Template Trigger Ambiguity

- **What is unclear**: T6 trigger is defined as "INCOMPLETE + partial responses." This overlaps with T1 (CHASE_APPROVERS + not overdue). When should T6 be used instead of T1?
- **What the spec says**: [Fix #5] defines T1–T6 triggers. T6 says "INCOMPLETE + partial responses" without specifying how it differs from T1.
- **Assumption**: T6 is used when the item has SOME responses (replied > 0 AND pending > 0) but is not yet complete, while T1 is used when NO responses have been received (replied = 0). This gives T6 the "follow-up" semantics (some approvers have responded, urging the rest).
- **Why it matters**: Incorrect template selection affects MOEX communication tone and urgency.
- **Implementation guidance** [OBS-1]: At coding time, the implementer should add a Layer 4 sub-row before row 4b: `CHASE_APPROVERS + is_overdue=false + replied > 0 → T6`. Row 4b (T1) then fires only when replied = 0. This preserves first-match-wins semantics and keeps the decision deterministic. If this approach is chosen, update §2.6 Layer 4 table accordingly.

### OP-02: SYSTEMIC_BLOCKER as Primary reason_code — RESOLVED

- **Status**: RESOLVED by Fix 4.
- **Resolution**: SYSTEMIC_BLOCKER removed from primary reason_code enum. Systemic blocker detection is surfaced via reason_details (systemic_blocker_detected, systemic_blockers fields) and escalation_level (Layer 5) only. The primary reason_code for ESCALATE actions is always BLOCKING_LOOP from Layer 2.

### OP-03: Cross-Lot Warning in M5 Suggestions

- **What is unclear**: Should M5 include cross-lot divergence warnings from M4 A5 in its suggestions? The spec mentions A5 in M4 context but does not explicitly state how M5 should surface it.
- **What the spec says**: [V2.2.2 §3] lists A5 output but M5 planning prompt §3 (upstream context) lists A5 as "divergent statuses across sheets" with usage "Cross-lot warnings" — but no specific M5 field or layer handles it.
- **Assumption**: Cross-lot A5 data is included in reason_details as an informational field (e.g., reason_details.cross_lot_divergence = true/false) but does NOT affect suggested_action or any structured decision.
- **Why it matters**: Missing cross-lot surfacing could mean MOEX misses important inter-lot coordination needs.

### OP-04: days_since_diffusion null Handling for NOT_STARTED Relance

- **What is unclear**: Layer 4 row 4f uses days_since_diffusion ≥ 7 for NOT_STARTED relance. What if days_since_diffusion is null (date_diffusion missing)?
- **What the spec says**: M3 computes days_since_diffusion from date_diffusion. If date_diffusion is null, days_since_diffusion may be null or 0.
- **Assumption**: If days_since_diffusion is null → treat as 0 → row 4g fires (< 7) → relance_required=false. Log WARNING: "null days_since_diffusion for NOT_STARTED item row_id={row_id}".
- **Why it matters**: Null handling affects whether NOT_STARTED items receive their first relance communication.

### OP-05: Relance Template for ESCALATE from Stale WAITING — RESOLVED

- **Status**: RESOLVED — The spec [V2.2.2 §3.3] definitively maps ESCALATE only to CHRONIC_BLOCKED lifecycle. Overdue modifies escalation_level and reason_code but never suggested_action. A WAITING_RESPONSES item that is overdue gets CHASE_APPROVERS with potentially elevated escalation_level (MOEX/DIRECTION), not ESCALATE.

### OP-06: UI Label "Action suggérée" Disclaimer [V2.2.2 §9, E4]

- **What is unclear**: The spec references UI labels and disclaimers. M5 is a data module — does it need to embed UI text in its output?
- **What the spec says**: [E4 §9] specifies "Action suggérée" label with disclaimer text.
- **Assumption**: M5 does NOT produce UI text. It produces structured data (SuggestionResult). UI labels and disclaimers are the responsibility of the presentation layer (future module). M5 output fields (suggested_action, proposed_visa, etc.) are machine-readable enums, not display strings.
- **Why it matters**: Prevents scope creep. M5 stays a pure data/logic module.

### OP-07: Cache Granularity — Batch vs. Item

- **What is unclear**: The spec says M5 is "computed on-demand when item/batch requested" [E3]. Should the cache work at individual item level or batch level?
- **What the spec says**: Cache key is (row_id, pipeline_run_id). This implies per-item caching.
- **Assumption**: Per-item caching. Each SuggestionResult is cached independently by (row_id, pipeline_run_id). A batch request checks cache for each item; cache misses are computed fresh. Reports (S1–S3) are recomputed from the full suggestion_results list each time (not independently cached), as they aggregate across items.
- **Why it matters**: Affects memory usage and cache invalidation complexity.

---

## 13. Success Criteria (Complete)

| # | Criterion | Verification Method |
|---|---|---|
| 1 | Every valid M3 queue item (lifecycle_state ≠ EXCLUDED) produces exactly one SuggestionResult. EXCLUDED items in queue are logged as WARNING and omitted. | Count assertion: len(results) = len(m3_queue) - count(EXCLUDED encountered) |
| 2 | suggested_action is always a valid enum value | GP8 enum validation on every output |
| 3 | proposed_visa is always a valid enum value | GP8 enum validation on every output |
| 4 | confidence is always in [0.0, 1.0] with 4 decimal places | Range check + precision check |
| 5 | action_priority is always in [0, 100] integer | Range check + type check |
| 6 | M5 does not modify any M1–M4 data | Assertion: input DataFrames unchanged after M5 execution |
| 7 | Relance templates are well-formed French, ≤ 200 chars | Length check on all relance_message outputs |
| 8 | S1, S2, S3 reports have correct schemas | Column presence + type validation |
| 9 | Performance: < 100ms per item, < 5s for 10K items | Timing assertions in test suite |
| 10 | GP10: M5 fully functional with ai_available=false | Run full suite with ai_available=false, verify identical structured outputs |
| 11 | Every lifecycle_state × consensus_type in decision table resolves to exactly one row [PATCH 1] | Exhaustive unit tests for all combinations |
| 12 | Every suggested_action × consensus_type in visa table resolves to exactly one proposed_visa [PATCH 2] | Exhaustive unit tests for all combinations |
| 13 | Confidence formula produces identical results for identical inputs, rounded to 4dp [PATCHES 3+6] | Repeated computation tests with assertion on equality |
| 14 | Escalation thresholds are all explicit named constants with no derived logic [PATCH 4] | Code review: no magic numbers in escalation logic |
| 15 | action_priority formula is explicit with named constants [PATCH 5] | Code review: formula matches spec exactly |
| 16 | Identical M3+M4 inputs produce bit-level identical JSON output [PATCH 6] | Serialize twice, compare bytes |
| 17 | All relance templates ≤ 200 chars, formal French, fixed placeholders only [PATCH 7] | Template unit tests with max-length parameter values |
| 18 | Every decision table ends with explicit fallback row — no unhandled branches [PATCH 8] | Code review + exhaustive input combination tests |

---

## 14. Patch Cross-Reference

| Patch | Section(s) in This Plan | New Named Constants | New Tables |
|---|---|---|---|
| P1 | §2.4 (Layer 2) | — | Full decision table (15 rows) |
| P2 | §2.5 (Layer 3) | — | Visa lookup (12 rows) |
| P3 | §2.8 (Layer 6) | BASE_CONFIDENCE, W_CONSENSUS, W_COMPLETENESS, W_MISSING, W_CONFLICT, W_OVERDUE, W_DEGRADED, OVERDUE_PENALTY_CAP | Component defs, worked examples |
| P4 | §2.7 (Layer 5) | ESCALATION_CONSEC_REJ_DIR/MOEX, ESCALATION_OVERDUE_DIR/MOEX, ESCALATION_SYSTEMIC_DIR/MOEX | Threshold table (8 rows) |
| P5 | §2.9 (action_priority) | ESCALATION_BOOST_MOEX/DIR, OVERDUE_CAP, OVERDUE_MULTIPLIER, BLOCKING_BOOST | Priority formula + constants |
| P6 | §1 (overview), §3 (schema), §10.3 | — | Constraint (not table) |
| P7 | §4 (Templates) | RELANCE_MAX_LENGTH = 200 | Constraint table + 6 templates |
| P8 | §2.2–§2.8 (All Layers) | — | Fallback rows in every table |

---

**END OF MODULE 5 IMPLEMENTATION PLAN**
