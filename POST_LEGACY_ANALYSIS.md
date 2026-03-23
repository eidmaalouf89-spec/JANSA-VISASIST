# Post-Legacy Exclusion Analysis Report

**Date**: 2026-03-23
**Dataset**: 17&CO Tranche 2 du 23 mars 2026, with ancien cross-reference from GrandFichier_1.xlsx
**Scope**: Active dataset only (legacy docs excluded)
**Purpose**: Refresh evidence base for override rules after legacy exclusion

---

## 1. Doc Type Inventory (Active Dataset)

| Metric | Value |
|---|---|
| Total active docs | 2,396 |
| Total active rows (GED long) | 11,760 |
| Doc types | 27 |
| Lot families | 37 |
| Cross-tab cells (type_doc × lot_family) | 162 |

### Doc Type Distribution

| TYPE_DOC | COUNT | % |
|---|---|---|
| MAT | 824 | 34.4% |
| PLN | 349 | 14.6% |
| TMX | 196 | 8.2% |
| ARM | 189 | 7.9% |
| NDC | 169 | 7.1% |
| COF | 126 | 5.3% |
| SYQ | 110 | 4.6% |
| DET | 94 | 3.9% |
| RSX | 68 | 2.8% |
| REP | 50 | 2.1% |
| LST | 49 | 2.0% |
| DVM | 43 | 1.8% |
| RSV | 26 | 1.1% |
| CLP | 19 | 0.8% |
| QLT | 15 | 0.6% |
| LTE | 14 | 0.6% |
| Others (11 types) | 75 | 3.1% |

### Top 15 Lot Families

| LOT | DOCS |
|---|---|
| 03 (GOE) | 486 |
| 31 (CFO) | 358 |
| 41 (CVC) | 244 |
| 42 (PLB) | 236 |
| 34 (SSI) | 150 |
| 08 (MUR-RID) | 117 |
| 12A (MEN-INT) | 112 |
| 33 (CFA) | 104 |
| 43 (SPK) | 57 |
| 05 (MEN-EXT) | 53 |
| 18 (SDS) | 50 |
| 35 (GTB) | 44 |
| 11 (CLD-FPL) | 38 |
| 04 (ETANCH) | 35 |
| 07 (BFUP) | 34 |

---

## 2. Reviewer Patterns by (type_doc, lot_family)

### Classification Summary

| Classification | Count | Description |
|---|---|---|
| VERY_STRONG | 461 | ≥90% presence, ≥3 docs |
| STRONG | 26 | 70–89% presence, ≥3 docs |
| CONDITIONAL | 145 | 50–69% presence, ≥2 docs |
| NOISE | 227 | Below thresholds |
| **Total** | **859** | |

### Key VERY_STRONG Patterns (by lot family)

**Universal reviewers** (appear across nearly all lots at ≥90%): AMO HQE, Bureau de Contrôle, Maître d'Oeuvre EXE, ARCHITECTE. These four form the "core circuit" for most doc types.

**Specialist reviewers** (lot-specific, VERY_STRONG):

| Reviewer | Lot Families | Doc Types |
|---|---|---|
| BET Acoustique | 03, 05, 06, 08, 12A, 31, 41, 42, 43 | MAT, PLN, DET, COF, CLP, NDC, TMX, SYQ, RSX |
| BET Electricité | 31, 33, 34, 35 | MAT, PLN, TMX, SYQ |
| BET Plomberie | 42 | MAT, PLN, NDC, DET, SYQ |
| BET CVC | 41 | MAT, PLN, NDC, RSX, SYQ, LTE |
| BET Façade | 05, 06, 08 | MAT, PLN, DET, DVM |
| BET Structure | 01, 02, 03 | CRV, MTD, NDC, ARM, COF, PLN |
| BET VRD | 62 | MAT, PLN |
| BET Géotech | 02 | NDC |
| BET POL | 01 | CRV, MTD |
| BET EV | 62 | MAT (93%) — STRONG, not VERY_STRONG |
| BET Ascenseur | 51 | PLN (83%) — STRONG, not VERY_STRONG |
| Sollicitation supplémentaire | 61, 62 | MAT (93–100%) |
| B13 - METALLERIE SERRURERIE | 61, 62 | MAT (93–100%) |

### STRONG Patterns (Potential Future Candidates)

26 patterns at 70–89% presence. Notable:

- PLN/03: ARCHITECTE (88%), BET Acoustique (88%), Maître d'Oeuvre EXE (88%)
- DET/42: ARCHITECTE (88%), BET Acoustique (88%), Maître d'Oeuvre EXE (88%)
- LST/12A: all 5 core reviewers at 83%
- PLN/51: all core + BET Ascenseur at 83%
- PLN/62: BET EV at 89%

---

## 3. Assignment Source & Residuals

### Assignment Source Distribution

| Source | Rows | % |
|---|---|---|
| MATRIX | 7,071 | 60.1% |
| DATA_OVERRIDE | 2,668 | 22.7% |
| GED_PRESENCE | 2,021 | 17.2% |

### Assignment Type Distribution

| Type | Rows | % |
|---|---|---|
| REQUIRED_VISA | 8,013 | 68.1% |
| NOT_ASSIGNED | 2,021 | 17.2% |
| INFORMATIONAL | 1,726 | 14.7% |

### Residuals

| Metric | Value |
|---|---|
| **UNKNOWN_REQUIRED rows** | **0** |
| GLOBAL_TYPE fallback rows | 0 |
| WILDCARD_LOT fallback rows | 0 |
| DATA_OVERRIDE docs | 1,765 |

**Zero UNKNOWN_REQUIRED** — every assigned reviewer is resolved through either the circuit matrix or data override rules. No unresolved assignments remain.

---

## 4. Candidate Override Rules

### Current State

| Metric | Value |
|---|---|
| Current override rules (type_doc, lot_family combos) | 89 |
| Current override patterns (type_doc, lot_family, reviewer triples) | 129 |
| New VERY_STRONG patterns (cleaned data) | 461 |

### Delta vs Current Overrides

| Category | Count |
|---|---|
| Patterns KEPT (still VERY_STRONG) | 122 |
| Patterns ADDED (new candidates) | 339 |
| Patterns REMOVED (no longer qualify) | 7 |

### Removed Patterns (7)

These fell below the VERY_STRONG threshold after legacy exclusion:

| Pattern | New Status | Reason |
|---|---|---|
| (ARM, 03, BET Acoustique) | Gone | Combo no longer in data |
| (CPE, 01, BET POL) | NOISE | Only 1 doc remains |
| (CPE, 01, BET Structure) | NOISE | Only 1 doc remains |
| (DET, 06B, BET Acoustique) | CONDITIONAL | Only 2 docs remain |
| (DET, 06B, BET Façade) | CONDITIONAL | Only 2 docs remain |
| (DET, 42, BET Acoustique) | STRONG | Dropped to 88% |
| (REP, 12, BET Acoustique) | CONDITIONAL | Only 2 docs remain |

### New Candidate Patterns (339)

339 new VERY_STRONG patterns emerged, mostly core circuit reviewers (AMO HQE, Bureau de Contrôle, ARCHITECTE, Maître d'Oeuvre EXE) that were already resolved by the circuit matrix but now qualify for explicit overrides. The current 129 overrides focused only on **specialist** BET reviewers; the 339 new candidates are predominantly the universal core reviewers.

**Breakdown of 339 new candidates by reviewer:**

| Reviewer | New Patterns |
|---|---|
| Bureau de Contrôle | ~70 |
| AMO HQE | ~70 |
| Maître d'Oeuvre EXE | ~65 |
| ARCHITECTE | ~65 |
| BET Structure | ~15 |
| BET Acoustique | ~10 |
| Others (specialist BETs) | ~44 |

---

## 5. Differences Caused by Legacy Exclusion

### Documents Removed

| Metric | Before | After | Delta |
|---|---|---|---|
| Active docs | 2,643 | 2,396 | −247 |
| Active rows | 12,890 | 11,760 | −1,130 |
| Doc types present | 28 | 27 | −1 (DOE removed) |

### Removed by Type Doc (top 10)

| TYPE_DOC | Removed |
|---|---|
| MAT | 116 |
| PLN | 32 |
| NDC | 22 |
| DET | 12 |
| TMX | 10 |
| CLP | 7 |
| ARM | 6 |
| COF | 6 |
| REP | 5 |
| SYQ | 5 |

### Removed by Lot Family (top 10)

| LOT | Removed |
|---|---|
| 31 (CFO) | 80 |
| 03 (GOE) | 57 |
| 34 (SSI) | 22 |
| 08 (MUR-RID) | 17 |
| 42 (PLB) | 11 |
| 41 (CVC) | 10 |
| 33 (CFA) | 9 |
| 01 (VDSTP) | 6 |
| 12A (MEN-INT) | 6 |
| 06 (RVT-FAC) | 6 |

Legacy docs concentrated in lots 31 (32%) and 03 (23%) — the largest lots with the most historical revisions.

### Pattern Classification Shift

| Classification | Before | After | Delta |
|---|---|---|---|
| VERY_STRONG | 467 | 461 | −6 |
| STRONG | 48 | 26 | −22 |
| CONDITIONAL | 127 | 145 | +18 |
| NOISE | 249 | 227 | −22 |

**14 patterns upgraded** to VERY_STRONG (from STRONG) — legacy docs were diluting their presence rates. Most notable: IMP/31 (all 6 patterns), NDC/02 (3 patterns), NDC/03 (3 patterns), PLN/03 (2 patterns).

**20 patterns downgraded** from VERY_STRONG — legacy removal reduced doc counts below the ≥3 threshold or reduced presence rates. Most affected: CPE/01 (6 patterns → NOISE, only 1 doc left), DET/06B (6 patterns → CONDITIONAL, only 2 docs left).

### Assignment Source Shift

| Source | Before | After | Delta |
|---|---|---|---|
| MATRIX | 7,692 (59.7%) | 7,071 (60.1%) | −621 |
| DATA_OVERRIDE | 2,968 (23.0%) | 2,668 (22.7%) | −300 |
| GED_PRESENCE | 2,230 (17.3%) | 2,021 (17.2%) | −209 |

Proportions remain stable — legacy exclusion affected all sources roughly equally.

---

## 6. Recommendations

1. **The 7 removed override patterns should be cleaned from `project_overrides.py`** — they no longer qualify. Specifically: (ARM,03,BET Acoustique), (CPE,01,*), (DET,06B,BET Acoustique/BET Façade), (DET,42,BET Acoustique), (REP,12,BET Acoustique).

2. **The 339 new candidates are mostly core circuit reviewers** already handled by MATRIX. Adding them as explicit overrides would shift ~7,000 rows from MATRIX to DATA_OVERRIDE without changing any actual assignments. Consider whether explicitness is worth the rule maintenance cost.

3. **14 upgraded patterns (STRONG → VERY_STRONG) deserve attention** — these are specialist BET assignments that became cleaner after legacy removal: IMP/31 and NDC/02-03 patterns.

4. **26 STRONG patterns** (70–89%) are borderline candidates. If legacy docs were inflating noise, these may stabilize further as new data arrives.

5. **Zero UNKNOWN_REQUIRED residuals** — the current rule set (circuit matrix + 129 overrides) resolves all assignments cleanly. No urgent gaps to fill.

---

*Analysis only. No code changes made.*
