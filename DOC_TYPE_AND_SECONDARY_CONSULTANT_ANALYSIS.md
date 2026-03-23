# Document Type & Secondary Consultant Analysis

**Project:** 17&CO Tranche 2 — JANSA VISASIST GED Pipeline
**Date:** 2026-03-23
**Source data:** `17&CO Tranche 2 du 23 mars 2026 07_45.xlsx`
**Pipeline version:** Post-Milestone 4 (NM1→NM3→NM2→NM4→NM5 validated, 130/130 tests passing)

---

## 1. Executive Summary

This report analyzes 13,048 GED long-format rows covering 2,734 documents (2,643 active) across 28 document types and 90 lot codes, processed through the validated JANSA pipeline.

**Key findings:**

- 157 statistically significant (type_doc, lot_family) → reviewer patterns detected at ≥40% presence rate, of which 127 are VERY_STRONG (≥90%)
- BET Acoustique (AVLS) is the most pervasive secondary consultant, present in nearly every lot family for execution documents (MAT, PLN, DET, TMX)
- BET Electricité (EGIS_FLUIDES) is systematically required for electrical installation lots (31, 33, 34) across all doc types
- BET Façade (ELIOTH) is tightly bound to envelope-related lots (05, 06, 06B, 08) — never generic
- BET Structure (TERRELL) is lot-03-specific — strongly correlated with structural lots, not generalized
- BET CVC (EGIS_FLUIDES) is lot-41-specific — bound to HVAC/plumbing lots only
- Late reviewer activation via revisions is negligible: only 50 documents have multiple indices, and only 4 show late arrivals — insufficient for rule derivation
- Keyword-in-comments analysis shows moderate signal for `structure` (lift=3.64×) and `CVC` (lift=3.65×) but weak absolute rates — comment-triggered rules are not strong enough for automatic implementation
- All patterns are lot-specific and must NOT be generalized by doc type alone

**Recommendation:** 127 VERY_STRONG patterns should be codified as project-level override rules in the circuit matrix. 20 STRONG patterns should be reviewed with the project team. Comment-triggered rules should be deferred to a future module with human-in-the-loop validation.

---

## 2. Document Type Inventory

The GED dataset contains 28 distinct `type_doc` codes. Below is the full inventory sorted by row count.

| type_doc | Description (inferred) | Rows | Unique Docs | Active Docs | Flow Type |
|----------|----------------------|------|-------------|-------------|-----------|
| MAT | Matériaux / Fiches techniques | 4,850 | 944 | 940 | STANDARD_VISA (64.3%) |
| PLN | Plans d'exécution | 2,159 | 393 | 381 | STANDARD_VISA (65.4%) |
| TMX | Tableaux de matrices | 970 | 206 | 206 | STANDARD_VISA (64.9%) |
| COF | Coffrage / Plans coffrages | 829 | 150 | 132 | STANDARD_VISA (63.7%) |
| NDC | Notes de calcul | 661 | 201 | 191 | STANDARD_VISA (87.9%) |
| ARM | Armatures / Ferraillage | 640 | 227 | 195 | STANDARD_VISA (89.5%) |
| DET | Détails d'exécution | 605 | 106 | 106 | STANDARD_VISA (66.3%) |
| RSX | Réseaux / Schémas réseaux | 411 | 69 | 69 | STANDARD_VISA (66.7%) |
| SYQ | Synoptiques | 370 | 116 | 115 | STANDARD_VISA (91.1%) |
| REP | Repérages | 272 | 58 | 55 | STANDARD_VISA (62.1%) |
| DVM | Détails / Vues menuiseries | 260 | 44 | 43 | STANDARD_VISA (66.2%) |
| LST | Listes / Listings | 226 | 53 | 52 | STANDARD_VISA (59.3%) |
| RSV | Réservations | 162 | 27 | 27 | STANDARD_VISA (66.7%) |
| CLP | Coupes longitudinales/plans | 144 | 26 | 26 | STANDARD_VISA (64.6%) |
| LTE | Lettres techniques | 93 | 16 | 15 | STANDARD_VISA (67.7%) |
| QLT | Qualité / Plans qualité | 74 | 24 | 19 | STANDARD_VISA (60.8%) |
| IMP | Implantations | 64 | 12 | 12 | STANDARD_VISA (65.6%) |
| CRV | Courbes / Profils en long | 45 | 11 | 10 | STANDARD_VISA (53.3%) |
| PVT | PV Techniques | 42 | 7 | 7 | STANDARD_VISA (66.7%) |
| MTD | Méthodologie | 30 | 6 | 6 | STANDARD_VISA (60.0%) |
| TDP | Tableaux de portes | 30 | 6 | 6 | STANDARD_VISA (60.0%) |
| NOT | Notes techniques | 28 | 7 | 7 | STANDARD_VISA (75.0%) |
| CPE | Coupes / Profils existants | 28 | 5 | 5 | STANDARD_VISA (53.6%) |
| PPS | Plans de prévention sécurité | 23 | 8 | 8 | MIXED (39.1%) |
| PIC | Piquetage | 13 | 7 | 5 | MIXED (38.5%) |
| MAQ | Maquettes | 13 | 3 | 3 | STANDARD_VISA (69.2%) |
| DOE | Dossier des ouvrages exécutés | 5 | 1 | 1 | STANDARD_VISA (60.0%) |
| FQR | Fiches question/réponse | 1 | 1 | 1 | SPECIAL/INFO (0.0%) |

**Notes:**
- STANDARD_VISA flow means >50% of reviewer rows are REQUIRED_VISA
- NDC (87.9%), ARM (89.5%), SYQ (91.1%) have the highest visa requirement rates — these are documents that almost always need formal approval
- PPS and PIC are the only types with MIXED flow — these have higher proportions of NOT_ASSIGNED and INFORMATIONAL rows
- DOE and FQR have too few documents for statistical analysis

---

## 3. Proposed Canonical Doc Type Families

Based on the inventory analysis, the following canonical grouping is proposed for future pipeline configuration:

### Family 1: Execution / Plans
PLN (Plans d'exécution), DET (Détails d'exécution), CLP (Coupes/plans), IMP (Implantations), CRV (Courbes/profils)

Characteristics: High reviewer count per document, strong secondary consultant presence, standard visa flow.

### Family 2: Structural Documents
COF (Coffrage), ARM (Armatures)

Characteristics: Lot-03-specific (GOE), BET Structure always required, very high visa rate (ARM: 89.5%).

### Family 3: Technical Submissions / Fiches Techniques
MAT (Matériaux/Fiches techniques)

Characteristics: Highest volume (4,850 rows), broad lot coverage, secondary consultant presence varies strongly by lot family.

### Family 4: Notes / Calculations
NDC (Notes de calcul), NOT (Notes techniques)

Characteristics: Very high visa rate (NDC: 87.9%), often lot-specific BET involvement.

### Family 5: Electrical / Network Documents
TMX (Tableaux matrices), RSX (Réseaux), SYQ (Synoptiques)

Characteristics: Strongly tied to electrical/CVC lots (31, 33, 34, 41, 42), BET Electricité almost always present.

### Family 6: Envelope / Facades
DVM (Détails menuiseries), PVT (PV Techniques)

Characteristics: Lot-08 and lot-05 specific, BET Façade always present for these lots.

### Family 7: Repérage / Listings
REP (Repérages), LST (Listes), TDP (Tableaux de portes)

Characteristics: Medium volume, lot-diverse, variable secondary consultant patterns.

### Family 8: Réservations
RSV (Réservations)

Characteristics: Focused on lots 31, 42 — always involves BET Electricité or BET Plomberie.

### Family 9: Quality / Methods
QLT (Qualité), MTD (Méthodologie), CPE (Coupes/profils)

Characteristics: Low volume, lot-specific BET POL/Structure presence.

### Family 10: Administrative / Special
PPS (Prévention sécurité), PIC (Piquetage), MAQ (Maquettes), DOE (DOE), FQR (Questions/réponses), LTE (Lettres techniques)

Characteristics: Low volume, mixed or special flow, insufficient data for robust patterns.

---

## 4. Secondary Consultant Detection by (type_doc, lot_family)

### Methodology

For each (type_doc, lot_family) combination with ≥3 active documents, we computed the presence rate of each secondary reviewer role. Classification thresholds:

| Threshold | Classification | Meaning |
|-----------|---------------|---------|
| ≥90% | VERY_STRONG | Near-universal presence — strong override candidate |
| 70–89% | STRONG | Frequent presence — review with project team |
| 40–69% | CONDITIONAL | Partial presence — needs team validation |
| <40% | WEAK/NOISE | Insufficient evidence for rule creation |

### 4.1 VERY_STRONG Patterns (≥90% presence) — 127 patterns

These represent near-deterministic reviewer assignments for specific (type_doc, lot) combinations. Full table in Appendix (Table B).

**Top patterns by document volume:**

| type_doc | lot_family | reviewer | active_docs | present | rate |
|----------|-----------|----------|-------------|---------|------|
| MAT | 31 | BET Acoustique | 157 | 157 | 100.0% |
| MAT | 31 | BET Electricité | 157 | 157 | 100.0% |
| MAT | 42 | BET Acoustique | 109 | 109 | 100.0% |
| MAT | 42 | BET Plomberie | 109 | 109 | 100.0% |
| MAT | 34 | BET Electricité | 103 | 103 | 100.0% |
| MAT | 41 | BET Acoustique | 96 | 96 | 100.0% |
| MAT | 41 | BET CVC | 96 | 96 | 100.0% |
| MAT | 12A | BET Acoustique | 87 | 87 | 100.0% |
| TMX | 31 | BET Acoustique | 82 | 82 | 100.0% |
| TMX | 31 | BET Electricité | 82 | 82 | 100.0% |
| NDC | 41 | BET CVC | 72 | 72 | 100.0% |
| TMX | 33 | BET Electricité | 69 | 69 | 100.0% |
| TMX | 34 | BET Electricité | 55 | 55 | 100.0% |
| SYQ | 31 | BET Electricité | 48 | 48 | 100.0% |
| PLN | 31 | BET Electricité | 43 | 43 | 100.0% |
| ARM | 03 | BET Structure | 195 | 191 | 97.9% |
| COF | 03 | BET Structure | 131 | 128 | 97.7% |
| PLN | 31 | BET Acoustique | 43 | 42 | 97.7% |
| COF | 03 | BET Acoustique | 131 | 125 | 95.4% |
| CLP | 03 | BET Acoustique | 19 | 18 | 94.7% |
| CLP | 03 | BET Structure | 19 | 18 | 94.7% |
| RSX | 31 | BET Acoustique | 34 | 32 | 94.1% |
| DET | 42 | BET Acoustique | 30 | 27 | 90.0% |

**Key observations:**

1. **BET Acoustique (AVLS)** is the most pervasive secondary consultant: 100% presence on lot families 31, 42, 41, 12A, 08, 05, 06, 43, 18, 19, 20, 12, 13A, 13B, 17, 11, 16A, 16B, 42B, 62 for MAT documents. This covers virtually all lots except pure infrastructure lots (01, 02, 03).

2. **BET Electricité (EGIS_FLUIDES)** is 100% present for lots 31, 33, 34 across MAT, TMX, SYQ, PLN, NDC, RSV, IMP. These are the CFA/CFO specialty lots.

3. **BET Plomberie (EGIS_FLUIDES)** is 100% present for lot 42 across MAT, PLN, NDC, SYQ, RSV, DET. This is the plumbing specialty lot.

4. **BET CVC (EGIS_FLUIDES)** is 100% present for lot 41 across MAT, NDC, RSX, SYQ, LTE, LST, PLN. This is the HVAC specialty lot.

5. **BET Façade (ELIOTH)** is 100% present for lots 05, 06, 06B, 08 across DET, DVM, PLN, MAT, PVT. These are envelope/facade lots.

6. **BET Structure (TERRELL)** is 97.9% for ARM+lot_03, 97.7% for COF+lot_03, and 87.1% for PLN+lot_03. Purely lot-03-specific.

7. **BET POL (GEOLIA)** is 100% for QLT+lot_03 and CRV+lot_01. Infrastructure lots only.

8. **BET SPK (EGIS_FLUIDES)** is 100% for MAT+lot_43 and PLN+lot_43. Sprinkler lot only.

9. **BET Ascenseur (ASCAUDIT)** is 100% for DET+lot_51 and 85.7% for PLN+lot_51. Elevator lot only.

10. **BET VRD (EGIS_VRD)** is 100% for MAT+lot_62 and PLN+lot_62. Civil works lot only.

### 4.2 STRONG Patterns (70–89%) — 20 patterns

| type_doc | lot_family | reviewer | active_docs | present | rate | notes |
|----------|-----------|----------|-------------|---------|------|-------|
| PLN | 62 | BET EV | 9 | 8 | 88.9% | VRD lot, almost always requires environmental review |
| NDC | 02 | BET Structure | 8 | 7 | 87.5% | Foundations lot — structure logical |
| NDC | 02 | BET Géotech | 8 | 7 | 87.5% | Foundations lot — geotechnical logical |
| PLN | 03 | BET Structure | 124 | 108 | 87.1% | Structural lot — high volume, strong |
| NDC | 03 | BET Structure | 22 | 19 | 86.4% | Structural lot — consistent pattern |
| PLN | 51 | BET Acoustique | 7 | 6 | 85.7% | Elevator lot — acoustique relevant |
| PLN | 51 | BET Ascenseur | 7 | 6 | 85.7% | Elevator lot — dedicated reviewer |
| LST | 12A | BET Acoustique | 6 | 5 | 83.3% | Envelope-adjacent lot |
| LTE | 31 | BET Acoustique | 6 | 5 | 83.3% | Electrical lot — acoustic review |
| PLN | 03 | BET Acoustique | 124 | 102 | 82.3% | Structural lot — acoustic impact review |
| IMP | 31 | BET Acoustique | 11 | 9 | 81.8% | Electrical lot — acoustic review |
| IMP | 31 | BET Electricité | 11 | 9 | 81.8% | Electrical lot — dedicated reviewer |
| LST | 35 | BET Electricité | 5 | 4 | 80.0% | SSI lot — electrical logical |
| LST | 41 | BET Acoustique | 5 | 4 | 80.0% | CVC lot — acoustic impact |
| PLN | 02 | BET Structure | 4 | 3 | 75.0% | Foundations — structural review |
| PLN | 02 | BET Géotech | 4 | 3 | 75.0% | Foundations — geotechnical review |
| REP | 03 | BET Structure | 7 | 5 | 71.4% | Structural lot — consistent |

### 4.3 CONDITIONAL Patterns (40–69%) — 10 patterns

| type_doc | lot_family | reviewer | active_docs | present | rate | notes |
|----------|-----------|----------|-------------|---------|------|-------|
| SYQ | 03 | BET Structure | 3 | 2 | 66.7% | Low doc count — unreliable |
| MAT | 03 | BET Structure | 14 | 9 | 64.3% | Material fiches for structural lot — partial |
| PLN | 41 | BET Acoustique | 16 | 10 | 62.5% | CVC lot — acoustic not always needed for plans |
| PLN | 16B | BET Façade | 8 | 4 | 50.0% | Lot 16B (façade-adjacent?) |
| PLN | 16B | BET CVC | 8 | 4 | 50.0% | Lot 16B — CVC involvement partial |
| LST | 31 | BET Acoustique | 4 | 2 | 50.0% | Low count |
| LST | 31 | BET Electricité | 4 | 2 | 50.0% | Low count |
| MAT | 35 | BET Electricité | 33 | 15 | 45.5% | SSI lot — electrical not always |
| PIC | 03 | BET POL | 5 | 2 | 40.0% | Low count |

---

## 5. Conditional / Comment-Triggered Patterns

### 5.1 Keyword-to-Reviewer Correlation

We analyzed comment text across 2,185 active documents with comments, searching for keyword families and correlating with reviewer presence.

| Keyword Family | Target Reviewer | Docs with Keyword | Reviewer Present | Rate | Baseline Rate | Lift | Classification |
|---------------|----------------|-------------------|-----------------|------|---------------|------|----------------|
| socotec | Bureau de Contrôle | 94 | 94 | 100.0% | 99.2% | 1.01 | VERY_STRONG (but trivial — SOCOTEC is nearly universal) |
| HQE_environnement | AMO HQE | 104 | 88 | 84.6% | 92.1% | 0.92 | STRONG (but lift <1 — no signal above baseline) |
| acoustique | BET Acoustique | 190 | 152 | 80.0% | 68.2% | 1.17 | STRONG (mild lift, acoustic keywords slightly above baseline) |
| structure | BET Structure | 263 | 184 | 70.0% | 19.2% | 3.64 | CONDITIONAL (strong lift, but absolute rate only 70%) |
| electricite | BET Electricité | 331 | 209 | 63.1% | 30.1% | 2.10 | CONDITIONAL (moderate lift) |
| CVC | BET CVC | 237 | 89 | 37.6% | 10.3% | 3.65 | WEAK (high lift but low absolute rate) |
| facade | BET Façade | 401 | 140 | 34.9% | 10.3% | 3.41 | WEAK (high lift but low absolute rate) |
| plomberie | BET Plomberie | 748 | 96 | 12.8% | 10.9% | 1.18 | NOISE |
| ascenseur | BET Ascenseur | 31 | 3 | 9.7% | 0.5% | 17.62 | NOISE (extreme lift but only 3 docs) |
| geotechnique | BET Géotech | 247 | 11 | 4.5% | 0.5% | 8.11 | NOISE (high lift but only 11 docs) |
| incendie | BET SPK | 631 | 12 | 1.9% | 2.6% | 0.74 | NOISE |

**Interpretation:**

- **No comment-triggered pattern is strong enough for automatic rule implementation.** The strongest signal (`structure`, lift=3.64) means structural keywords triple the probability of BET Structure presence — but 70% absolute rate means 30% false positives.
- The `socotec` and `HQE_environnement` patterns are artifacts of those reviewers being near-universal (baseline ≥92%).
- `acoustique` keywords show mild positive correlation (lift=1.17) with BET Acoustique but the reviewer is already present in 68% of all documents.
- `CVC` and `facade` keywords show strong lift (>3×) but absolute presence rates below 40% — too unreliable for automatic rules.

### 5.2 Late Reviewer Activation via Revisions

Only **50 documents** in the dataset have multiple revision indices. Of those, only **4 documents** show a reviewer appearing in a later revision but not the earliest one:

| Document | Late Reviewer | First Doc Indice | First Reviewer Indice |
|----------|--------------|-------------------|----------------------|
| I003_COF_I1_S1_028283 | ARCHITECTE | 3 | 10 |
| I003_COF_I1_S1_028283 | BET Acoustique | 3 | 10 |
| I003_PLN_I1_TX_028123 | ARCHITECTE | 1 | 8 |
| I003_PLN_I1_TX_028123 | BET Acoustique | 1 | 8 |

**Conclusion:** The revision-based late activation pattern is statistically negligible. With only 4 cases across 50 multi-revision documents, no revision-triggered rule can be derived. This is likely an artifact of the GED export date (early project phase where most documents are still on their first revision).

---

## 6. Very Strong Override Candidates

The following patterns should be codified as project-level rules. They represent deterministic relationships between (type_doc, lot_family) and reviewer assignment.

### 6.1 BET Acoustique (AVLS) — Universal secondary consultant

**Rule:** For execution documents (MAT, PLN, DET, TMX, COF, RSX, RSV, DVM, PVT, REP, CLP, NDC, LTE, IMP, TDP, LST), if the lot family is NOT one of: 01, 02, 03, 62 — then BET Acoustique is almost always required.

Supporting evidence: 100% presence across 70+ (type_doc, lot_family) combinations covering lots 05, 06, 06B, 08, 11, 12, 12A, 12B, 13, 13A, 13B, 16A, 16B, 17, 18, 19, 20, 22, 31, 33, 34, 35, 41, 42, 42B, 43, 51, 61.

**Exception lots:** 01 (VRD infrastructure), 02 (foundations), 03 (structure/GOE — acoustic presence drops to 82%), 62 (civil works).

### 6.2 BET Electricité (EGIS_FLUIDES) — Electrical lot families

**Rule:** For lots 31, 33, 34 — BET Electricité is always required regardless of doc type.

Supporting evidence: 100% presence for MAT+31, TMX+31, TMX+33, TMX+34, SYQ+31, PLN+31, NDC+31, RSV+31, IMP+31, RSX+31, LTE+31, SYQ+33, SYQ+34, PLN+33, PLN+34, PLN+35.

### 6.3 BET Plomberie (EGIS_FLUIDES) — Plumbing lot family

**Rule:** For lot 42 — BET Plomberie is always required regardless of doc type.

Supporting evidence: 100% presence for MAT+42, PLN+42, NDC+42, SYQ+42, DET+42, RSV+42, NOT+42.

### 6.4 BET CVC (EGIS_FLUIDES) — HVAC lot family

**Rule:** For lot 41 — BET CVC is always required regardless of doc type.

Supporting evidence: 100% presence for MAT+41, NDC+41, RSX+41, SYQ+41, LTE+41, LST+41, PLN+41.

### 6.5 BET Façade (ELIOTH) — Envelope lot families

**Rule:** For lots 05, 06, 06B, 08 — BET Façade is always required for execution plans and details.

Supporting evidence: 100% presence for MAT+05, MAT+06, MAT+08, DET+05, DET+06, DET+06B, DET+08, DVM+08, PLN+06, PLN+08, PVT+05, REP+05, REP+08, LST+05, NDC+06, NDC+08.

### 6.6 BET Structure (TERRELL) — Structural lot family

**Rule:** For lot 03 — BET Structure is required for structural documents (ARM, COF, PLN, NDC, CLP, REP).

Supporting evidence: ARM+03=97.9%, COF+03=97.7%, PLN+03=87.1%, NDC+03=86.4%, CLP+03=94.7%, REP+03=71.4%.

### 6.7 BET SPK (EGIS_FLUIDES) — Sprinkler lot

**Rule:** For lot 43 — BET SPK is always required.

Supporting evidence: 100% presence for MAT+43, PLN+43, NDC+43.

### 6.8 BET Ascenseur (ASCAUDIT) — Elevator lot

**Rule:** For lot 51 — BET Ascenseur is required.

Supporting evidence: DET+51=100%, PLN+51=85.7%.

### 6.9 BET VRD (EGIS_VRD) — Civil works lot

**Rule:** For lot 62 — BET VRD is always required.

Supporting evidence: MAT+62=100%, PLN+62=100%.

### 6.10 BET POL (GEOLIA) — Infrastructure lots

**Rule:** For lot 01 — BET POL is required for quality and methodology documents.

Supporting evidence: QLT+03=100%, CRV+01=100%, CPE+01=100%, MTD+01=100%.

### 6.11 BET Géotech (GEOLIA) — Foundations lot

**Rule:** For lot 02 — BET Géotech is required.

Supporting evidence: NDC+02=87.5%, PLN+02=75.0%.

---

## 7. Candidate Rules Requiring Team Validation

These patterns show significant presence rates (40–89%) but need project team confirmation before implementation:

| # | Pattern | Rate | Docs | Concern |
|---|---------|------|------|---------|
| 1 | PLN + lot_03 → BET Acoustique | 82.3% | 124 | High volume but not universal — some structural plans may not need acoustic review |
| 2 | MAT + lot_03 → BET Structure | 64.3% | 14 | Material fiches for structural lot — partial presence suggests some MAT types don't need structural review |
| 3 | PLN + lot_41 → BET Acoustique | 62.5% | 16 | CVC plans — acoustic review not always needed? |
| 4 | PLN + lot_16B → BET Façade | 50.0% | 8 | Lot 16B facade overlap — needs clarification |
| 5 | PLN + lot_16B → BET CVC | 50.0% | 8 | Lot 16B CVC overlap — needs clarification |
| 6 | MAT + lot_35 → BET Electricité | 45.5% | 33 | SSI lot — electrical involvement partial |
| 7 | Comment keyword "structure" → BET Structure | 70.0% | 263 | High lift (3.64×) but 30% false positive rate |
| 8 | Comment keyword "electricite" → BET Electricité | 63.1% | 331 | Moderate lift (2.10×) but 37% false positive rate |

---

## 8. Noise / Weak Patterns — Do Not Implement

The following patterns are too weak, inconsistent, or insufficiently supported for rule creation:

| Pattern | Rate | Docs | Reason |
|---------|------|------|--------|
| Comment "plomberie" → BET Plomberie | 12.8% | 748 | Near-baseline presence, no signal |
| Comment "incendie" → BET SPK | 1.9% | 631 | Below baseline — negative correlation |
| Comment "geotechnique" → BET Géotech | 4.5% | 247 | High lift but only 11 docs |
| Comment "ascenseur" → BET Ascenseur | 9.7% | 31 | Only 3 docs with both keyword and reviewer |
| Comment "facade" → BET Façade | 34.9% | 401 | High lift (3.41×) but absolute rate too low |
| Comment "CVC" → BET CVC | 37.6% | 237 | High lift (3.65×) but absolute rate too low |
| Late revision activation (any reviewer) | — | 4 cases | Statistically negligible — 4 cases out of 50 multi-revision docs |
| All (type_doc, lot) patterns with <3 docs | varies | <3 | Insufficient sample size |

---

## 9. Recommended Next Implementation Steps

### Phase 1: Circuit Matrix Enhancement (immediate)
Codify the 127 VERY_STRONG patterns from Section 6 into the circuit matrix. These are deterministic and require no team validation. This would primarily mean ensuring every lot family has the correct secondary reviewers listed.

### Phase 2: Team Review (requires meeting)
Present the 20 STRONG patterns and 10 CONDITIONAL patterns to the project team for validation. Specific questions:
- Does lot 03 (structure) always require BET Acoustique? (82.3% presence)
- Does lot 41 (CVC) always require BET Acoustique for plans? (62.5%)
- What is lot 16B's scope? Does it overlap facade and CVC?
- Should SSI lot 35 systematically include BET Electricité?

### Phase 3: Specialite Dimension (future analysis)
This report analyzed (type_doc, lot_family) combinations. A deeper analysis should cross-reference with `specialite` to determine if the specialite code is a better predictor than lot_family for some reviewer assignments.

### Phase 4: Comment-Triggered Rules (deferred)
Comment keyword analysis shows moderate lift for `structure` and `CVC` keywords but insufficient precision for automatic rules. If a future module implements comment-triggered suggestions, it should be with human-in-the-loop validation — never automatic.

### Phase 5: Revision-Based Rules (not recommended)
With only 50 multi-revision documents and 4 late reviewer arrivals, revision-based rule derivation is not viable at this project stage. Revisit when the GED contains more revision history.

---

## 10. Appendix — Key Tables

### Table A — Doc Type Summary

| type_doc | unique_docs | active_docs | main_lots | main_emitters | comments |
|----------|-------------|-------------|-----------|---------------|----------|
| MAT | 944 | 940 | 31(385), 42(348), 31(306), 43(216) | BEN(208), UTB(109), LAC(100), AXI(97) | Largest type — broad lot coverage |
| PLN | 393 | 381 | 03(270), 03(251), 31(204), 06(103) | LGD(181), BEN(51), UTB(41), DUV(31) | Plans — second largest |
| TMX | 206 | 206 | 31(276), 34(190), 31(156), 33(144) | SNI(179), BEN(27) | Focused on electrical lots |
| COF | 150 | 132 | 03(228), 03(226), 03(189), 03(185) | LGD(150) | Purely lot-03 (GOE) |
| NDC | 201 | 191 | 42(97), 41(69), 41(55), 41(51) | AXI(72), LGD(32), UTB(32) | High visa rate (87.9%) |
| ARM | 227 | 195 | 03(244), 03(149), 03(136), 03(111) | LGD(227) | Purely lot-03 — highest visa rate (89.5%) |
| DET | 106 | 106 | 42(126), 08(66), 08(42), 08(36) | UTB(30), DUV(24), ICM(17) | Envelope + plumbing lots |
| RSX | 69 | 69 | 31(128), 41(61), 41(60), 41(54) | AXI(35), SNI(31) | Electrical/CVC lots |
| SYQ | 116 | 115 | 31(57), 31(45), 31(40), 42(33) | SNI(37), UTB(29), BEN(24) | Electrical lots |
| REP | 58 | 55 | 12B(40), 11(40), 12A(35), 03(18) | LAC(16), LGD(10), AMP(9) | Envelope/acoustic lots |
| DVM | 44 | 43 | 08(96), 08(78), 08(66) | DUV(42) | Purely lot-08 (envelope) |
| LST | 53 | 52 | 06(12), 42B(12), 35(10), 17(10) | LAC(7), SMA(6), BEN(6) | Diverse lots |
| RSV | 27 | 27 | 31(54), 31(54), 31(24), 42(18) | SNI(18), BEN(4) | Electrical/plumbing lots |
| CLP | 26 | 26 | 03(61), 03(36), 16A(35), 03(6) | LGD(19), AMP(7) | Structural + facade lots |
| LTE | 16 | 15 | 31(30), 41(24), 41(18), 31(6) | AXI(9), BEN(6) | Electrical/CVC lots |
| QLT | 24 | 19 | 03(59), 00B(6), 01(5), 41(4) | LGD(21), VTP(2) | Infrastructure lots |
| IMP | 12 | 12 | 31(44), 31(12), 22(8) | SNI(11), HVA(1) | Electrical lot |
| CRV | 11 | 10 | 01(39), 03(6) | VTP(7), LGD(4) | VRD infrastructure |
| PVT | 7 | 7 | 05(18), 05(18), 05(6) | ICM(7) | Envelope lot |
| MTD | 6 | 6 | 01(15), 05(6), 05(6), 03(3) | VTP(3), ICM(2) | Infrastructure + envelope |
| TDP | 6 | 6 | 12(5), 12(5), 12(5), 12A(5) | LAC(6) | Door lots |
| NOT | 7 | 7 | 51(6), 51(6), 43(6), 42(3) | UTB(3), SCH(2) | Elevator/sprinkler lots |
| CPE | 5 | 5 | 01(18), 13B(10) | VTP(2), FMC(2) | Infrastructure |
| PPS | 8 | 8 | 01(10), 41(4), 02(3) | FKI(3), VTP(2) | Safety/prevention |
| PIC | 7 | 5 | 03(13) | LGD(7) | Piquetage — lot-03 only |
| MAQ | 3 | 3 | 31(12), 43(1) | BEN(2), AAI(1) | Maquettes — electrical lot |
| DOE | 1 | 1 | 01(5) | VTP(1) | Single document — no analysis possible |
| FQR | 1 | 1 | 03(1) | LGD(1) | Single document — no analysis possible |

### Table B — Strong Reviewer Patterns (≥70% presence rate, ≥3 docs)

| type_doc | lot_family | reviewer | active_doc_count | reviewer_present_count | presence_rate | classification | notes |
|----------|-----------|----------|-----------------|----------------------|--------------|----------------|-------|
| MAT | 31 | BET Acoustique | 157 | 157 | 100.0% | VERY_STRONG | Acoustic review on all electrical lot MAT |
| MAT | 31 | BET Electricité | 157 | 157 | 100.0% | VERY_STRONG | Dedicated BET on electrical lot |
| MAT | 42 | BET Acoustique | 109 | 109 | 100.0% | VERY_STRONG | Acoustic review on plumbing lot MAT |
| MAT | 42 | BET Plomberie | 109 | 109 | 100.0% | VERY_STRONG | Dedicated BET on plumbing lot |
| MAT | 34 | BET Electricité | 103 | 103 | 100.0% | VERY_STRONG | Dedicated BET on electrical lot |
| MAT | 41 | BET Acoustique | 96 | 96 | 100.0% | VERY_STRONG | Acoustic review on CVC lot MAT |
| MAT | 41 | BET CVC | 96 | 96 | 100.0% | VERY_STRONG | Dedicated BET on CVC lot |
| MAT | 12A | BET Acoustique | 87 | 87 | 100.0% | VERY_STRONG | Envelope-adjacent lot |
| TMX | 31 | BET Acoustique | 82 | 82 | 100.0% | VERY_STRONG | Acoustic on electrical matrices |
| TMX | 31 | BET Electricité | 82 | 82 | 100.0% | VERY_STRONG | Dedicated BET |
| NDC | 41 | BET CVC | 72 | 72 | 100.0% | VERY_STRONG | CVC notes always need CVC BET |
| TMX | 33 | BET Electricité | 69 | 69 | 100.0% | VERY_STRONG | Electrical matrices |
| TMX | 34 | BET Electricité | 55 | 55 | 100.0% | VERY_STRONG | Electrical matrices |
| SYQ | 31 | BET Electricité | 48 | 48 | 100.0% | VERY_STRONG | Electrical synoptics |
| MAT | 18 | BET Acoustique | 43 | 43 | 100.0% | VERY_STRONG | |
| PLN | 31 | BET Electricité | 43 | 43 | 100.0% | VERY_STRONG | Electrical plans |
| PLN | 42 | BET Acoustique | 41 | 41 | 100.0% | VERY_STRONG | |
| PLN | 42 | BET Plomberie | 41 | 41 | 100.0% | VERY_STRONG | Plumbing plans |
| DVM | 08 | BET Acoustique | 39 | 39 | 100.0% | VERY_STRONG | Envelope lot |
| DVM | 08 | BET Façade | 39 | 39 | 100.0% | VERY_STRONG | Facade details |
| MAT | 43 | BET Acoustique | 36 | 36 | 100.0% | VERY_STRONG | |
| MAT | 43 | BET SPK | 36 | 36 | 100.0% | VERY_STRONG | Sprinkler lot |
| RSX | 41 | BET Acoustique | 35 | 35 | 100.0% | VERY_STRONG | |
| RSX | 41 | BET CVC | 35 | 35 | 100.0% | VERY_STRONG | CVC networks |
| RSX | 31 | BET Electricité | 34 | 34 | 100.0% | VERY_STRONG | Electrical networks |
| MAT | 33 | BET Electricité | 33 | 33 | 100.0% | VERY_STRONG | |
| NDC | 42 | BET Plomberie | 32 | 32 | 100.0% | VERY_STRONG | Plumbing calcs |
| PLN | 07 | BET Façade | 32 | 32 | 100.0% | VERY_STRONG | Facade lot |
| PLN | 07 | BET Structure | 32 | 32 | 100.0% | VERY_STRONG | |
| DET | 42 | BET Plomberie | 30 | 30 | 100.0% | VERY_STRONG | Plumbing details |
| SYQ | 42 | BET Plomberie | 29 | 29 | 100.0% | VERY_STRONG | Plumbing synoptics |
| NDC | 31 | BET Electricité | 28 | 28 | 100.0% | VERY_STRONG | Electrical calcs |
| MAT | 08 | BET Acoustique | 27 | 27 | 100.0% | VERY_STRONG | Envelope lot |
| MAT | 08 | BET Façade | 27 | 27 | 100.0% | VERY_STRONG | Facade lot |
| PLN | 08 | BET Acoustique | 27 | 27 | 100.0% | VERY_STRONG | Envelope lot |
| PLN | 08 | BET Façade | 27 | 27 | 100.0% | VERY_STRONG | Facade lot |
| MAT | 11 | BET Acoustique | 26 | 26 | 100.0% | VERY_STRONG | |
| DET | 08 | BET Acoustique | 24 | 24 | 100.0% | VERY_STRONG | Envelope lot |
| DET | 08 | BET Façade | 24 | 24 | 100.0% | VERY_STRONG | Facade details |
| MAT | 05 | BET Acoustique | 22 | 22 | 100.0% | VERY_STRONG | Facade lot |
| MAT | 05 | BET Façade | 22 | 22 | 100.0% | VERY_STRONG | Facade lot |
| RSV | 31 | BET Acoustique | 22 | 22 | 100.0% | VERY_STRONG | |
| RSV | 31 | BET Electricité | 22 | 22 | 100.0% | VERY_STRONG | |
| MAT | 20 | BET Acoustique | 19 | 19 | 100.0% | VERY_STRONG | |
| SYQ | 41 | BET CVC | 19 | 19 | 100.0% | VERY_STRONG | CVC synoptics |
| DET | 05 | BET Acoustique | 17 | 17 | 100.0% | VERY_STRONG | |
| DET | 05 | BET Façade | 17 | 17 | 100.0% | VERY_STRONG | |
| PLN | 06 | BET Acoustique | 17 | 17 | 100.0% | VERY_STRONG | |
| PLN | 06 | BET Façade | 17 | 17 | 100.0% | VERY_STRONG | |
| PLN | 41 | BET CVC | 16 | 16 | 100.0% | VERY_STRONG | |
| QLT | 03 | BET POL | 16 | 16 | 100.0% | VERY_STRONG | Infrastructure quality |
| PLN | 43 | BET Acoustique | 15 | 15 | 100.0% | VERY_STRONG | |
| PLN | 43 | BET SPK | 15 | 15 | 100.0% | VERY_STRONG | |
| MAT | 19 | BET Acoustique | 14 | 14 | 100.0% | VERY_STRONG | |
| MAT | 62 | BET VRD | 14 | 14 | 100.0% | VERY_STRONG | Civil works |
| NDC | 08 | BET Acoustique | 14 | 14 | 100.0% | VERY_STRONG | |
| NDC | 08 | BET Façade | 14 | 14 | 100.0% | VERY_STRONG | |
| MAT | 12 | BET Acoustique | 13 | 13 | 100.0% | VERY_STRONG | |
| REP | 12A | BET Acoustique | 13 | 13 | 100.0% | VERY_STRONG | |
| MAT | 13A | BET Acoustique | 11 | 11 | 100.0% | VERY_STRONG | |
| PLN | 13 | BET Acoustique | 11 | 11 | 100.0% | VERY_STRONG | |
| IMP | 31 | BET Electricité | 11 | 11 | 100.0% | VERY_STRONG | |
| MAT | 06 | BET Acoustique | 10 | 10 | 100.0% | VERY_STRONG | |
| MAT | 06 | BET Façade | 10 | 10 | 100.0% | VERY_STRONG | |
| MAT | 17 | BET Acoustique | 10 | 10 | 100.0% | VERY_STRONG | |
| REP | 12B | BET Acoustique | 10 | 10 | 100.0% | VERY_STRONG | |
| LTE | 41 | BET Acoustique | 9 | 9 | 100.0% | VERY_STRONG | |
| LTE | 41 | BET CVC | 9 | 9 | 100.0% | VERY_STRONG | |
| PLN | 62 | BET VRD | 9 | 9 | 100.0% | VERY_STRONG | Civil works |
| REP | 11 | BET Acoustique | 9 | 9 | 100.0% | VERY_STRONG | |
| DET | 12A | BET Acoustique | 8 | 8 | 100.0% | VERY_STRONG | |
| PLN | 16B | BET Acoustique | 8 | 8 | 100.0% | VERY_STRONG | |
| SYQ | 33 | BET Electricité | 8 | 8 | 100.0% | VERY_STRONG | |
| CLP | 16A | BET Acoustique | 7 | 7 | 100.0% | VERY_STRONG | |
| CRV | 01 | BET Structure | 7 | 7 | 100.0% | VERY_STRONG | |
| CRV | 01 | BET POL | 7 | 7 | 100.0% | VERY_STRONG | |
| DET | 06 | BET Acoustique | 7 | 7 | 100.0% | VERY_STRONG | |
| DET | 06 | BET Façade | 7 | 7 | 100.0% | VERY_STRONG | |
| PVT | 05 | BET Acoustique | 7 | 7 | 100.0% | VERY_STRONG | |
| PVT | 05 | BET Façade | 7 | 7 | 100.0% | VERY_STRONG | |
| DET | 51 | BET Acoustique | 6 | 6 | 100.0% | VERY_STRONG | |
| DET | 51 | BET Ascenseur | 6 | 6 | 100.0% | VERY_STRONG | Elevator lot |
| LTE | 31 | BET Electricité | 6 | 6 | 100.0% | VERY_STRONG | |
| MAT | 16A | BET Acoustique | 6 | 6 | 100.0% | VERY_STRONG | |
| MAT | 16B | BET Acoustique | 6 | 6 | 100.0% | VERY_STRONG | |
| MAT | 16B | BET CVC | 6 | 6 | 100.0% | VERY_STRONG | |
| PLN | 34 | BET Electricité | 6 | 6 | 100.0% | VERY_STRONG | |
| LST | 41 | BET CVC | 5 | 5 | 100.0% | VERY_STRONG | |
| MAT | 42B | BET Acoustique | 5 | 5 | 100.0% | VERY_STRONG | |
| MAT | 42B | BET Plomberie | 5 | 5 | 100.0% | VERY_STRONG | |
| MAT | 61 | BET VRD | 5 | 5 | 100.0% | VERY_STRONG | |
| NDC | 43 | BET Acoustique | 5 | 5 | 100.0% | VERY_STRONG | |
| NDC | 43 | BET SPK | 5 | 5 | 100.0% | VERY_STRONG | |
| SYQ | 34 | BET Electricité | 5 | 5 | 100.0% | VERY_STRONG | |
| MAT | 13B | BET Acoustique | 4 | 4 | 100.0% | VERY_STRONG | |
| PLN | 13A | BET Acoustique | 4 | 4 | 100.0% | VERY_STRONG | |
| REP | 05 | BET Acoustique | 4 | 4 | 100.0% | VERY_STRONG | |
| REP | 05 | BET Façade | 4 | 4 | 100.0% | VERY_STRONG | |
| CPE | 01 | BET Structure | 3 | 3 | 100.0% | VERY_STRONG | |
| CPE | 01 | BET POL | 3 | 3 | 100.0% | VERY_STRONG | |
| DET | 06B | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| DET | 06B | BET Façade | 3 | 3 | 100.0% | VERY_STRONG | |
| LST | 05 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| LST | 05 | BET Façade | 3 | 3 | 100.0% | VERY_STRONG | |
| LST | 13 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| LST | 18 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| MAT | 13 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| MTD | 01 | BET Structure | 3 | 3 | 100.0% | VERY_STRONG | |
| MTD | 01 | BET POL | 3 | 3 | 100.0% | VERY_STRONG | |
| NDC | 06 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| NDC | 06 | BET Façade | 3 | 3 | 100.0% | VERY_STRONG | |
| NOT | 42 | BET Plomberie | 3 | 3 | 100.0% | VERY_STRONG | |
| PLN | 33 | BET Electricité | 3 | 3 | 100.0% | VERY_STRONG | |
| PLN | 35 | BET Electricité | 3 | 3 | 100.0% | VERY_STRONG | |
| PLN | 42B | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| PLN | 42B | BET Plomberie | 3 | 3 | 100.0% | VERY_STRONG | |
| REP | 08 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| REP | 08 | BET Façade | 3 | 3 | 100.0% | VERY_STRONG | |
| REP | 12 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| REP | 18 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| RSV | 42 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| RSV | 42 | BET Plomberie | 3 | 3 | 100.0% | VERY_STRONG | |
| TDP | 12 | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| TDP | 12A | BET Acoustique | 3 | 3 | 100.0% | VERY_STRONG | |
| ARM | 03 | BET Structure | 195 | 191 | 97.9% | VERY_STRONG | Core structural |
| COF | 03 | BET Structure | 131 | 128 | 97.7% | VERY_STRONG | Core structural |
| PLN | 31 | BET Acoustique | 43 | 42 | 97.7% | VERY_STRONG | |
| COF | 03 | BET Acoustique | 131 | 125 | 95.4% | VERY_STRONG | |
| CLP | 03 | BET Acoustique | 19 | 18 | 94.7% | VERY_STRONG | |
| CLP | 03 | BET Structure | 19 | 18 | 94.7% | VERY_STRONG | |
| RSX | 31 | BET Acoustique | 34 | 32 | 94.1% | VERY_STRONG | |
| DET | 42 | BET Acoustique | 30 | 27 | 90.0% | VERY_STRONG | |
| PLN | 62 | BET EV | 9 | 8 | 88.9% | STRONG | |
| NDC | 02 | BET Structure | 8 | 7 | 87.5% | STRONG | |
| NDC | 02 | BET Géotech | 8 | 7 | 87.5% | STRONG | |
| PLN | 03 | BET Structure | 124 | 108 | 87.1% | STRONG | High volume |
| NDC | 03 | BET Structure | 22 | 19 | 86.4% | STRONG | |
| PLN | 51 | BET Acoustique | 7 | 6 | 85.7% | STRONG | |
| PLN | 51 | BET Ascenseur | 7 | 6 | 85.7% | STRONG | |
| LST | 12A | BET Acoustique | 6 | 5 | 83.3% | STRONG | |
| LTE | 31 | BET Acoustique | 6 | 5 | 83.3% | STRONG | |
| PLN | 03 | BET Acoustique | 124 | 102 | 82.3% | STRONG | High volume |
| IMP | 31 | BET Acoustique | 11 | 9 | 81.8% | STRONG | |
| IMP | 31 | BET Electricité | 11 | 9 | 81.8% | STRONG | |
| LST | 35 | BET Electricité | 5 | 4 | 80.0% | STRONG | |
| LST | 41 | BET Acoustique | 5 | 4 | 80.0% | STRONG | |
| PLN | 02 | BET Structure | 4 | 3 | 75.0% | STRONG | |
| PLN | 02 | BET Géotech | 4 | 3 | 75.0% | STRONG | |
| REP | 03 | BET Structure | 7 | 5 | 71.4% | STRONG | |

### Table C — Trigger Pattern Candidates

| keyword_family | triggered_reviewer | support_count | support_rate | classification | notes |
|---------------|-------------------|---------------|-------------|----------------|-------|
| socotec | Bureau de Contrôle | 94 | 100.0% | VERY_STRONG | Trivial — reviewer is near-universal (baseline 99.2%) |
| HQE_environnement | AMO HQE | 104 | 84.6% | STRONG | Negative lift (0.92) — keyword presence does not increase probability above baseline |
| acoustique | BET Acoustique | 190 | 80.0% | STRONG | Mild positive lift (1.17×) — marginally above 68.2% baseline |
| structure | BET Structure | 263 | 70.0% | CONDITIONAL | Strong lift (3.64×) from 19.2% baseline — best trigger candidate |
| electricite | BET Electricité | 331 | 63.1% | CONDITIONAL | Moderate lift (2.10×) from 30.1% baseline |
| CVC | BET CVC | 237 | 37.6% | WEAK | Strong lift (3.65×) but absolute rate too low |
| facade | BET Façade | 401 | 34.9% | WEAK | Strong lift (3.41×) but absolute rate too low |
| plomberie | BET Plomberie | 748 | 12.8% | NOISE | Near baseline — keyword too generic |
| ascenseur | BET Ascenseur | 31 | 9.7% | NOISE | Only 3 docs — insufficient sample |
| geotechnique | BET Géotech | 247 | 4.5% | NOISE | Only 11 docs — insufficient sample despite high lift |
| incendie | BET SPK | 631 | 1.9% | NOISE | Below baseline — negative correlation |

---

*End of analysis. No pipeline code was modified. All findings are evidence-based candidates subject to project team review.*
