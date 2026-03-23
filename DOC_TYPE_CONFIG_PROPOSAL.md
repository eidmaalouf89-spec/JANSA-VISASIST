# Doc Type Configuration Proposal

**Date**: 2026-03-23
**Status**: Proposal — analysis only, no implementation
**Dataset**: Active dataset post-legacy exclusion (2,396 docs, 11,760 rows, 27 types)

---

## 1. Canonical Doc Type Families

Grouping the 27 observed doc types into 7 functional families based on their role in the construction execution workflow.

### Family A — Graphical Documents (Plans & Drawings)

Core execution documents requiring full visa circuit review.

| Code | Name | Docs | Avg Reviewers | Reject Rate |
|---|---|---|---|---|
| PLN | Plans d'exécution | 349 | 5.7 | 26.1% |
| DET | Détails d'exécution | 94 | 5.7 | 5.2% |
| ARM | Plans de ferraillage | 189 | 3.1 | 5.5% |
| COF | Plans de coffrage | 126 | 6.1 | 5.6% |
| CLP | Calpinages | 19 | 5.6 | 34.7% |
| IMP | Plans d'implantation | 8 | 6.2 | 0.0% |
| RSV | Plans de réservations | 26 | 6.0 | 32.1% |

**Characteristics**: High reviewer count (5–6 per doc), standard blocking visa workflow. PLN, CLP, and RSV show elevated reject rates — they produce more conflict.

### Family B — Technical Notes & Calculations

Specialist review documents focused on compliance verification.

| Code | Name | Docs | Avg Reviewers | Reject Rate |
|---|---|---|---|---|
| NDC | Notes de calcul | 169 | 3.5 | 8.8% |
| MAT | Fiches techniques matériaux | 824 | 5.2 | 13.7% |
| RSX | Réseaux | 68 | 6.0 | 2.1% |
| TMX | Tableaux / matrices | 196 | 4.7 | 13.0% |

**Characteristics**: MAT is the single largest type (34% of all docs). NDC has the highest READY_TO_ISSUE rate (21%) — reviewers act fast. TMX and RSX are nearly always in WAITING_RESPONSES (96%).

### Family C — Synthesis & Coordination

Cross-disciplinary documents for inter-lot coordination.

| Code | Name | Docs | Avg Reviewers | Reject Rate |
|---|---|---|---|---|
| SYQ | Synthèses techniques | 110 | 3.2 | 18.2% |
| TDP | Tableaux de pose | 6 | 5.0 | 0.0% |

**Characteristics**: SYQ has the highest FAST_REJECT rate (7.3%) and significant CONFLICT (5.5%). These documents are coordination-critical. TDP is low volume but follows the same coordination workflow.

### Family D — Administrative & Quality

Process documents, not directly part of the technical visa circuit.

| Code | Name | Docs | Avg Reviewers | Reject Rate |
|---|---|---|---|---|
| QLT | Fiches qualité | 15 | 3.2 | 9.5% |
| FQR | Fiche qualité réception | 1 | 1.0 | N/A |
| PPS | Plan particulier sécurité | 6 | 2.8 | 0.0% |
| MTD | Méthodologie | 6 | 5.0 | 0.0% |
| PVT | Procès-verbaux | 7 | 6.0 | 0.0% |

**Characteristics**: FQR has exactly 1 doc with 1 row, NOT_ASSIGNED, 100% NOT_RESPONDED — it is not part of any visa workflow. PPS has very few reviewers and 35% HM rate. QLT is MOEX-driven (60% READY_TO_ISSUE = blocker is GEMO_MOEX). MTD has 50% HM rate.

### Family E — Correspondence & Reporting

Non-blocking communication documents.

| Code | Name | Docs | Avg Reviewers | Reject Rate |
|---|---|---|---|---|
| LTE | Lettres | 14 | 5.8 | 6.5% |
| NOT | Notes | 6 | 4.5 | 0.0% |
| REP | Réponses / observations | 50 | 4.9 | 25.5% |
| CPE | Comptes-rendus | 3 | 5.3 | 0.0% |
| LST | Listes | 49 | 4.2 | 3.4% |

**Characteristics**: LTE, CRV, and CPE have high SYNTHESIS_ISSUED rates (57–86%), meaning they complete quickly. REP has high reject rate (25.5%) but is a response document — rejects are expected (they ARE observations). LST has high NOT_STARTED (43%).

### Family F — Diverse & Reference Documents

Supplementary documents with various purposes.

| Code | Name | Docs | Avg Reviewers | Reject Rate |
|---|---|---|---|---|
| DVM | Documents divers / mémoires | 43 | 5.9 | 0.0% |
| CRV | Courbes / nivellement | 7 | 5.1 | 40.0% |
| MAQ | Maquettes | 2 | 6.0 | 40.0% |
| PIC | Pièces / photos | 3 | 2.3 | 0.0% |

**Characteristics**: DVM is 81% NOT_STARTED with 43% HM — reviewers largely ignore it. CRV has 39% HM and high reject rate but 86% already SYNTHESIS_ISSUED — likely legacy-era completions. PIC has only 3 docs and 2.3 reviewers, 57% approved. MAQ is very low volume.

### Family G — Rare / Singleton Types

Types with ≤3 docs that lack statistical significance.

| Code | Name | Docs | Notes |
|---|---|---|---|
| CPE | Comptes-rendus | 3 | 69% NOT_RESPONDED |
| PIC | Pièces / photos | 3 | 57% APPROVED |
| MAQ | Maquettes | 2 | High reject rate but n=2 |
| FQR | Fiche qualité réception | 1 | Single doc, no workflow |

---

## 2. Type-to-Family Classification Table

| Code | Family | Family Name |
|---|---|---|
| PLN | A | Graphical Documents |
| DET | A | Graphical Documents |
| ARM | A | Graphical Documents |
| COF | A | Graphical Documents |
| CLP | A | Graphical Documents |
| IMP | A | Graphical Documents |
| RSV | A | Graphical Documents |
| NDC | B | Technical Notes |
| MAT | B | Technical Notes |
| RSX | B | Technical Notes |
| TMX | B | Technical Notes |
| SYQ | C | Synthesis & Coordination |
| TDP | C | Synthesis & Coordination |
| QLT | D | Administrative & Quality |
| FQR | D | Administrative & Quality |
| PPS | D | Administrative & Quality |
| MTD | D | Administrative & Quality |
| PVT | D | Administrative & Quality |
| LTE | E | Correspondence |
| NOT | E | Correspondence |
| REP | E | Correspondence |
| CPE | E | Correspondence |
| LST | E | Correspondence |
| DVM | F | Diverse & Reference |
| CRV | F | Diverse & Reference |
| MAQ | F | Diverse & Reference |
| PIC | F | Diverse & Reference |

---

## 3. Workflow Classification

### 3a. Standard Visa Workflow (BLOCKING)

These types follow the full visa diffusion process: document enters → reviewers assigned → responses collected → synthesis issued. A missing or rejected response blocks progression.

| Code | Family | Rationale |
|---|---|---|
| PLN | A | Core execution plans, high reject rate, 5.7 reviewers |
| DET | A | Execution details, full circuit, 5.7 reviewers |
| ARM | A | Structural drawings, 94% REQUIRED_VISA — highest of all types |
| COF | A | Structural drawings, 6.1 reviewers, full circuit |
| CLP | A | Calpinage coordination, high reject rate |
| IMP | A | Implantation, 6.2 reviewers |
| RSV | A | Reservations, high reject rate (32%) — coordination-critical |
| NDC | B | Calculations, 90% REQUIRED_VISA, fast review cycle |
| MAT | B | Largest type, standard circuit, 5.2 reviewers |
| RSX | B | Network plans, 6.0 reviewers, low reject |
| TMX | B | Matrices, 96% WAITING_RESPONSES |
| SYQ | C | Synthesis, 91% REQUIRED_VISA, high conflict rate |
| DET | A | Full review circuit |

**Count**: 12 types, covering 2,179 docs (91.0% of active dataset).

### 3b. Standard Visa Workflow (NON-BLOCKING)

These types go through the visa circuit but should not block overall project progression. They either complete quickly, have low priority, or represent secondary documentation.

| Code | Family | Rationale |
|---|---|---|
| TDP | C | Low volume, coordination doc, 0% reject |
| LTE | E | Letters, 57% already SYNTHESIS_ISSUED, informational purpose |
| NOT | E | Notes, 33% HM rate, secondary importance |
| REP | E | Response docs — rejects expected by design |
| LST | E | Lists, 43% NOT_STARTED, low urgency |
| DVM | F | 81% NOT_STARTED, 43% HM — reviewers ignore; should not block |
| CRV | F | 86% SYNTHESIS_ISSUED, mostly completed |
| PVT | D | Process-verbal, 0% reject, completes cleanly |

**Count**: 8 types, covering 183 docs (7.6% of active dataset).

### 3c. Special / Non-Standard Workflow

These types should not enter the standard NM7 priority queue, or require modified handling.

| Code | Family | Behavior | Rationale |
|---|---|---|---|
| FQR | D | **EXCLUDE** from NM7 | 1 doc, 1 row, NOT_ASSIGNED, 100% NOT_RESPONDED. Not part of any visa circuit. Quality reception document — standalone process. |
| PPS | D | **REDUCED** workflow | 35% HM, only 2.8 reviewers, MOEX-driven. Safety plan — follows its own regulatory process, not technical visa. |
| MTD | D | **REDUCED** workflow | 50% HM rate, half are SYNTHESIS_ISSUED. Methodology docs — informational, not blocking. |
| QLT | D | **MOEX-ONLY** workflow | 60% READY_TO_ISSUE with blocker=GEMO_MOEX. Quality docs driven by MOEX validation, not multi-reviewer visa. |
| CPE | E | **EXCLUDE** from NM7 | 3 docs, 69% NOT_RESPONDED. Meeting minutes — should not be in visa workflow. |
| PIC | F | **EXCLUDE** from NM7 | 3 docs, photos/attachments. Not a document requiring visa. |
| MAQ | F | **MONITOR** only | 2 docs, maquettes. Too few to classify reliably. Keep in queue but do not prioritize. |

**Count**: 7 types, covering 34 docs (1.4% of active dataset).

---

## 4. Behavioral Attributes

### Proposed `doc_type_config` Schema

```python
DOC_TYPE_CONFIG = {
    'PLN': {
        'family': 'A',
        'workflow': 'STANDARD_VISA',
        'blocking': True,
        'priority_weight': 1.2,        # elevated — execution plans
        'expected_reviewers': ['AMO HQE', 'ARCHITECTE', 'Bureau de Contrôle',
                               'Maître d\'Oeuvre EXE', '<specialist BET>'],
        'reject_sensitivity': 'HIGH',   # 26% reject rate
    },
    # ...
}
```

### Full Attribute Table

| Code | Family | Workflow | Blocking | Priority Weight | Reject Sensitivity |
|---|---|---|---|---|---|
| **PLN** | A | STANDARD_VISA | Yes | **1.2** | HIGH |
| **DET** | A | STANDARD_VISA | Yes | 1.0 | NORMAL |
| **ARM** | A | STANDARD_VISA | Yes | **1.3** | NORMAL |
| **COF** | A | STANDARD_VISA | Yes | 1.0 | NORMAL |
| **CLP** | A | STANDARD_VISA | Yes | 1.0 | HIGH |
| **IMP** | A | STANDARD_VISA | Yes | 1.0 | NORMAL |
| **RSV** | A | STANDARD_VISA | Yes | 1.0 | HIGH |
| **NDC** | B | STANDARD_VISA | Yes | **1.3** | NORMAL |
| **MAT** | B | STANDARD_VISA | Yes | 1.0 | NORMAL |
| **RSX** | B | STANDARD_VISA | Yes | 1.0 | LOW |
| **TMX** | B | STANDARD_VISA | Yes | 1.0 | NORMAL |
| **SYQ** | C | STANDARD_VISA | Yes | **1.2** | HIGH |
| **TDP** | C | STANDARD_VISA | No | 0.8 | LOW |
| **QLT** | D | MOEX_ONLY | Yes | 1.0 | NORMAL |
| **FQR** | D | EXCLUDED | No | 0.0 | N/A |
| **PPS** | D | REDUCED | No | 0.5 | LOW |
| **MTD** | D | REDUCED | No | 0.5 | LOW |
| **PVT** | D | STANDARD_VISA | No | 0.8 | LOW |
| **LTE** | E | STANDARD_VISA | No | 0.6 | LOW |
| **NOT** | E | STANDARD_VISA | No | 0.6 | LOW |
| **REP** | E | STANDARD_VISA | No | 0.8 | EXPECTED |
| **CPE** | E | EXCLUDED | No | 0.0 | N/A |
| **LST** | E | STANDARD_VISA | No | 0.6 | LOW |
| **DVM** | F | STANDARD_VISA | No | 0.4 | LOW |
| **CRV** | F | STANDARD_VISA | No | 0.6 | LOW |
| **MAQ** | F | MONITOR | No | 0.3 | LOW |
| **PIC** | F | EXCLUDED | No | 0.0 | N/A |

### Attribute Definitions

**workflow**:
- `STANDARD_VISA` — Full visa diffusion circuit: assignment → response collection → synthesis
- `MOEX_ONLY` — MOEX/GEMO validates; other reviewers are informational only
- `REDUCED` — Fewer required reviewers; does not need full circuit to progress
- `EXCLUDED` — Not part of NM7 priority queue; filtered out before lifecycle computation
- `MONITOR` — Stays in dataset but with zero priority; tracked for completeness only

**blocking**:
- `True` — A missing or rejected response from this doc type blocks lot-level progression. The doc appears in the priority queue with full scoring.
- `False` — Doc appears in the queue for awareness but does not contribute to lot-level blocking assessment.

**priority_weight**:
Multiplier applied to the NM7 base priority score. Range 0.0–1.5.
- `1.3` — ARM, NDC: structural/calculation documents that gate all downstream work
- `1.2` — PLN, SYQ: execution plans and syntheses that coordinate multiple trades
- `1.0` — Standard weight for most blocking types
- `0.8` — TDP, REP, PVT: secondary documents, still tracked
- `0.6` — LTE, NOT, LST, CRV: correspondence/listings, low urgency
- `0.5` — PPS, MTD: administrative, own process
- `0.4` — DVM: largely ignored by reviewers
- `0.3` — MAQ: monitor only
- `0.0` — FQR, CPE, PIC: excluded from scoring

**reject_sensitivity**:
How the system interprets a REJ response from this doc type.
- `HIGH` — PLN (26%), CLP (35%), RSV (32%), SYQ (18%): rejects are frequent and indicate real coordination issues
- `NORMAL` — Standard treatment; rejects trigger normal escalation
- `LOW` — Rejects are rare and may indicate misclassification
- `EXPECTED` — REP: this type carries observations; "rejects" are normal
- `N/A` — Excluded types

---

## 5. Expected Reviewers (Informational)

This is NOT for assignment logic (that is NM4's job). This is for **anomaly detection**: if a doc of a given type does NOT have these reviewers, it may indicate a configuration gap.

### Universal Core Circuit (present for almost all blocking types)

- AMO HQE
- ARCHITECTE
- Bureau de Contrôle
- Maître d'Oeuvre EXE

### Specialist Reviewers by Type + Lot

| Type | Specialist Reviewers | Applicable Lots |
|---|---|---|
| PLN, DET, MAT | BET Acoustique | 03, 05, 06, 08, 12A, 31, 41, 42, 43 |
| PLN, MAT, TMX, SYQ | BET Electricité | 31, 33, 34, 35 |
| PLN, MAT, NDC, SYQ | BET Plomberie | 42 |
| PLN, MAT, NDC, RSX, SYQ, LTE | BET CVC | 41 |
| PLN, DET, MAT, DVM | BET Façade | 05, 06, 08 |
| ARM, COF, NDC, PLN | BET Structure | 01, 02, 03 |
| PLN, MAT | BET VRD | 62 |
| NDC | BET Géotech | 02 |
| CRV, MTD | BET POL | 01 |
| PLN | BET Ascenseur | 51 (STRONG, not yet VERY_STRONG) |
| PLN | BET EV | 62 (STRONG, not yet VERY_STRONG) |
| MAT | B13 - METALLERIE SERRURERIE | 61, 62 |
| MAT | Sollicitation supplémentaire | 61, 62 |

---

## 6. Implementation Considerations

### Where This Config Would Live

A new module `jansa/adapters/ged/doc_type_config.py` containing a single `DOC_TYPE_CONFIG` dictionary, imported by NM7 to influence:

1. **Priority scoring** — `priority_weight` multiplied into the base score
2. **Queue filtering** — `workflow=EXCLUDED` types removed before lifecycle computation
3. **Blocking assessment** — `blocking=False` types excluded from lot-level blocking signals
4. **Anomaly flagging** — `expected_reviewers` used to flag docs missing expected specialist BETs

### What This Does NOT Change

- NM4 assignment logic (unchanged — already resolved, 0 UNKNOWN_REQUIRED)
- NM5 active dataset logic (unchanged — legacy exclusion is independent)
- Circuit matrix or override rules (unchanged)
- Response normalization (NM3 — unchanged)

### Risks

1. **Priority weight calibration** — the proposed weights are based on qualitative assessment of reject rates and construction workflow importance. They should be validated by the MOEX team before deployment.

2. **EXCLUDED types** — FQR (1 doc), CPE (3 docs), PIC (3 docs) are proposed for exclusion. If new docs of these types arrive and DO require visa, the exclusion must be revisited.

3. **MOEX_ONLY for QLT** — this reclassification assumes QLT docs are driven by MOEX validation. If the team uses QLT for other purposes, this needs adjustment.

4. **Low-volume types** — MAQ (2), PIC (3), CPE (3), FQR (1), MTD (6), PPS (6), TDP (6), CRV (7), PVT (7) all have <10 docs. Their behavioral profiles may shift as more data arrives. The config should be reviewed after the next GED export.

---

*Proposal only. No code changes made. Awaiting validation before implementation.*
