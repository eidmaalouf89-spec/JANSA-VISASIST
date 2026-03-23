"""Project-level override rules for P17&CO Tranche 2.

Source: DOC_TYPE_AND_SECONDARY_CONSULTANT_ANALYSIS.md
Only VERY_STRONG patterns (≥90% presence rate, ≥3 active docs) are included.

Each rule: (type_doc, lot_family, reviewer_role) → assignment_type
where reviewer_role is the GED mission role (e.g. 'BET Acoustique'),
NOT the circuit reviewer key (e.g. 'AVLS').

These rules are checked BEFORE the circuit matrix. They add coverage for
(type_doc, lot_family, reviewer) combinations that the PDF-based circuit
matrix does not explicitly list.

If the circuit matrix already returns a result for a given combination,
the override is NOT applied (circuit matrix takes priority for combinations
it explicitly covers). Overrides only fire when the circuit matrix returns None.
"""

_V = 'REQUIRED_VISA'

# ---------------------------------------------------------------------------
# Override rules: (type_doc, lot_family) → {reviewer_role: assignment_type}
# Only VERY_STRONG (≥90%) patterns from data analysis.
# Sorted by lot_family, then type_doc for readability.
# ---------------------------------------------------------------------------

OVERRIDE_RULES = {
    # ── lot 03 — GOE / CHARPENTE ────────────────────────────────────────
    # ARM+03: BET Structure=97.9% — already in circuit, BET Acoustique NOT
    ('ARM', '03'):   {'BET Acoustique': _V},  # not in circuit for ARM
    # COF+03: BET Structure=97.7%, BET Acoustique=95.4% — acoustique in circuit
    # CLP+03: BET Acoustique=94.7%, BET Structure=94.7%
    ('CLP', '03'):   {'BET Acoustique': _V, 'BET Structure': _V},

    # ── lot 05 — MENUISERIES EXT / BSO ──────────────────────────────────
    ('DET', '05'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('LST', '05'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('MAT', '05'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('PVT', '05'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('REP', '05'):   {'BET Acoustique': _V, 'BET Façade': _V},

    # ── lot 06 — REVETEMENT FACADES ─────────────────────────────────────
    ('DET', '06'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('MAT', '06'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('NDC', '06'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('PLN', '06'):   {'BET Acoustique': _V, 'BET Façade': _V},

    # ── lot 06B — sub-lot (alias to 09 in circuit, but data shows direct) ─
    ('DET', '06B'):  {'BET Acoustique': _V, 'BET Façade': _V},

    # ── lot 07 — BETON PREFA ────────────────────────────────────────────
    ('PLN', '07'):   {'BET Façade': _V, 'BET Structure': _V},

    # ── lot 08 — MURS RIDEAUX ───────────────────────────────────────────
    ('DET', '08'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('DVM', '08'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('MAT', '08'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('NDC', '08'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('PLN', '08'):   {'BET Acoustique': _V, 'BET Façade': _V},
    ('REP', '08'):   {'BET Acoustique': _V, 'BET Façade': _V},

    # ── lot 11 — CLOISONS / DOUBLAGES ──────────────────────────────────
    ('MAT', '11'):   {'BET Acoustique': _V},
    ('REP', '11'):   {'BET Acoustique': _V},

    # ── lot 12 — MENUISERIES INT ────────────────────────────────────────
    ('MAT', '12'):   {'BET Acoustique': _V},
    ('REP', '12'):   {'BET Acoustique': _V},
    ('TDP', '12'):   {'BET Acoustique': _V},

    # ── lot 12A — sub-lot ───────────────────────────────────────────────
    ('DET', '12A'):  {'BET Acoustique': _V},
    ('MAT', '12A'):  {'BET Acoustique': _V},
    ('REP', '12A'):  {'BET Acoustique': _V},
    ('TDP', '12A'):  {'BET Acoustique': _V},

    # ── lot 12B — sub-lot ───────────────────────────────────────────────
    ('REP', '12B'):  {'BET Acoustique': _V},

    # ── lot 13 — METALLERIE / SERRURERIE ────────────────────────────────
    ('LST', '13'):   {'BET Acoustique': _V},
    ('MAT', '13'):   {'BET Acoustique': _V},
    ('PLN', '13'):   {'BET Acoustique': _V},

    # ── lot 13A — sub-lot ───────────────────────────────────────────────
    ('MAT', '13A'):  {'BET Acoustique': _V},
    ('PLN', '13A'):  {'BET Acoustique': _V},

    # ── lot 13B — sub-lot ───────────────────────────────────────────────
    ('MAT', '13B'):  {'BET Acoustique': _V},

    # ── lot 16A — FAUX PLAFONDS ─────────────────────────────────────────
    ('CLP', '16A'):  {'BET Acoustique': _V},
    ('MAT', '16A'):  {'BET Acoustique': _V},

    # ── lot 16B — PLAFONDS RAYONNANTS ───────────────────────────────────
    ('MAT', '16B'):  {'BET Acoustique': _V, 'BET CVC': _V},
    ('PLN', '16B'):  {'BET Acoustique': _V},

    # ── lot 17 — FAUX PLANCHERS ─────────────────────────────────────────
    ('MAT', '17'):   {'BET Acoustique': _V},

    # ── lot 18 — REVETEMENT DURS ────────────────────────────────────────
    ('LST', '18'):   {'BET Acoustique': _V},
    ('MAT', '18'):   {'BET Acoustique': _V},
    ('REP', '18'):   {'BET Acoustique': _V},

    # ── lot 19 — SOLS SOUPLES ───────────────────────────────────────────
    ('MAT', '19'):   {'BET Acoustique': _V},

    # ── lot 20 — PEINTURE ───────────────────────────────────────────────
    ('MAT', '20'):   {'BET Acoustique': _V},

    # ── lot 31 — ELECTRICITE CFO ────────────────────────────────────────
    ('IMP', '31'):   {'BET Electricité': _V},
    ('LTE', '31'):   {'BET Electricité': _V},
    ('MAT', '31'):   {'BET Acoustique': _V, 'BET Electricité': _V},
    ('NDC', '31'):   {'BET Electricité': _V},
    ('PLN', '31'):   {'BET Acoustique': _V, 'BET Electricité': _V},
    ('RSV', '31'):   {'BET Acoustique': _V, 'BET Electricité': _V},
    ('RSX', '31'):   {'BET Acoustique': _V, 'BET Electricité': _V},
    ('SYQ', '31'):   {'BET Electricité': _V},
    ('TMX', '31'):   {'BET Acoustique': _V, 'BET Electricité': _V},

    # ── lot 33 — SSI ───────────────────────────────────────────────────
    ('MAT', '33'):   {'BET Electricité': _V},
    ('SYQ', '33'):   {'BET Electricité': _V},
    ('TMX', '33'):   {'BET Electricité': _V},
    ('PLN', '33'):   {'BET Electricité': _V},

    # ── lot 34 — CFA / CA / SURETE ─────────────────────────────────────
    ('MAT', '34'):   {'BET Electricité': _V},
    ('PLN', '34'):   {'BET Electricité': _V},
    ('SYQ', '34'):   {'BET Electricité': _V},
    ('TMX', '34'):   {'BET Electricité': _V},

    # ── lot 41 — CVC ───────────────────────────────────────────────────
    ('LST', '41'):   {'BET CVC': _V},
    ('LTE', '41'):   {'BET Acoustique': _V, 'BET CVC': _V},
    ('MAT', '41'):   {'BET Acoustique': _V, 'BET CVC': _V},
    ('NDC', '41'):   {'BET CVC': _V},
    ('PLN', '41'):   {'BET CVC': _V},
    ('RSX', '41'):   {'BET Acoustique': _V, 'BET CVC': _V},
    ('SYQ', '41'):   {'BET CVC': _V},

    # ── lot 42 — PLOMBERIE ──────────────────────────────────────────────
    ('DET', '42'):   {'BET Acoustique': _V, 'BET Plomberie': _V},
    ('MAT', '42'):   {'BET Acoustique': _V, 'BET Plomberie': _V},
    ('NDC', '42'):   {'BET Plomberie': _V},
    ('NOT', '42'):   {'BET Plomberie': _V},
    ('PLN', '42'):   {'BET Acoustique': _V, 'BET Plomberie': _V},
    ('RSV', '42'):   {'BET Acoustique': _V, 'BET Plomberie': _V},
    ('SYQ', '42'):   {'BET Plomberie': _V},

    # ── lot 42B — sub-lot ───────────────────────────────────────────────
    ('MAT', '42B'):  {'BET Acoustique': _V, 'BET Plomberie': _V},
    ('PLN', '42B'):  {'BET Acoustique': _V, 'BET Plomberie': _V},

    # ── lot 43 — PROTECTION INCENDIE ────────────────────────────────────
    ('MAT', '43'):   {'BET Acoustique': _V, 'BET SPK': _V},
    ('NDC', '43'):   {'BET Acoustique': _V, 'BET SPK': _V},
    ('PLN', '43'):   {'BET Acoustique': _V, 'BET SPK': _V},

    # ── lot 51 — APPAREILS ELEVATEURS ───────────────────────────────────
    ('DET', '51'):   {'BET Acoustique': _V, 'BET Ascenseur': _V},

    # ── lot 61 — VRD ───────────────────────────────────────────────────
    ('MAT', '61'):   {'BET VRD': _V},

    # ── lot 62 — ESPACES VERTS ──────────────────────────────────────────
    ('MAT', '62'):   {'BET VRD': _V},
    ('PLN', '62'):   {'BET VRD': _V},

    # ── Infrastructure lots ─────────────────────────────────────────────
    # lot 01: BET Structure + BET POL (CRV, CPE, MTD already in circuit)
    ('CRV', '01'):   {'BET Structure': _V, 'BET POL': _V},
    ('CPE', '01'):   {'BET Structure': _V, 'BET POL': _V},
    ('MTD', '01'):   {'BET Structure': _V, 'BET POL': _V},

    # lot 03: BET POL (QLT)
    ('QLT', '03'):   {'BET POL': _V},
}


def lookup_override(lot_family: str, type_doc: str, mission_role: str) -> str:
    """Look up project override rule for a (lot_family, type_doc, mission_role).

    Args:
        lot_family: Normalized lot family (e.g. '31', '03', '12A')
        type_doc: Document type code (e.g. 'MAT', 'PLN')
        mission_role: GED mission role WITHOUT building prefix
                      (e.g. 'BET Acoustique', NOT '0-BET Acoustique')

    Returns:
        'REQUIRED_VISA' if override rule matches, else None.
    """
    if not lot_family or not type_doc or not mission_role:
        return None
    entry = OVERRIDE_RULES.get((type_doc, lot_family))
    if entry and mission_role in entry:
        return entry[mission_role]
    return None
