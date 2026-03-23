"""Circuit de diffusion matrix for P17&CO Tranche 2.

Source: P17&CO-T2-Circuit de diffusion des documents-V2.pdf
Source: P17&CO-T2-Liste des lots-V18.pdf
Source: P17&CO-T2-Procédure de Codification des Docs TCE-V20.pdf

Maps (lot_family, type_doc, reviewer_key) → assignment_type.
"""

import re

from jansa.adapters.ged.logging import log_event
from jansa.adapters.ged.project_overrides import lookup_override


# ---------------------------------------------------------------------------
# 1. Reviewer normalization: GED mission role → circuit reviewer key
# ---------------------------------------------------------------------------

MISSION_ROLE_TO_REVIEWER = {
    'ARCHITECTE':         'HARDEL',
    'Bureau de Contrôle': 'SOCOTEC',
    'AMO HQE':            'LE_SOMMER',
    'BET Structure':      'TERRELL',
    'BET Façade':         'ELIOTH',
    'BET Acoustique':     'AVLS',
    'BET Ascenseur':      'ASCAUDIT',
    'BET CVC':            'EGIS_FLUIDES',
    'BET Electricité':    'EGIS_FLUIDES',
    'BET Plomberie':      'EGIS_FLUIDES',
    'BET SPK':            'EGIS_FLUIDES',
    'BET VRD':            'EGIS_VRD',
    'BET EV':             'EGIS_VRD',
    'BET Géotech':        'GEOLIA',
    'BET POL':            'GEOLIA',
    'CSPS':               'LM3C',
    'BIM Manager':        'ALTO',
}


def extract_mission_role(mission: str) -> str:
    """Extract reviewer role from GED mission name.

    '0-Bureau de Contrôle' → 'Bureau de Contrôle'
    'B13 - METALLERIE SERRURERIE' → 'B13 - METALLERIE SERRURERIE'
    'H51-ASC' → 'H51-ASC'
    """
    if not mission:
        return ''
    # Standard pattern: {bat_prefix}-{role}
    m = re.match(r'^[0ABHG]-(.+)$', mission)
    if m:
        return m.group(1).strip()
    return mission.strip()


def mission_to_reviewer_key(mission: str) -> str:
    """Map GED mission name to canonical circuit reviewer key.

    Returns reviewer_key or None if not a mapped reviewer.
    """
    role = extract_mission_role(mission)
    return MISSION_ROLE_TO_REVIEWER.get(role)


# ---------------------------------------------------------------------------
# 2. Lot family extraction
# ---------------------------------------------------------------------------

# Explicit alias for lot families that differ between GED and circuit
LOT_FAMILY_ALIAS = {
    '06B': '09',    # COUVERTURE (Bureau/Hotel B06B/H06B) = circuit lot 09
    '0A':  None,    # DESAMIANTAGE — not in circuit
    '02A': None,    # INJECTION — not in circuit
}


def extract_lot_family(lot_code: str) -> str:
    """Extract lot family from GED lot code.

    I003 → '03', A12B → '12B', G61A → '61A', I00B → '00B', B06B → '06B'
    I012A → '12A', I013B → '13B', I016A → '16A'
    """
    if not lot_code or not isinstance(lot_code, str):
        return None
    raw = lot_code.strip()
    # Strip building prefix (first char if I/A/B/H/G)
    if raw and raw[0] in 'IABHG':
        raw = raw[1:]
    if not raw:
        return None
    # Split into numeric prefix and alpha suffix:
    # '012A' → prefix='012', suffix='A'
    # '003'  → prefix='003', suffix=''
    # '06B'  → prefix='06',  suffix='B'
    # '00B'  → prefix='00',  suffix='B'
    prefix = ''
    suffix = ''
    for i, c in enumerate(raw):
        if c.isdigit():
            prefix += c
        else:
            suffix = raw[i:]
            break
    # Strip leading zeros from numeric prefix, keep at least 2 digits
    while len(prefix) > 2 and prefix[0] == '0' and prefix[1].isdigit():
        prefix = prefix[1:]
    return prefix + suffix


def resolve_lot_family(lot_family: str) -> str:
    """Resolve lot family through alias table and parent-lot fallback.

    Returns the circuit lot key to use for lookup, or None if unmapped.
    """
    if not lot_family:
        return None
    # Direct alias
    if lot_family in LOT_FAMILY_ALIAS:
        return LOT_FAMILY_ALIAS[lot_family]
    # Direct match in circuit
    if lot_family in _LOT_FAMILIES_IN_CIRCUIT:
        return lot_family
    # Parent-lot fallback: strip trailing letter(s) → e.g. '12A' → '12'
    parent = re.sub(r'[A-Za-z]+$', '', lot_family)
    if parent and parent != lot_family and parent in _LOT_FAMILIES_IN_CIRCUIT:
        return parent
    return lot_family  # return as-is; lookup will handle miss


# ---------------------------------------------------------------------------
# 3. Circuit matrix data
#    Key: (lot_family, type_doc) where type_doc='*' = lot default
#    Value: {reviewer_key: assignment_type}
# ---------------------------------------------------------------------------

_V = 'REQUIRED_VISA'
_I = 'INFORMATIONAL'

CIRCUIT = {
    # ── GLOBAL document types (apply regardless of lot) ──────────────
    ('GLOBAL', 'PPS'):        {'LM3C': _V},
    ('GLOBAL', 'IMP'):        {'LE_SOMMER': _I, 'LM3C': _V},
    ('GLOBAL', 'MTD'):        {'LE_SOMMER': _I, 'SOCOTEC': _I, 'LM3C': _V,
                               'TERRELL': _I, 'ELIOTH': _I, 'EGIS_FLUIDES': _I,
                               'EGIS_VRD': _I, 'GEOLIA': _I},
    ('GLOBAL', 'NOT'):        {'LE_SOMMER': _V},
    ('GLOBAL', 'MAT'):        {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _I,
                               'TERRELL': _V, 'ELIOTH': _V, 'EGIS_FLUIDES': _V,
                               'AVLS': _V, 'ASCAUDIT': _V, 'EGIS_VRD': _V},
    ('GLOBAL', 'MAQ'):        {'HARDEL': _I, 'ALTO': _V, 'TERRELL': _I,
                               'ELIOTH': _I, 'EGIS_FLUIDES': _I},

    # ── BIM Synthèse documents ───────────────────────────────────────
    ('BIM', 'SYQ'):  {'HARDEL': _I, 'ALTO': _V, 'SOCOTEC': _I,
                      'TERRELL': _I, 'ELIOTH': _I, 'EGIS_FLUIDES': _I},
    ('BIM', 'RSV'):  {'HARDEL': _I, 'ALTO': _V, 'SOCOTEC': _I,
                      'TERRELL': _I, 'ELIOTH': _I, 'EGIS_FLUIDES': _I},
    ('BIM', 'RSX'):  {'HARDEL': _I, 'ALTO': _V, 'SOCOTEC': _I,
                      'TERRELL': _I, 'ELIOTH': _I, 'EGIS_FLUIDES': _I},
    ('BIM', 'TMX'):  {'HARDEL': _I, 'ALTO': _V, 'SOCOTEC': _I,
                      'TERRELL': _I, 'ELIOTH': _I, 'EGIS_FLUIDES': _I},
    ('BIM', 'DET'):  {'HARDEL': _I, 'ALTO': _V, 'SOCOTEC': _I,
                      'TERRELL': _I, 'ELIOTH': _I, 'EGIS_FLUIDES': _I},

    # ── 00B — DECONSTRUCTION / CURAGE ────────────────────────────────
    ('00B', '*'):   {'HARDEL': _I, 'LE_SOMMER': _I, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'AVLS': _V,
                     'EGIS_VRD': _I, 'GEOLIA': _I},

    # ── 01 — TERRASSEMENTS ───────────────────────────────────────────
    ('01', 'NDC'):  {'LE_SOMMER': _I, 'SOCOTEC': _V, 'LM3C': _I,
                     'TERRELL': _V, 'EGIS_VRD': _I, 'GEOLIA': _V},
    ('01', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _I, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _I, 'EGIS_VRD': _I, 'GEOLIA': _V},
    ('01', '*'):    {'HARDEL': _I, 'LE_SOMMER': _I, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'EGIS_VRD': _I, 'GEOLIA': _V},

    # ── 02 — FONDATIONS SPECIALES ────────────────────────────────────
    ('02', 'NDC'):  {'LE_SOMMER': _I, 'SOCOTEC': _V, 'LM3C': _I,
                     'TERRELL': _V, 'EGIS_VRD': _I, 'GEOLIA': _V},
    ('02', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'EGIS_VRD': _I, 'GEOLIA': _V},

    # ── 03 — GOE / CHARPENTE BOIS-METAL ─────────────────────────────
    ('03', 'COF'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'AVLS': _V},
    ('03', 'ARM'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'TERRELL': _V},
    ('03', 'CPE'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'AVLS': _V},
    ('03', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'TERRELL': _V},
    ('03', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'AVLS': _V, 'GEOLIA': _V},
    ('03', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'AVLS': _V},

    # ── 04 — ETANCHEITE ─────────────────────────────────────────────
    ('04', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I},
    ('04', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I},
    ('04', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I},

    # ── 05 — MENUISERIES EXTERIEURES / BSO ──────────────────────────
    ('05', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'ELIOTH': _V, 'AVLS': _V},
    ('05', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'AVLS': _V},
    ('05', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'AVLS': _V},

    # ── 06 — REVETEMENT DE FACADES ──────────────────────────────────
    ('06', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'ELIOTH': _V, 'AVLS': _V},
    ('06', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'AVLS': _V},
    ('06', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'AVLS': _V},

    # ── 07 — BETON PREFA ────────────────────────────────────────────
    ('07', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'TERRELL': _V},
    ('07', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'ELIOTH': _V, 'AVLS': _V},
    ('07', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'TERRELL': _V, 'ELIOTH': _V, 'AVLS': _V},

    # ── 08 — MURS RIDEAUX ───────────────────────────────────────────
    ('08', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'ELIOTH': _V, 'AVLS': _V},
    ('08', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'AVLS': _V},
    ('08', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'AVLS': _V},

    # ── 09 — COUVERTURE (also maps from 06B via alias) ──────────────
    ('09', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'AVLS': _V},

    # ── 11 — CLOISONS / DOUBLAGES ───────────────────────────────────
    ('11', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 12 — MENUISERIES INTERIEURES (also 12A, 12B via parent) ─────
    ('12', 'NDC'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},
    ('12', 'DVM'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'BATISS': _V, 'AVLS': _V},
    ('12', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 13 — METALLERIE / SERRURERIE (also 13A, 13B) ────────────────
    ('13', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 14 — PORTE DE PARKING ────────────────────────────────────────
    ('14', '*'):    {'HARDEL': _I, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V, 'AVLS': _V},

    # ── 16A — FAUX PLAFONDS ─────────────────────────────────────────
    ('16A', '*'):   {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 16B — PLAFONDS RAYONNANTS ────────────────────────────────────
    ('16B', '*'):   {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'ELIOTH': _V, 'EGIS_FLUIDES': _V, 'AVLS': _V},

    # ── 17 — FAUX PLANCHERS ─────────────────────────────────────────
    ('17', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 18 — REVETEMENT DURS SOLS ET MURS ───────────────────────────
    ('18', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 19 — SOLS SOUPLES ───────────────────────────────────────────
    ('19', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 20 — PEINTURE ───────────────────────────────────────────────
    ('20', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V},

    # ── 22 — CUISINE (EQUIPEMENTS) ────────────────────────────────
    ('22', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},

    # ── 31 — ELECTRICITE CFO / ONDULEURS ─────────────────────────────
    ('31', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V},
    ('31', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V},
    ('31', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('31', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},

    # ── 33 — SSI ─────────────────────────────────────────────────────
    ('33', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'BATISS': _V, 'EGIS_FLUIDES': _V},
    ('33', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'BATISS': _V, 'EGIS_FLUIDES': _V},
    ('33', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'BATISS': _V, 'EGIS_FLUIDES': _V},
    ('33', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'BATISS': _V, 'EGIS_FLUIDES': _V, 'AVLS': _V},

    # ── 34 — CFA / CA / SURETE ──────────────────────────────────────
    ('34', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V},
    ('34', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V},
    ('34', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V},
    ('34', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V},

    # ── 35 — GTB ─────────────────────────────────────────────────────
    ('35', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V},
    ('35', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V},
    ('35', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V},
    ('35', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V},

    # ── 41 — CVC ─────────────────────────────────────────────────────
    ('41', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('41', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('41', 'DVM'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'BATISS': _V, 'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('41', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('41', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V, 'GEOLIA': _I},

    # ── 42 — PLOMBERIE / SANITAIRES (also 42B via parent) ───────────
    ('42', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('42', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('42', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('42', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},

    # ── 43 — PROTECTION INCENDIE ────────────────────────────────────
    ('43', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('43', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('43', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},
    ('43', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_FLUIDES': _V, 'AVLS': _V},

    # ── 51 — APPAREILS ELEVATEURS ───────────────────────────────────
    ('51', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'AVLS': _V, 'ASCAUDIT': _V},
    ('51', 'SYQ'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'AVLS': _V, 'ASCAUDIT': _V},
    ('51', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V, 'ASCAUDIT': _V},
    ('51', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'AVLS': _V, 'ASCAUDIT': _V},

    # ── 61 — VRD (61A + 61B, same circuit) ──────────────────────────
    ('61', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_VRD': _V, 'GEOLIA': _V},
    ('61', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_VRD': _V, 'GEOLIA': _V},
    ('61', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_VRD': _V, 'GEOLIA': _V},

    # ── 62 — ESPACES VERTS ──────────────────────────────────────────
    ('62', 'NDC'):  {'LE_SOMMER': _V, 'SOCOTEC': _V, 'LM3C': _I,
                     'EGIS_VRD': _V, 'GEOLIA': _V},
    ('62', 'PLN'):  {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_VRD': _V, 'GEOLIA': _V},
    ('62', '*'):    {'HARDEL': _I, 'LE_SOMMER': _V, 'SOCOTEC': _V,
                     'LM3C': _I, 'EGIS_VRD': _V, 'GEOLIA': _V},
}

# Pre-compute the set of lot families present in the circuit
_LOT_FAMILIES_IN_CIRCUIT = {k[0] for k in CIRCUIT.keys()} - {'GLOBAL', 'BIM'}


# ---------------------------------------------------------------------------
# 4. Lookup function
# ---------------------------------------------------------------------------

def lookup_assignment(lot_code: str, type_doc: str, mission: str) -> tuple:
    """Look up assignment type for a (lot, type_doc, mission) combination.

    Matching order:
      0. Project override rules (data-driven VERY_STRONG patterns)
      1. Exact: (resolved_lot, type_doc) in CIRCUIT
      2. Lot wildcard: (resolved_lot, '*') in CIRCUIT
      3. Parent lot: strip sub-lot letter, retry steps 1-2
      4. GLOBAL: ('GLOBAL', type_doc) in CIRCUIT
      5. Fallback: return None → caller uses UNKNOWN_REQUIRED

    Returns: ('REQUIRED_VISA', 'DATA_OVERRIDE'),
             ('REQUIRED_VISA'|'INFORMATIONAL', 'MATRIX'),
             or None.
    """
    reviewer_key = mission_to_reviewer_key(mission)
    if reviewer_key is None:
        return None  # Not a mapped reviewer (SAS, MOEX, etc.)

    # Extract mission role for override lookup
    mission_role = extract_mission_role(mission)

    lot_family = extract_lot_family(lot_code)
    resolved = resolve_lot_family(lot_family) if lot_family else None

    td = str(type_doc).strip() if type_doc else '*'

    # Step 0: Project override rules (checked first)
    # Use raw lot_family (before resolve) for sub-lot-specific overrides
    override = lookup_override(lot_family, td, mission_role)
    if override is not None:
        return override, 'DATA_OVERRIDE'

    # Also check with resolved lot_family (for alias handling)
    if resolved and resolved != lot_family:
        override = lookup_override(resolved, td, mission_role)
        if override is not None:
            return override, 'DATA_OVERRIDE'

    # Step 1: Exact match
    entry = CIRCUIT.get((resolved, td))
    if entry and reviewer_key in entry:
        return entry[reviewer_key], 'MATRIX'

    # Step 2: Lot wildcard
    entry = CIRCUIT.get((resolved, '*'))
    if entry and reviewer_key in entry:
        return entry[reviewer_key], 'MATRIX'

    # Step 3: Parent lot (if resolved differs from what parent would give)
    if resolved and lot_family:
        parent = re.sub(r'[A-Za-z]+$', '', resolved)
        if parent and parent != resolved:
            entry = CIRCUIT.get((parent, td))
            if entry and reviewer_key in entry:
                return entry[reviewer_key], 'MATRIX'
            entry = CIRCUIT.get((parent, '*'))
            if entry and reviewer_key in entry:
                return entry[reviewer_key], 'MATRIX'

    # Step 4: GLOBAL type_doc
    entry = CIRCUIT.get(('GLOBAL', td))
    if entry and reviewer_key in entry:
        return entry[reviewer_key], 'MATRIX'

    # Step 5: No match
    return None
