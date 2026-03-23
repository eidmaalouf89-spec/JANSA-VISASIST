"""Doc Type Configuration — workflow and priority behavior per document type.

Defines how document types influence NM7 workflow behavior (not assignment).
Based on post-legacy analysis of 2,396 active docs across 27 types.

Phase 1 attributes:
  - family: canonical grouping (A–F)
  - workflow: STANDARD_VISA | REDUCED | EXCLUDED | MONITOR
  - priority_weight: multiplier for NM7 base priority score

Phase 2 (NOT YET IMPLEMENTED):
  - blocking: whether missing responses block lot-level progression
  - reject_sensitivity: how rejects are interpreted
  - expected_reviewers: for anomaly detection
"""

# ---------------------------------------------------------------------------
# Family codes
# ---------------------------------------------------------------------------
FAMILY_A = 'A'  # Graphical Documents (Plans & Drawings)
FAMILY_B = 'B'  # Technical Notes & Calculations
FAMILY_C = 'C'  # Synthesis & Coordination
FAMILY_D = 'D'  # Administrative & Quality
FAMILY_E = 'E'  # Correspondence & Reporting
FAMILY_F = 'F'  # Diverse & Reference

# ---------------------------------------------------------------------------
# Workflow modes
# ---------------------------------------------------------------------------
STANDARD_VISA = 'STANDARD_VISA'
REDUCED = 'REDUCED'
EXCLUDED = 'EXCLUDED'
MONITOR = 'MONITOR'
# MOEX_ONLY = 'MOEX_ONLY'  # Phase 2

# ---------------------------------------------------------------------------
# Priority weight scale (Phase 1 simplified)
#   1.2 — elevated: structural/coordination-critical
#   1.0 — standard: normal blocking types
#   0.7 — reduced: non-blocking, secondary docs
#   0.0 — excluded: not in priority queue
# ---------------------------------------------------------------------------
WEIGHT_ELEVATED = 1.2
WEIGHT_STANDARD = 1.0
WEIGHT_REDUCED = 0.7
WEIGHT_EXCLUDED = 0.0


# ---------------------------------------------------------------------------
# Configuration dictionary
# ---------------------------------------------------------------------------
DOC_TYPE_CONFIG = {
    # ── Family A: Graphical Documents ──────────────────────────────────
    'PLN': {'family': FAMILY_A, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_ELEVATED},
    'DET': {'family': FAMILY_A, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},
    'ARM': {'family': FAMILY_A, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_ELEVATED},
    'COF': {'family': FAMILY_A, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},
    'CLP': {'family': FAMILY_A, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},
    'IMP': {'family': FAMILY_A, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},
    'RSV': {'family': FAMILY_A, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},

    # ── Family B: Technical Notes & Calculations ──────────────────────
    'NDC': {'family': FAMILY_B, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_ELEVATED},
    'MAT': {'family': FAMILY_B, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},
    'RSX': {'family': FAMILY_B, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},
    'TMX': {'family': FAMILY_B, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},

    # ── Family C: Synthesis & Coordination ─────────────────────────────
    'SYQ': {'family': FAMILY_C, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_ELEVATED},
    'TDP': {'family': FAMILY_C, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},

    # ── Family D: Administrative & Quality ─────────────────────────────
    'QLT': {'family': FAMILY_D, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_STANDARD},
    'FQR': {'family': FAMILY_D, 'workflow': EXCLUDED,      'priority_weight': WEIGHT_EXCLUDED},
    'PPS': {'family': FAMILY_D, 'workflow': REDUCED,        'priority_weight': WEIGHT_REDUCED},
    'MTD': {'family': FAMILY_D, 'workflow': REDUCED,        'priority_weight': WEIGHT_REDUCED},
    'PVT': {'family': FAMILY_D, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},

    # ── Family E: Correspondence & Reporting ───────────────────────────
    'LTE': {'family': FAMILY_E, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},
    'NOT': {'family': FAMILY_E, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},
    'REP': {'family': FAMILY_E, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},
    'CPE': {'family': FAMILY_E, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},
    'LST': {'family': FAMILY_E, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},

    # ── Family F: Diverse & Reference ──────────────────────────────────
    'DVM': {'family': FAMILY_F, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},
    'CRV': {'family': FAMILY_F, 'workflow': STANDARD_VISA, 'priority_weight': WEIGHT_REDUCED},
    'MAQ': {'family': FAMILY_F, 'workflow': MONITOR,       'priority_weight': WEIGHT_REDUCED},
    'PIC': {'family': FAMILY_F, 'workflow': EXCLUDED,      'priority_weight': WEIGHT_EXCLUDED},
}

# Set of type_doc values that should be excluded from NM7 processing
EXCLUDED_DOC_TYPES = frozenset(
    td for td, cfg in DOC_TYPE_CONFIG.items() if cfg['workflow'] == EXCLUDED
)


def get_config(type_doc: str) -> dict:
    """Get configuration for a document type.

    Returns the config dict if known, otherwise a safe default
    (STANDARD_VISA, weight 1.0) so unknown types are never silently dropped.
    """
    return DOC_TYPE_CONFIG.get(type_doc, {
        'family': None,
        'workflow': STANDARD_VISA,
        'priority_weight': WEIGHT_STANDARD,
    })


def get_priority_weight(type_doc: str) -> float:
    """Get priority weight for a document type."""
    return get_config(type_doc)['priority_weight']


def is_excluded(type_doc: str) -> bool:
    """Check if a document type is excluded from NM7 processing."""
    return type_doc in EXCLUDED_DOC_TYPES
