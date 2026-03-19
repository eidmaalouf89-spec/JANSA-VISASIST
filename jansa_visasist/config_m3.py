"""
JANSA VISASIST — Module 3 Configuration
Constants for Pilotage / Prioritization Engine.
"""

# ──────────────────────────────────────────────────
# Input contract
# ──────────────────────────────────────────────────

# [SAFEGUARD] Required columns from M2 enriched output
M3_REQUIRED_COLUMNS = {
    "visa_global", "date_contractuelle_visa", "date_diffusion",
    "assigned_approvers", "is_latest", "ind", "revision_count",
    "source_sheet", "duplicate_flag", "doc_family_key", "lot",
}

# [SAFEGUARD] Optional columns (degrade gracefully if missing)
M3_OPTIONAL_COLUMNS = {"row_id", "doc_version_key"}

# ──────────────────────────────────────────────────
# Valid enum values
# ──────────────────────────────────────────────────

VALID_CONSENSUS_TYPES = {
    "NOT_STARTED", "INCOMPLETE", "ALL_HM",
    "MIXED", "ALL_REJECT", "ALL_APPROVE",
}

VALID_CATEGORIES = {
    "EASY_WIN_APPROVE", "BLOCKED", "FAST_REJECT",
    "CONFLICT", "WAITING", "NOT_STARTED",
}

EXCLUSION_REASONS = {
    "NOT_LATEST", "DUPLICATE", "VISA_RESOLVED",
    "VISA_HM", "ALL_APPROVERS_HM",
}

# ──────────────────────────────────────────────────
# Approver status classification (consensus-driving only)
# ──────────────────────────────────────────────────

# [SPEC] Only these statuses drive the consensus decision tree.
# Any other non-null statut is "replied but non-driving" (WARNING logged).
CONSENSUS_VSO_VAO_STATUSES = {"VSO", "VAO"}
CONSENSUS_REF_STATUSES = {"REF"}
CONSENSUS_HM_STATUSES = {"HM"}

# ──────────────────────────────────────────────────
# Scoring weights
# ──────────────────────────────────────────────────

# [SPEC] Overdue component: 0–40, linear from 0 at deadline to 40 at 30+ days
OVERDUE_MAX_POINTS = 40
OVERDUE_CAP_DAYS = 30

# [SPEC] Deadline proximity component
PROXIMITY_3D_POINTS = 25
PROXIMITY_7D_POINTS = 20
PROXIMITY_14D_POINTS = 10

# [SPEC] Completeness / consensus component
COMPLETENESS_ALL_APPROVE = 20
COMPLETENESS_ALL_REJECT = 15
COMPLETENESS_MIXED = 10

# [SPEC] Revision depth component
REVISION_DEPTH_HIGH = 5       # revision_count > 2
REVISION_DEPTH_MED = 3        # revision_count == 2

# [SPEC] Missing deadline penalty
MISSING_DEADLINE_PENALTY = -10

# [SPEC] Score bounds
SCORE_MIN = 0
SCORE_MAX = 100
