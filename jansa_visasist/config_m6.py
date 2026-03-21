"""
JANSA VISASIST — Module 6 Configuration
Constants, static dictionaries, field list constants for the Chatbot module.
"""

# ──────────────────────────────────────────────
# Thresholds and limits
# ──────────────────────────────────────────────

AI_CLASSIFIER_CONFIDENCE_THRESHOLD = 0.70
CHATBOT_INLINE_LIMIT = 20
CHATBOT_PREVIEW_COUNT = 5
LARGE_EXPORT_THRESHOLD = 500
PROJECT_DOC_PREFIX = "P17"

# ──────────────────────────────────────────────
# fields_used per command (explicit constants — P8)
# ──────────────────────────────────────────────

FIELDS_FILTER_ROW = [
    "row_id", "document", "titre", "source_sheet",
    "category", "priority_score", "is_overdue",
]

FIELDS_C6_M3 = [
    "row_id", "document", "titre", "source_sheet", "lot", "ind",
    "category", "consensus_type", "priority_score", "is_overdue",
    "days_overdue", "missing_approvers", "blocking_approvers", "observations",
]

FIELDS_C6_M4 = ["lifecycle_state", "analysis_degraded"]

FIELDS_C6_M5 = ["suggested_action", "proposed_visa"]

FIELDS_C6_DISAMBIGUATION = [
    "row_id", "document", "source_sheet", "priority_score", "ind",
]

FIELDS_C7 = [
    "row_id", "document", "priority_score", "category", "consensus_type",
    "is_overdue", "days_overdue", "revision_count",
    "missing_approvers", "blocking_approvers",
]

FIELDS_C8 = [
    "source_sheet", "category", "priority_score", "is_overdue", "days_overdue",
]

FIELDS_C9 = [
    "missing_approvers", "blocking_approvers", "source_sheet",
    "assigned_approvers", "priority_score",
]

# Truncation aggregate fields
FIELDS_TRUNCATION_AGGREGATE = ["result_count", "category"]

# C10 and C12 data_references fields
FIELDS_C10 = ["result_count"]
FIELDS_C12 = ["result_count"]

# ──────────────────────────────────────────────
# Static dictionaries
# ──────────────────────────────────────────────

CATEGORY_ALIASES = {
    "easy win": "EASY_WIN_APPROVE", "easy_win": "EASY_WIN_APPROVE",
    "approve": "EASY_WIN_APPROVE", "approved": "EASY_WIN_APPROVE",
    "blocked": "BLOCKED", "blocking": "BLOCKED", "chronic": "BLOCKED",
    "fast reject": "FAST_REJECT", "reject": "FAST_REJECT", "rejected": "FAST_REJECT",
    "conflict": "CONFLICT", "mixed": "CONFLICT",
    "waiting": "WAITING", "incomplete": "WAITING", "pending": "WAITING",
    "not started": "NOT_STARTED", "new": "NOT_STARTED", "fresh": "NOT_STARTED",
    # French (singular + plural)
    "approuve": "EASY_WIN_APPROVE", "approuves": "EASY_WIN_APPROVE",
    "bloque": "BLOCKED", "bloques": "BLOCKED", "chronique": "BLOCKED", "chroniques": "BLOCKED",
    "refuse": "FAST_REJECT", "refuses": "FAST_REJECT", "rejet": "FAST_REJECT", "rejets": "FAST_REJECT",
    "conflit": "CONFLICT", "conflits": "CONFLICT",
    "en attente": "WAITING", "attente": "WAITING",
    "pas demarre": "NOT_STARTED", "non demarre": "NOT_STARTED", "nouveau": "NOT_STARTED", "nouveaux": "NOT_STARTED",
}

STATUS_SYNONYMS = {
    "overdue": {"filter": "is_overdue", "value": True},
    "en retard": {"filter": "is_overdue", "value": True},
    "retard": {"filter": "is_overdue", "value": True},
    "late": {"filter": "is_overdue", "value": True},
    "urgent": {"filter": "priority_score", "operator": ">=", "value": 40},
    "critique": {"filter": "priority_score", "operator": ">=", "value": 40},
    "critical": {"filter": "priority_score", "operator": ">=", "value": 40},
}

ACTION_KEYWORDS = {
    "pourquoi": "EXPLAIN", "why": "EXPLAIN", "expliqu": "EXPLAIN", "raison": "EXPLAIN",
    "export": "EXPORT", "exporter": "EXPORT", "telecharger": "EXPORT",
    "download": "EXPORT", "csv": "EXPORT", "excel": "EXPORT",
    "combien": "COUNT", "how many": "COUNT", "count": "COUNT",
    "nombre": "COUNT", "nb": "COUNT",
    "resume": "SUMMARY", "summary": "SUMMARY", "synthese": "SUMMARY",
    "situation": "SUMMARY", "rapport": "SUMMARY", "bilan": "SUMMARY",
}

# Action priority order for detection
ACTION_PRIORITY = ["EXPLAIN", "EXPORT", "COUNT", "SUMMARY", "RETRIEVE"]

# ──────────────────────────────────────────────
# Sources used per command
# ──────────────────────────────────────────────

SOURCES_M3_ONLY = ["M3"]
SOURCES_C7_DEFAULT = ["M3", "M4"]

# ──────────────────────────────────────────────
# Warning templates
# ──────────────────────────────────────────────

WARNING_AI_LOW_CONFIDENCE = "Classification AI confidence faible, resolution deterministe."
WARNING_AMBIGUOUS_DOC = "Document ambigu : {n} correspondances dans {sheets}."
WARNING_M4_UNAVAILABLE = "Donnees M4 non disponibles. Resultat partiel."
WARNING_M5_UNAVAILABLE = "Donnees M5 non disponibles. Resultat partiel."
WARNING_LARGE_EXPORT = "Export volumineux : {count} lignes."
