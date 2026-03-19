"""
JANSA VISASIST — Module 2 Configuration
Constants for Data Model & Revision Linking.
"""

# ──────────────────────────────────────────────────
# Duplicate comparison exclusions
# ──────────────────────────────────────────────────

# Duplicate comparison rule: see pipeline/m2/duplicate_detection.py for
# the authoritative implementation. Only columns unique by definition
# are excluded. Everything else from M1 participates.
DUPLICATE_EXCLUDE_COLS = {
    "row_id",       # Unique by definition: {sheet_index}_{source_row}
    "source_row",   # Unique by definition: original Excel row number
}

# [SAFEGUARD] M2-derived columns present at Step 5 execution time.
# Excluded at runtime because they are deterministically derived from
# columns already in the comparison set.
M2_DERIVED_COLS_STEP5 = {
    "doc_family_key",   # Derived from document
    "ind_sort_order",   # Derived from ind
}

# ──────────────────────────────────────────────────
# Anomaly type constants
# ──────────────────────────────────────────────────

ANOMALY_REVISION_GAP = "REVISION_GAP"
ANOMALY_LATE_FIRST = "LATE_FIRST_APPEARANCE"
ANOMALY_DATE_REGRESSION = "DATE_REGRESSION"
ANOMALY_DUPLICATE_EXACT = "DUPLICATE_EXACT"
ANOMALY_DUPLICATE_SUSPECT = "DUPLICATE_SUSPECT"
ANOMALY_MISSING_IND = "MISSING_IND"
ANOMALY_UNPARSEABLE = "UNPARSEABLE_DOCUMENT"

# ──────────────────────────────────────────────────
# Key construction constants
# ──────────────────────────────────────────────────

# [SPEC] Prefix for fallback keys when document is null
UNPARSEABLE_PREFIX = "UNPARSEABLE::"

# [SAFEGUARD] Hash truncation length (hex chars) for UNPARSEABLE keys
HASH_TRUNCATE_LENGTH = 16

# [SAFEGUARD] Label used in doc_version_key when ind is null.
# The spec does not define a string representation for null IND in key
# construction. "NULL" is chosen to produce unambiguous, readable keys
# and avoid empty segments ("::::").
NULL_IND_LABEL = "NULL"

# ──────────────────────────────────────────────────
# Input contract
# ──────────────────────────────────────────────────

# [SAFEGUARD] Required M1 columns for M2 to proceed
M2_REQUIRED_COLUMNS = {"document", "ind", "source_sheet", "source_row", "row_id"}

# [SAFEGUARD] Optional M1 columns (degrade gracefully if missing)
M2_OPTIONAL_COLUMNS = {"date_diffusion", "visa_global"}
