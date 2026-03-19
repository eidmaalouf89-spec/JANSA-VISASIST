"""
JANSA VISASIST — Module 1 Configuration
Constants, canonical schema, approver dictionary, status maps.
"""

# ──────────────────────────────────────────────
# Core Columns — canonical schema
# ──────────────────────────────────────────────

CORE_COLUMNS = [
    "source_sheet", "source_row", "row_id", "row_quality", "row_quality_details",
    "document_raw", "document",
    "titre",
    "date_diffusion_raw", "date_diffusion",
    "lot", "type_doc", "niv", "zone", "n_doc",
    "ind_raw", "ind",
    "type_format", "ancien", "n_bdx",
    "date_reception_raw", "date_reception",
    "non_recu_papier",
    "date_contractuelle_visa_raw", "date_contractuelle_visa",
    "visa_global_raw", "visa_global",
    "observations",
    "assigned_approvers",
]

# ──────────────────────────────────────────────
# Canonical column mapping dictionary
# Maps canonical_key -> {keywords, aliases}
# ──────────────────────────────────────────────

CANONICAL_COLUMN_MAP = {
    "document": {
        "exact": ["document"],
        "keywords": ["document"],
    },
    "titre": {
        "exact": ["titre", "titre du document"],
        "keywords": ["titre"],
    },
    "date_diffusion": {
        "exact": ["date diffusion", "date de diffusion"],
        "keywords": ["date", "diffusion"],
    },
    "lot": {
        "exact": ["lot"],
        "keywords": ["lot"],
    },
    "type_doc": {
        "exact": ["type doc", "type de document", "type document"],
        "keywords": ["type", "doc"],
    },
    "niv": {
        "exact": ["niv", "niveau"],
        "keywords": ["niv"],
    },
    "zone": {
        "exact": ["zone"],
        "keywords": ["zone"],
    },
    "n_doc": {
        "exact": ["n doc", "n° doc", "numero doc"],
        "keywords": ["n", "doc"],
    },
    "ind": {
        "exact": ["ind", "indice"],
        "keywords": ["ind"],
    },
    "type_format": {
        "exact": ["type format", "format"],
        "keywords": ["format"],
    },
    "ancien": {
        "exact": ["ancien", "ancien n", "ancien numero"],
        "keywords": ["ancien"],
    },
    "n_bdx": {
        "exact": ["n bdx", "n° bdx", "numero bdx", "bordereau"],
        "keywords": ["bdx"],
    },
    "date_reception": {
        "exact": ["date reception", "date de reception"],
        "keywords": ["date", "reception"],
    },
    "non_recu_papier": {
        "exact": ["non recu papier", "non recu"],
        "keywords": ["non", "recu"],
    },
    "date_contractuelle_visa": {
        "exact": ["date contractuelle visa", "date contractuelle"],
        "keywords": ["contractuelle"],
    },
    "visa_global": {
        "exact": ["visa global", "visa"],
        "keywords": ["visa", "global"],
    },
    "observations": {
        "exact": ["observations", "observation"],
        "keywords": ["observation"],
    },
}

# The set of canonical column keys that are "core" (not approver columns)
MAPPABLE_CORE_KEYS = set(CANONICAL_COLUMN_MAP.keys())

# ──────────────────────────────────────────────
# Canonical Approver Keys (14)
# ──────────────────────────────────────────────

CANONICAL_APPROVERS = [
    "MOEX_GEMO", "ARCHI_MOX", "BET_STR_TERRELL", "BET_GEOLIA_G4",
    "ACOUSTICIEN_AVLS", "AMO_HQE_LE_SOMMER", "BET_POLLUTION_DIE",
    "SOCOTEC", "BET_ELIOTH", "BET_EGIS", "BET_ASCAUDIT",
    "BET_ASCENSEUR", "BET_SPK", "PAYSAGISTE_MUGO",
]

# Approver variant -> canonical key mapping
APPROVER_VARIANT_MAP = {
    "MOEX GEMO": "MOEX_GEMO",
    "MOEX_GEMO": "MOEX_GEMO",
    "ARCHI MOX": "ARCHI_MOX",
    "ARCHI_MOX": "ARCHI_MOX",
    "STR-TERRELL": "BET_STR_TERRELL",
    "BET STR-TERRELL": "BET_STR_TERRELL",
    "BET_STR_TERRELL": "BET_STR_TERRELL",
    "STR TERRELL": "BET_STR_TERRELL",
    "GEOLIA - G4": "BET_GEOLIA_G4",
    "BET GEOLIA - G4": "BET_GEOLIA_G4",
    "BET GEOLIA G4": "BET_GEOLIA_G4",
    "BET_GEOLIA_G4": "BET_GEOLIA_G4",
    "GEOLIA G4": "BET_GEOLIA_G4",
    "ACOUSTICIEN AVLS": "ACOUSTICIEN_AVLS",
    "ACOUSTICIEN_AVLS": "ACOUSTICIEN_AVLS",
    "AMO HQE LE SOMMER": "AMO_HQE_LE_SOMMER",
    "AMO_HQE_LE_SOMMER": "AMO_HQE_LE_SOMMER",
    "AMO HQE": "AMO_HQE_LE_SOMMER",
    "LE SOMMER": "AMO_HQE_LE_SOMMER",
    "POLLUTION DIE": "BET_POLLUTION_DIE",
    "BET POLLUTION DIE": "BET_POLLUTION_DIE",
    "BET_POLLUTION_DIE": "BET_POLLUTION_DIE",
    "SOCOTEC": "SOCOTEC",
    "BC SOCOTEC": "SOCOTEC",
    "BET ELIOTH": "BET_ELIOTH",
    "BET_ELIOTH": "BET_ELIOTH",
    "BET FACADE - ELIOTH": "BET_ELIOTH",
    "BET FACADE ELIOTH": "BET_ELIOTH",
    "BET EGIS": "BET_EGIS",
    "BET_EGIS": "BET_EGIS",
    "BET ASCAUDIT": "BET_ASCAUDIT",
    "BET_ASCAUDIT": "BET_ASCAUDIT",
    "BET ASCENSEUR": "BET_ASCENSEUR",
    "BET_ASCENSEUR": "BET_ASCENSEUR",
    "BET SPK": "BET_SPK",
    "BET_SPK": "BET_SPK",
    "PAYSAGISTE MUGO": "PAYSAGISTE_MUGO",
    "PAYSAGISTE_MUGO": "PAYSAGISTE_MUGO",
}

# Each approver has 3 source columns (in Excel) and 5 output columns
APPROVER_SOURCE_SUBLABELS = ["DATE", "N°", "STATUT"]
APPROVER_OUTPUT_SUFFIXES = ["_date_raw", "_date", "_n", "_statut_raw", "_statut"]

# ──────────────────────────────────────────────
# VISA Status Vocabulary
# ──────────────────────────────────────────────

VISA_GLOBAL_VALUES = {"VSO", "VAO", "REF", "HM", "SUS", "DEF", "FAV"}

# Approver status synonym map: raw (after basic normalization) -> canonical
APPROVER_STATUS_SYNONYMS = {
    "VSO": "VSO",
    "VAO": "VAO",
    "REF": "REF",
    "HM": "HM",
    "SUS": "SUS",
    "DEF": "DEF",
    "FAV": "FAV",
    "V.S.O": "VSO",
    "V.A.O": "VAO",
    "V.S.O.": "VSO",
    "V.A.O.": "VAO",
    "VISA SANS OBSERVATION": "VSO",
    "VISA AVEC OBSERVATION": "VAO",
    "REFUSE": "REF",
    "REFUS": "REF",
    "HORSMARCHE": "HM",
    "HORS MARCHE": "HM",
    "SUSPENDU": "SUS",
    "DEFINITIF": "DEF",
    "FAVORABLE": "FAV",
}

# Ambiguous typos that should NOT be auto-mapped
AMBIGUOUS_STATUSES = {"VSA", "VOA", "VOS", "VAS"}

# ──────────────────────────────────────────────
# Date constants
# ──────────────────────────────────────────────

# Excel serial date range
EXCEL_DATE_MIN = 1
EXCEL_DATE_MAX = 2958465

# Date sanity bounds for validation (Step 10)
DATE_SANITY_MIN = "2020-01-01"
DATE_SANITY_MAX = "2030-12-31"

# Common date formats for string parsing
DATE_FORMATS = [
    "%Y-%m-%d",      # ISO: 2024-01-15
    "%d/%m/%Y",      # French: 15/01/2024
    "%d-%m-%Y",      # French alt: 15-01-2024
    "%d.%m.%Y",      # French dot: 15.01.2024
    "%d/%m/%y",      # Short year: 15/01/24
    "%Y/%m/%d",      # ISO slash: 2024/01/15
]

# ──────────────────────────────────────────────
# Header detection
# ──────────────────────────────────────────────

HEADER_SCAN_MAX_ROW = 15
HEADER_ANCHOR_KEYWORD = "document"

# ──────────────────────────────────────────────
# Document validation rules (Step 7a)
# ──────────────────────────────────────────────

DOC_MIN_LENGTH = 10
DOC_MAX_NOISE_RATIO = 0.30

# ──────────────────────────────────────────────
# Fuzzy matching
# ──────────────────────────────────────────────

FUZZY_THRESHOLD = 0.80
