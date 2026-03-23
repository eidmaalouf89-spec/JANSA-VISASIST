"""Shared constants for GED pipeline.

All canonical enums and mappings live here (Global Enum Registry).
No enum values may be hardcoded inconsistently across modules.
"""

# Source sheet names
GED_PRIMARY_SHEET = 'Vue détaillée des documents 1'
GED_FALLBACK_SHEET = 'Vue détaillée des documents'

# Document identity columns — forward-filled within each doc_id group
IDENTITY_COLS = [
    'Chemin', 'AFFAIRE', 'PROJET', 'BATIMENT', 'PHASE', 'EMETTEUR',
    'SPECIALITE', 'LOT', 'TYPE DE DOC', 'ZONE', 'NIVEAU', 'NUMERO',
    'INDICE', 'Libellé du fichier', 'Description', 'Format',
    'Version créée par', 'Date prévisionnelle', 'Date de dépôt effectif',
    'Écart avec la date de dépôt prévue', 'Version', 'Dernière modification',
    'Taille (Mo)', 'Statut final du document'
]

# Columns used to build famille_key (excludes INDICE)
FAMILY_KEY_COLS = [
    'AFFAIRE', 'PROJET', 'BATIMENT', 'PHASE', 'EMETTEUR',
    'SPECIALITE', 'LOT', 'TYPE DE DOC', 'ZONE', 'NIVEAU', 'NUMERO'
]

# All required columns that must be present in the source sheet
REQUIRED_COLUMNS = IDENTITY_COLS + [
    'Identifiant',
    'Mission', 'Répondant', 'Date limite pour répondre',
    'Réponse donnée le', 'Écart avec la date de réponse prévue',
    'Réponse', 'Commentaire', 'Pièces jointes', 'Type de réponse',
    'Mission associée'
]

# Column rename mapping: source name -> internal alias
COLUMN_RENAME_MAP = {
    'Chemin': 'chemin',
    'AFFAIRE': 'affaire',
    'PROJET': 'tranche',
    'BATIMENT': 'batiment',
    'PHASE': 'phase',
    'EMETTEUR': 'emetteur',
    'SPECIALITE': 'specialite',
    'LOT': 'lot',
    'TYPE DE DOC': 'type_doc',
    'ZONE': 'zone',
    'NIVEAU': 'niveau',
    'NUMERO': 'numero',
    'INDICE': 'indice',
    'Libellé du fichier': 'libelle',
    'Description': 'description',
    'Format': 'format_fichier',
    'Version créée par': 'deposant',
    'Date prévisionnelle': 'date_prevue_raw',
    'Date de dépôt effectif': 'date_depot_raw',
    'Écart avec la date de dépôt prévue': 'ecart_depot',
    'Dernière modification': 'derniere_modif_raw',
    'Taille (Mo)': 'taille_mo',
    'Statut final du document': 'statut_final_raw',
    'Mission': 'mission',
    'Répondant': 'repondant',
    'Date limite pour répondre': 'deadline_raw',
    'Réponse donnée le': 'date_reponse_raw',
    'Écart avec la date de réponse prévue': 'ecart_reponse',
    'Réponse': 'reponse_raw',
    'Commentaire': 'commentaire',
    'Pièces jointes': 'pieces_jointes',
    'Type de réponse': 'type_reponse',
    'Mission associée': 'mission_associee',
    'Type de document': 'type_document_ged',
}

# ---------------------------------------------------------------------------
# NM3 — Mission type classification patterns
# ---------------------------------------------------------------------------
MISSION_TYPE_PATTERNS = {
    'SAS': ['SAS'],
    'MOEX': ["Maître d'Oeuvre EXE", "Maitre d'Oeuvre EXE"],
    'SUBCONTRACTOR': [
        'Sollicitation supplémentaire',   # ad-hoc solicitation, not a circuit reviewer
        'H51-ASC',                        # lot 51 subcontractor (Ascenseur building H)
        'B13 - METALLERIE SERRURERIE',    # lot 13 subcontractor (Métallerie building B)
    ],
    # anything else → REVIEWER; null/empty → UNKNOWN
}

# ---------------------------------------------------------------------------
# NM3 — Vocabulary mapping (prefix-based, order matters — first match wins)
# (prefix, reponse_normalized, response_status)
# ---------------------------------------------------------------------------
VOCAB_MAP = [
    ('Validé sans observation - SAS', 'VSO', 'RESPONDED_APPROVE'),
    ('Validé sans observation',       'VSO', 'RESPONDED_APPROVE'),
    ('Validé avec observation',       'VAO', 'RESPONDED_APPROVE'),
    ('Favorable',                     'FAV', 'RESPONDED_APPROVE'),
    ('Refusé',                        'REF', 'RESPONDED_REJECT'),
    ('Défavorable',                   'DEF', 'RESPONDED_REJECT'),
    ('Hors Mission',                  'HM',  'RESPONDED_HM'),
    ('Suspendu',                      'SUS', 'RESPONDED_OTHER'),
    ('Sollicitation supplémentaire',  'SUP', 'RESPONDED_OTHER'),
    ('En attente',                    None,  'NOT_RESPONDED'),
    ('Soumis',                        None,  'PENDING_CIRCUIT'),
]

# ---------------------------------------------------------------------------
# NM3 — Canonical enum values
# ---------------------------------------------------------------------------
MISSION_TYPE_VALUES = {'SAS', 'MOEX', 'REVIEWER', 'SUBCONTRACTOR', 'UNKNOWN'}

RESPONSE_STATUS_VALUES = {
    'RESPONDED_APPROVE', 'RESPONDED_REJECT', 'RESPONDED_HM',
    'RESPONDED_OTHER', 'RESPONDED_AMBIGUOUS',
    'NOT_RESPONDED', 'PENDING_CIRCUIT',
}

# ---------------------------------------------------------------------------
# NM2 — SAS state enum values
# ---------------------------------------------------------------------------
SAS_STATE_VALUES = {
    'SAS_UNKNOWN', 'SAS_PENDING', 'SAS_BLOCKED',
    'SAS_PASSED', 'SAS_WITH_VERDICT',
}

SAS_CONFIDENCE_VALUES = {'HIGH', 'LOW'}

# ---------------------------------------------------------------------------
# NM7 — Lifecycle state enum values
# ---------------------------------------------------------------------------
LIFECYCLE_STATE_VALUES = {
    'SYNTHESIS_ISSUED', 'SAS_BLOCKED', 'SAS_PENDING', 'HM_EXCLUDED',
    'NOT_STARTED', 'WAITING_RESPONSES', 'READY_TO_ISSUE',
    'FAST_REJECT', 'CHRONIC_BLOCKED', 'CONFLICT',
}

QUEUE_DESTINATION_VALUES = {'PRIORITY_QUEUE', 'INTAKE_ISSUES', 'EXCLUDED'}

BLOCKER_TYPE_VALUES = {'COMPANY', 'GEMO_SAS', 'GEMO_MOEX', 'CONSULTANT', 'NONE'}

CONSENSUS_TYPE_VALUES = {
    'NOT_STARTED', 'INCOMPLETE', 'ALL_APPROVE', 'ALL_REJECT', 'MIXED', 'ALL_HM',
}

PRIORITY_CATEGORY_VALUES = {'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'}

# ---------------------------------------------------------------------------
# NM7 — Scoring weights (R-NM7-09: all named constants, never hardcoded)
# ---------------------------------------------------------------------------
LIFECYCLE_WEIGHT = {
    'CONFLICT':           100,
    'READY_TO_ISSUE':      90,
    'CHRONIC_BLOCKED':     80,
    'FAST_REJECT':         70,
    'WAITING_RESPONSES':   60,
    'NOT_STARTED':         30,
    'SAS_PENDING':         10,
    'SAS_BLOCKED':         10,
}

BLOCKER_WEIGHT = {
    'GEMO_MOEX': 20,
    'CONSULTANT': 10,
    'COMPANY':     5,
    'GEMO_SAS':   5,
    'NONE':        0,
}

CONFIDENCE_DEDUCTIONS = {
    'SAS_ASSUMED_PASSED':               -0.30,
    'USED_FALLBACK_DISCIPLINE':         -0.20,
    'CONDITIONAL_TRIGGERED_FROM_COMMENT': -0.10,
    'AMBIGUOUS_RESPONSE':               -0.20,
    'MULTIPLE_MOEX_VERDICTS':           -0.25,
    'MISSING_ASSIGNMENT_DATA':          -0.20,
    'MISSING_MOEX_ASSIGNMENT':          -0.15,
    'UNKNOWN_ASSIGNMENT':               -0.15,
}

# ---------------------------------------------------------------------------
# NM7 — MOEX definitive verdict set
# ---------------------------------------------------------------------------
MOEX_DEFINITIVE_VERDICTS = {'VSO', 'VAO', 'REF', 'HM', 'FAV', 'DEF', 'SUS'}

# ---------------------------------------------------------------------------
# NM7 — Excluded procedure families (R-NM7-11)
# When NM6 unavailable, exclude by type_doc
# ---------------------------------------------------------------------------
EXCLUDED_TYPE_DOC = {'FQR', 'FTM', 'DOE'}
