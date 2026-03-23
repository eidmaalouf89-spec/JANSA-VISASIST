"""Inline test fixtures for NM1-GED tests. No file I/O in tests."""

# Base template with all required columns defaulting to None
_BASE = {
    'Chemin': None, 'Identifiant': None, 'AFFAIRE': None, 'PROJET': None,
    'BATIMENT': None, 'PHASE': None, 'EMETTEUR': None, 'SPECIALITE': None,
    'LOT': None, 'TYPE DE DOC': None, 'ZONE': None, 'NIVEAU': None,
    'NUMERO': None, 'INDICE': None, 'Libellé du fichier': None,
    'Description': None, 'Format': None, 'Version créée par': None,
    'Date prévisionnelle': None, 'Date de dépôt effectif': None,
    'Écart avec la date de dépôt prévue': None, 'Version': None,
    'Dernière modification': None, 'Taille (Mo)': None,
    'Statut final du document': None, 'Mission': None, 'Répondant': None,
    'Date limite pour répondre': None, 'Réponse donnée le': None,
    'Écart avec la date de réponse prévue': None, 'Réponse': None,
    'Commentaire': None, 'Pièces jointes': None, 'Type de réponse': None,
    'Mission associée': None, 'Type de document': None,
}


def _row(**overrides):
    """Create a row dict with defaults + overrides."""
    r = dict(_BASE)
    r.update(overrides)
    return r


# Document 60003453 — 3 reviewer rows + separator
SAMPLE_ROWS = [
    _row(
        Identifiant='60003453', AFFAIRE='P17', PROJET='T2',
        BATIMENT='GE', PHASE='EXE', EMETTEUR='LGD',
        SPECIALITE='GOE', LOT='I003', **{'TYPE DE DOC': 'NDC'},
        ZONE='TZ', NIVEAU='TX', NUMERO='028000', INDICE='A',
        **{'Libellé du fichier': 'Hypothèses générales.pdf'},
        Version='0.2', **{'Réponse donnée le': '13/12/2023'},
        Mission='0-BET Structure', Répondant='Olga DELAURISTON',
        **{'Date limite pour répondre': '26/12/2023'},
        **{'Écart avec la date de réponse prévue': '-13'},
        Réponse='Validé avec observation',
        Commentaire='Voir la note annotée',
    ),
    _row(
        Mission='0-Bureau de Contrôle', Répondant='Marion MOTTIER',
        Réponse='Défavorable',
        **{'Date limite pour répondre': '26/12/2023'},
        **{'Réponse donnée le': '16/02/2024'},
        **{'Écart avec la date de réponse prévue': '51'},
    ),
    _row(
        Mission='0-AMO HQE', Répondant='Jean-François LABORDE',
        Réponse='Hors Mission',
    ),
    _row(),  # separator row
    # Document 60003454 — new group
    _row(
        Identifiant='60003454', AFFAIRE='P17', PROJET='T2',
        BATIMENT='GE', PHASE='EXE', EMETTEUR='LGD',
        SPECIALITE='GOE', LOT='I003', **{'TYPE DE DOC': 'NDC'},
        ZONE='TZ', NIVEAU='TX', NUMERO='028001', INDICE='A',
        Version='0.2', Mission='0-BET Structure',
        Réponse='En attente',
    ),
    # Document 60009000 — with SAS mission
    _row(
        Identifiant='60009000', AFFAIRE='P17', PROJET='T2',
        LOT='B041', NUMERO='049219', INDICE='A', Version='0.1',
        Mission='0-SAS', Répondant='Patrice BRIN',
        Réponse='Refusé',
    ),
    # Document 60009001 — MOEX mission, different INDICE
    _row(
        Identifiant='60009001', AFFAIRE='P17', PROJET='T2',
        BATIMENT='BX', LOT='B041', NUMERO='049220', INDICE='B',
        Version='0.1',
        Mission="B-Maître d'Oeuvre EXE", Répondant='Patrice BRIN',
        Réponse='Validé avec observation',
    ),
]

# Minimal valid rows for edge case tests
SINGLE_DOC_ROW = [
    _row(
        Identifiant='99999', AFFAIRE='P17', PROJET='T2',
        BATIMENT='GE', LOT='I003', NUMERO='000001', INDICE='C',
        Version='0.3', Mission='0-AMO HQE', Réponse='En attente',
    ),
]

# Rows for indice sort order testing
INDICE_TEST_ROWS = [
    _row(Identifiant='10001', AFFAIRE='P17', PROJET='T2', LOT='X', NUMERO='000001', INDICE='A', Version='0.1'),
    _row(Identifiant='10002', AFFAIRE='P17', PROJET='T2', LOT='X', NUMERO='000001', INDICE='B', Version='0.1'),
    _row(Identifiant='10003', AFFAIRE='P17', PROJET='T2', LOT='X', NUMERO='000001', INDICE='Z', Version='0.1'),
    _row(Identifiant='10004', AFFAIRE='P17', PROJET='T2', LOT='X', NUMERO='000001', INDICE='AA', Version='0.1'),
    _row(Identifiant='10005', AFFAIRE='P17', PROJET='T2', LOT='X', NUMERO='000001', INDICE='AB', Version='0.1'),
    _row(Identifiant='10006', AFFAIRE='P17', PROJET='T2', LOT='X', NUMERO='000001', Version='0.1'),  # null indice
]

# Same famille_key, different INDICE (revisions)
REVISION_ROWS = [
    _row(
        Identifiant='20001', AFFAIRE='P17', PROJET='T2', BATIMENT='GE',
        PHASE='EXE', EMETTEUR='LGD', SPECIALITE='GOE', LOT='I003',
        **{'TYPE DE DOC': 'NDC'}, ZONE='TZ', NIVEAU='TX',
        NUMERO='028000', INDICE='A', Version='0.1',
        Mission='0-AMO HQE', Réponse='En attente',
    ),
    _row(
        Identifiant='20002', AFFAIRE='P17', PROJET='T2', BATIMENT='GE',
        PHASE='EXE', EMETTEUR='LGD', SPECIALITE='GOE', LOT='I003',
        **{'TYPE DE DOC': 'NDC'}, ZONE='TZ', NIVEAU='TX',
        NUMERO='028000', INDICE='B', Version='0.1',
        Mission='0-AMO HQE', Réponse='En attente',
    ),
]

# Row with bad date for parse testing
BAD_DATE_ROW = [
    _row(
        Identifiant='30001', AFFAIRE='P17', PROJET='T2', LOT='I003',
        NUMERO='000001', INDICE='A', Version='0.1',
        **{'Date de dépôt effectif': 'not-a-date'},
        Mission='0-AMO HQE', Réponse='En attente',
    ),
]
