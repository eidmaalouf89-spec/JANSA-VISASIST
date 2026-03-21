# JANSA VISASIST

Pipeline de traitement des VISA documents de construction.
Transforme un GrandFichier Excel brut en dataset structuré, priorisé et analysé.

## Stack
- Python 3.10+
- pandas, openpyxl, rapidfuzz

## Installation
```bash
pip install -r jansa_visasist/requirements.txt
```

## Lancement pipeline complet (M1 → M2 → M3 → M4)
```bash
python -m jansa_visasist.main data/GrandFichier_1.xlsx
```

## Outputs produits
| Fichier | Description |
|---|---|
| output/master_dataset.json | Dataset maître normalisé (M1+M2) |
| output/priority_queue.json | File de priorité MOEX (M3) |
| output/analysis_report.json | Analyses globales G1–G4 + lifecycle_state (M4) |
| output/import_log.json | Log de tous les anomalies d'import |
| output/header_mapping_report.json | Rapport de mapping des colonnes par feuille |

## Modules
| Module | Rôle |
|---|---|
| M1 | Ingestion & normalisation (25 feuilles → dataset plat) |
| M2 | Modèle de données & chaînes de révisions |
| M3 | Moteur de priorisation (score 0–100, 6 catégories) |
| M4 | Moteur d'analyse (G1–G4, lifecycle_state) |
| M5 | Moteur de suggestion (déterministe + IA optionnelle) |
| M6 | Chatbot contraint (lecture seule) |
| M7 | Workflow batch (sessions MOEX persistées) |

## Tests
```bash
pytest jansa_visasist/tests/
```

## Spec de référence
- Phase 1 (M1–M3) : JANSA_VISASIST_V2.2_Module_Specs_M1_M2_M3.docx
- Phase 2 (M4–M7) : JANSA_VISASIST_Phase2_Spec_M4_M5_M6_M7def.docx
