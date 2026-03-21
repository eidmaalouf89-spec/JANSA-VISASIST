const fr = {
  // Navigation
  nav_dashboard: 'Tableau de bord',
  nav_queue: 'File de priorit\u00e9',
  nav_workspace: 'Espace document',
  nav_suggestions: 'Suggestions',
  nav_ai_assistant: 'Assistant IA',
  nav_reports: 'Rapports',
  nav_audit: 'Audit et administration',

  // Categories
  cat_easy_win: 'GAIN FACILE',
  cat_blocked: 'BLOQU\u00c9',
  cat_fast_reject: 'REJET RAPIDE',
  cat_conflict: 'CONFLIT',
  cat_waiting: 'EN ATTENTE',
  cat_not_started: 'NON D\u00c9MARR\u00c9',

  // Score bands
  band_critical: 'CRITIQUE',
  band_high: '\u00c9LEV\u00c9',
  band_medium: 'MOYEN',
  band_low: 'BAS',

  // Actions
  action_issue_vao: '\u00c9mettre VAO',
  action_issue_ref: '\u00c9mettre REF',
  action_chase: 'Relancer les approbateurs',
  action_hold: 'Maintenir',

  // KPI labels
  kpi_total_docs: 'Documents totaux',
  kpi_pending: 'VISA en attente',
  kpi_overdue: 'En retard',
  kpi_easy_wins: 'Gains faciles',
  kpi_blocked: 'Bloqu\u00e9',
  kpi_conflicts: 'Conflits',

  // Suggested actions
  suggested_issue_visa: '\u00c9mettre VISA',
  suggested_escalate: 'Escalader',
  suggested_arbitrate: 'Arbitrer',
  suggested_chase_approvers: 'Relancer les approbateurs',
  suggested_hold: 'Maintenir',

  // Consensus types
  consensus_all_approve: 'TOUS APP',
  consensus_all_reject: 'TOUS REJ',
  consensus_mixed: 'MIXTE',
  consensus_incomplete: 'INCOMPLET',
  consensus_not_started: 'NON D\u00c9MARR\u00c9',
  consensus_all_hm: 'TOUS HM',

  // Badges
  badge_system: 'SYST\u00c8ME',
  badge_ai_m5: 'IA \u00b7 M5',
  badge_ai_m6: 'IA \u00b7 M6',
  badge_advisory: 'Consultatif',
  badge_template_m5: 'Mod\u00e8le \u00b7 M5',
  badge_m1_import: 'M1 \u00b7 Importation',

  // Lifecycle states
  lifecycle_not_started: 'Non d\u00e9marr\u00e9',
  lifecycle_waiting_responses: 'En attente de r\u00e9ponses',
  lifecycle_ready_to_issue: 'Pr\u00eat \u00e0 \u00e9mettre',
  lifecycle_ready_to_reject: 'Pr\u00eat \u00e0 rejeter',
  lifecycle_needs_arbitration: 'N\u00e9cessite un arbitrage',
  lifecycle_chronic_blocked: 'Bloqu\u00e9 chroniquement',
  lifecycle_on_hold: 'En attente',
  lifecycle_excluded: 'Exclu',

  // Empty/error states
  empty_no_pending: 'Tous les \u00e9l\u00e9ments VISA trait\u00e9s.',
  empty_no_results: 'Aucun \u00e9l\u00e9ment ne correspond \u00e0 vos filtres actuels.',
  empty_no_anomalies: 'Aucune anomalie d\u00e9tect\u00e9e.',
  empty_no_ai: 'Suggestions IA indisponibles. Affichage du mod\u00e8le.',
  empty_no_revisions: 'Premier envoi',

  // Advisory
  advisory_disclaimer: 'Consultatif uniquement',

  // Dashboard specific
  dashboard_title: 'Tableau de bord',
  dashboard_page_header: 'Aper\u00e7u du pipeline',
  dashboard_lots: 'lots',
  dashboard_documents: 'documents',
  dashboard_last_run: 'Derni\u00e8re ex\u00e9cution',
  dashboard_category_breakdown: 'R\u00e9partition par cat\u00e9gorie',
  dashboard_lot_health: 'Sant\u00e9 du lot',
  dashboard_urgent_items: '\u00c9l\u00e9ments urgents',
  dashboard_recent_anomalies: 'Anomalies r\u00e9centes',
  dashboard_recommendations: 'Recommandations',
  dashboard_pending_trend: 'Tendance en attente',
  dashboard_main_blocker: 'Principal bloqueur',
  dashboard_view_in_queue: 'Afficher dans la file de priorit\u00e9',
  dashboard_ask_ai: 'Demander \u00e0 l\'IA',
  dashboard_days_overdue: 'j en retard',
  dashboard_no_overdue: 'Pas d\'\u00e9tiquette en retard',

  // Loading / error states
  dashboard_loading: 'Chargement des donn\u00e9es du tableau de bord\u2026',
  dashboard_error: 'Impossible de charger les donn\u00e9es du tableau de bord.',

  // Anomaly action messages — keyed by AnomalyCategory
  anomaly_corrupted_date: 'Ligne signal\u00e9e pour v\u00e9rification manuelle',
  anomaly_unknown_status: 'Trait\u00e9 comme nul ; alerte g\u00e9n\u00e9r\u00e9e',
  anomaly_missing_field: 'Document trait\u00e9 avec champ nul',
  anomaly_fuzzy_match: 'Correspondance approximative appliqu\u00e9e',
  anomaly_unparseable_document: 'Class\u00e9 par estimation',
  anomaly_trailing_punctuation: 'Caract\u00e8res de fin supprim\u00e9s',
  anomaly_duplicate_row: 'Marqu\u00e9 comme doublon ; conserv\u00e9 pour v\u00e9rification',
  anomaly_missing_sheet: 'R\u00e9f\u00e9rences de feuille introuvables ; ignor\u00e9',
  anomaly_column_mismatch: 'Colonne suppl\u00e9mentaire ignor\u00e9e ; correspondance la plus proche',
  anomaly_revision_gap: '\u00c9cart de r\u00e9vision d\u00e9tect\u00e9 ; marqu\u00e9 pour investigation',

  // Recommendation template fallback text
  rec_template_issue_visa: 'Le consensus est unanime. Cet \u00e9l\u00e9ment peut \u00eatre sign\u00e9 imm\u00e9diatement selon les statuts des approbateurs.',
  rec_template_escalate: 'Cet \u00e9l\u00e9ment est bloqu\u00e9 sur plusieurs r\u00e9visions. Une escalade vers la direction de projet est recommand\u00e9e.',
  rec_template_arbitrate: 'Opinions divergentes d\u00e9tect\u00e9es parmi les approbateurs. Un arbitrage est n\u00e9cessaire pour r\u00e9soudre le d\u00e9saccord.',
  rec_template_chase_approvers: 'Des r\u00e9ponses d\'approbateurs sont encore en attente. Une relance est recommand\u00e9e pour d\u00e9bloquer cet \u00e9l\u00e9ment.',
  rec_template_hold: 'Donn\u00e9es insuffisantes pour prendre une d\u00e9cision. Maintenir cet \u00e9l\u00e9ment en attente d\'informations compl\u00e9mentaires.',

  // Common
  common_import: 'Importer',
  common_notifications: 'Notifications',
  common_search: 'Recherche',
} as const;

export type TranslationKey = keyof typeof fr;
export default fr;
