const en = {
  // Navigation
  nav_dashboard: 'Dashboard',
  nav_queue: 'Queue',
  nav_workspace: 'Document Workspace',
  nav_suggestions: 'Suggestions',
  nav_ai_assistant: 'AI Assistant',
  nav_reports: 'Reports',
  nav_audit: 'Audit & Admin',

  // Categories
  cat_easy_win: 'EASY WIN',
  cat_blocked: 'BLOCKED',
  cat_fast_reject: 'FAST REJECT',
  cat_conflict: 'CONFLICT',
  cat_waiting: 'WAITING',
  cat_not_started: 'NOT STARTED',

  // Score bands
  band_critical: 'CRITICAL',
  band_high: 'HIGH',
  band_medium: 'MEDIUM',
  band_low: 'LOW',

  // Actions
  action_issue_vao: 'Issue VAO',
  action_issue_ref: 'Issue REF',
  action_chase: 'Chase approvers',
  action_hold: 'Hold',

  // KPI labels
  kpi_total_docs: 'Total Documents',
  kpi_pending: 'Pending VISA',
  kpi_overdue: 'Overdue',
  kpi_easy_wins: 'Easy Wins',
  kpi_blocked: 'Blocked',
  kpi_conflicts: 'Conflicts',

  // Suggested actions
  suggested_issue_visa: 'Issue VISA',
  suggested_escalate: 'Escalate',
  suggested_arbitrate: 'Arbitrate',
  suggested_chase_approvers: 'Chase Approvers',
  suggested_hold: 'Hold',

  // Consensus types
  consensus_all_approve: 'ALL APP',
  consensus_all_reject: 'ALL REJ',
  consensus_mixed: 'MIXED',
  consensus_incomplete: 'INCOMPLETE',
  consensus_not_started: 'NOT STARTED',
  consensus_all_hm: 'ALL HM',

  // Badges
  badge_system: 'SYSTEM',
  badge_ai_m5: 'AI \u00b7 M5',
  badge_ai_m6: 'AI \u00b7 M6',
  badge_advisory: 'Advisory',
  badge_template_m5: 'Template \u00b7 M5',
  badge_m1_import: 'M1 \u00b7 Import',

  // Lifecycle states
  lifecycle_not_started: 'Not Started',
  lifecycle_waiting_responses: 'Waiting Responses',
  lifecycle_ready_to_issue: 'Ready to Issue',
  lifecycle_ready_to_reject: 'Ready to Reject',
  lifecycle_needs_arbitration: 'Needs Arbitration',
  lifecycle_chronic_blocked: 'Chronic Blocked',
  lifecycle_on_hold: 'On Hold',
  lifecycle_excluded: 'Excluded',

  // Empty/error states
  empty_no_pending: 'All VISA items processed.',
  empty_no_results: 'No items match your current filters.',
  empty_no_anomalies: 'No anomalies detected.',
  empty_no_ai: 'AI suggestions unavailable. Showing template.',
  empty_no_revisions: 'First submission',

  // Advisory
  advisory_disclaimer: 'Advisory only',

  // Dashboard specific
  dashboard_title: 'Dashboard',
  dashboard_page_header: 'Pipeline Overview',
  dashboard_lots: 'lots',
  dashboard_documents: 'documents',
  dashboard_last_run: 'Last run',
  dashboard_category_breakdown: 'Category Breakdown',
  dashboard_lot_health: 'Lot Health',
  dashboard_urgent_items: 'Urgent Items',
  dashboard_recent_anomalies: 'Recent Anomalies',
  dashboard_recommendations: 'Recommendations',
  dashboard_pending_trend: 'Pending Trend',
  dashboard_main_blocker: 'Main blocker',
  dashboard_view_in_queue: 'View in queue',
  dashboard_ask_ai: 'Ask AI',
  dashboard_days_overdue: 'd overdue',
  dashboard_no_overdue: 'No overdue label',

  // Loading / error states
  dashboard_loading: 'Loading dashboard data\u2026',
  dashboard_error: 'Unable to load dashboard data.',

  // Anomaly action messages — keyed by AnomalyCategory
  anomaly_corrupted_date: 'Row flagged for manual review',
  anomaly_unknown_status: 'Treated as null; alert generated',
  anomaly_missing_field: 'Document proceeded with null field',
  anomaly_fuzzy_match: 'Matched with fuzzy confidence',
  anomaly_unparseable_document: 'Classified with best-guess',
  anomaly_trailing_punctuation: 'Trailing characters trimmed',
  anomaly_duplicate_row: 'Marked as duplicate; retained for review',
  anomaly_missing_sheet: 'Sheet references not found; skipped',
  anomaly_column_mismatch: 'Extra column ignored; mapped to closest match',
  anomaly_revision_gap: 'Revision gap detected; marked for investigation',

  // Recommendation template fallback text
  rec_template_issue_visa: 'Consensus is unanimous. This item can be signed immediately based on approver statuses.',
  rec_template_escalate: 'This item has been blocked across multiple revisions. Escalation to project management is recommended.',
  rec_template_arbitrate: 'Conflicting approver opinions detected. Arbitration is needed to resolve the disagreement.',
  rec_template_chase_approvers: 'Approver responses are still pending. A follow-up is recommended to unblock this item.',
  rec_template_hold: 'Insufficient data to make a decision. Hold this item until further information is available.',

  // Common
  common_import: 'Import',
  common_notifications: 'Notifications',
  common_search: 'Search',

  // Queue P2 — Filters
  filter_quick: 'Quick',
  filter_category: 'Category',
  filter_lot: 'Lot',
  filter_lot_search_placeholder: 'Search lots\u2026',
  filter_score_band: 'Score band',
  filter_approver: 'Approver',
  filter_reset: 'Reset',
  filter_active_count: '{n} active',

  // Queue P2 — Bulk actions
  bulk_selected: 'selected',
  bulk_issue: 'Issue VISA',
  bulk_chase: 'Chase approvers',
  bulk_export: 'Export CSV',
  bulk_flag: 'Flag',
  bulk_clear: 'Clear',

  // Queue P2 — Column headers
  col_document: 'Document',
  col_lot: 'Lot',
  col_category: 'Category',
  col_score: 'Score',
  col_deadline: 'Deadline',
  col_overdue: 'Overdue',
  col_consensus: 'Consensus',
  col_missing: 'Missing',
  col_rev: 'Rev',
  col_action: 'Action',

  // Queue P2 — Detail panel
  why_rank_label: 'Why this rank',
  detail_responded: 'Responded',
  detail_blocking: 'Blocking',
  detail_missing: 'Missing',
  open_workspace: 'Open workspace',
} as const;

export type TranslationKey = keyof typeof en;
export default en;
