import type { Category, ConsensusType, ScoreBand, SuggestedAction, RowQuality, DuplicateFlag, LifecycleState, VisaStatus } from './index';
import type { Approver } from './approver';
import type { AISuggestion } from './ai-suggestion';

export interface ScoreComponents {
  overdue_points: number;
  deadline_points: number;
  completeness_points: number;
  revision_points: number;
  deadline_penalty: number;
}

export interface Document {
  // Traceability (from QueueItem)
  doc_version_key: string;
  doc_family_key: string;
  row_id: string;
  source_sheet: string;
  source_row: number;

  // Document identity (from QueueItem)
  document: string;
  document_raw: string;
  titre: string | null;
  lot: string | null;
  type_doc: string | null;
  niv: string | null;
  ind: string | null;
  ind_sort_order: number;

  // Dates (from QueueItem)
  date_diffusion: string | null;
  date_contractuelle_visa: string | null;
  days_since_diffusion: number | null;
  days_until_deadline: number | null;
  days_overdue: number;
  is_overdue: boolean;

  // M3 scoring & categorisation (from QueueItem)
  priority_score: number;
  score_band: ScoreBand;
  category: Category;
  consensus_type: ConsensusType;
  missing_approvers: string[];
  blocking_approvers: string[];

  // M5 suggestion (from QueueItem)
  suggested_action: SuggestedAction;

  // M2 revision info (from QueueItem)
  revision_count: number;
  is_latest: true;
  is_cross_lot: boolean;

  // Quality (from QueueItem)
  row_quality: RowQuality;
  duplicate_flag: DuplicateFlag;

  // Extended identity
  zone: string | null;
  n_doc: string | null;
  type_format: string | null;
  ancien: string | null;
  n_bdx: string | null;
  date_reception: string | null;
  non_recu_papier: string | null;
  observations: string | null;
  visa_global: VisaStatus | null;
  visa_global_raw: string | null;

  // M2 extended
  lifecycle_state: LifecycleState;
  cross_lot_sheets: string[] | null;
  previous_version_key: string | null;

  // Full approver list
  approvers: Approver[];
  assigned_approvers: string[];

  // Score breakdown (M3)
  score_components: ScoreComponents;

  // M5 AI suggestion
  ai_suggestion: AISuggestion | null;
}
