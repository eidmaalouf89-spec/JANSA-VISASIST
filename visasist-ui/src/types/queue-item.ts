import type { Category, ConsensusType, ScoreBand, SuggestedAction, RowQuality, DuplicateFlag } from './index';

export interface QueueItem {
  // Traceability
  doc_version_key: string;
  doc_family_key: string;
  row_id: string;
  source_sheet: string;
  source_row: number;

  // Document identity
  document: string;
  document_raw: string;
  titre: string | null;
  lot: string | null;
  type_doc: string | null;
  niv: string | null;
  ind: string | null;
  ind_sort_order: number;

  // Dates
  date_diffusion: string | null;
  date_contractuelle_visa: string | null;
  days_since_diffusion: number | null;
  days_until_deadline: number | null;
  days_overdue: number;
  is_overdue: boolean;

  // M3 scoring & categorisation
  priority_score: number;
  score_band: ScoreBand;
  category: Category;
  consensus_type: ConsensusType;
  missing_approvers: string[];
  blocking_approvers: string[];

  // M5 suggestion
  suggested_action: SuggestedAction;

  // M2 revision info
  revision_count: number;
  is_latest: true;
  is_cross_lot: boolean;

  // Quality
  row_quality: RowQuality;
  duplicate_flag: DuplicateFlag;
}
