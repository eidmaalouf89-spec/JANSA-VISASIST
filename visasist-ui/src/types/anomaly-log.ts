export type AnomalySeverity = 'ERROR' | 'WARNING' | 'INFO';

export type AnomalyCategory =
  | 'corrupted_date'
  | 'unknown_status'
  | 'missing_field'
  | 'fuzzy_match'
  | 'unparseable_document'
  | 'trailing_punctuation'
  | 'duplicate_row'
  | 'missing_sheet'
  | 'column_mismatch'
  | 'revision_gap';

export interface AnomalyLog {
  log_id: number;
  sheet: string;
  row: number;
  column: string | null;
  severity: AnomalySeverity;
  category: AnomalyCategory;
  raw_value: string | null;
  action_taken: string;
  confidence: number | null;
}
