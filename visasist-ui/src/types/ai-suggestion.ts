import type { SuggestedAction, VisaStatus } from './index';

export interface DraftAction {
  type: 'relance' | 'synthesis' | 'note';
  content: string | null;
  template: string;
}

export interface AISuggestion {
  doc_version_key: string;
  pipeline_run_id: string;
  suggested_action: SuggestedAction;
  proposed_visa: VisaStatus | null;
  confidence: number;
  ai_available: boolean;
  explanation_text: string | null;
  explanation_template: string;
  draft_actions: DraftAction[];
  validation_failed: boolean;
  guard_reason: string | null;
}
