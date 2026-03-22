import type { AISuggestion } from '../types';

export const aiSuggestion: AISuggestion = {
  doc_version_key: 'P17_T2_GE_EXE_LGD_GOE_I003_NDC_TZ_TX_028000::A::2',
  pipeline_run_id: 'run-2026-03-23',
  suggested_action: 'HOLD',
  proposed_visa: null, confidence: 0.0, ai_available: false,
  explanation_text: null, explanation_template: '', draft_actions: [],
  validation_failed: false, guard_reason: 'M5 AI module not yet connected',
};

export const aiSuggestionFallback: AISuggestion = {
  doc_version_key: 'P17_T2_GE_EXE_LGD_GOE_I003_NDC_TZ_TX_028000::A::2',
  pipeline_run_id: 'run-2026-03-23',
  suggested_action: 'HOLD', proposed_visa: null, confidence: 0.0,
  ai_available: false, explanation_text: null, explanation_template: '',
  draft_actions: [], validation_failed: false,
  guard_reason: 'M5 AI module not yet connected',
};
