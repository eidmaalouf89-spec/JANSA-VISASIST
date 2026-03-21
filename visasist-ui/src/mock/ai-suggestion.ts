import type { AISuggestion } from '../types';

export const aiSuggestion: AISuggestion = {
  doc_version_key: '14_84::A::LOT 42-PLB-UTB',
  pipeline_run_id: 'r42',
  suggested_action: 'ISSUE_VISA',
  proposed_visa: 'VAO',
  confidence: 0.92,
  ai_available: true,
  explanation_text:
    'Tous les approbateurs ont donn\u00e9 leur avis positif (VSO, VAO, REF). Le consensus est unanime (ALL_APPROVE). ' +
    'Le document est en retard de 5 jours mais peut \u00eatre sign\u00e9 imm\u00e9diatement. Recommandation: \u00e9mettre le visa sans attendre.',
  explanation_template: 'rec_template_issue_visa',
  draft_actions: [
    {
      type: 'relance',
      content: null,
      template: 'visa_approved_notification',
    },
    {
      type: 'synthesis',
      content:
        'Visa approuv\u00e9 suite \u00e0 consensus positif de tous les approbateurs. Le document respecte les normes en vigueur.',
      template: 'standard_synthesis',
    },
    {
      type: 'note',
      content:
        'Document install\u00e9 avec approbations compl\u00e8tes. Peut proc\u00e9der \u00e0 la signature formelle.',
      template: 'archive_note',
    },
  ],
  validation_failed: false,
  guard_reason: null,
};

export const aiSuggestionFallback: AISuggestion = {
  doc_version_key: '21_88::H::LOT 05-ARC',
  pipeline_run_id: 'r42',
  suggested_action: 'ARBITRATE',
  proposed_visa: null,
  confidence: 0.0,
  ai_available: false,
  explanation_text: null,
  explanation_template: 'rec_template_arbitrate',
  draft_actions: [
    {
      type: 'relance',
      content: null,
      template: 'escalation_to_human',
    },
  ],
  validation_failed: false,
  guard_reason: 'AI service unavailable; manual review required',
};
