import type { Document } from '../types';
import type { ScoreComponents } from '../types/document';

export const documentDetail: Document = {
  // Traceability
  doc_version_key: '14_84::A::LOT 42-PLB-UTB',
  doc_family_key: '14_84',
  row_id: 'row_001',
  source_sheet: 'Sheet1',
  source_row: 2,

  // Document identity
  document: 'D001 - Installation électrique',
  document_raw: 'D001 - Installation electrique',
  titre: 'Plan électrique général - Version A',
  lot: 'LOT 42-PLB-UTB',
  type_doc: 'Technical Drawing',
  niv: 'Level A',
  ind: 'A',
  ind_sort_order: 1,

  // Dates
  date_diffusion: '2026-03-01T10:00:00Z',
  date_contractuelle_visa: '2026-03-08T23:59:59Z',
  days_since_diffusion: 20,
  days_until_deadline: -5,
  days_overdue: 5,
  is_overdue: true,

  // M3 scoring & categorisation
  priority_score: 95.5,
  score_band: 'CRITICAL',
  category: 'EASY_WIN_APPROVE',
  consensus_type: 'ALL_APPROVE',
  missing_approvers: [],
  blocking_approvers: [],

  // M5 suggestion
  suggested_action: 'ISSUE_VISA',

  // M2 revision info
  revision_count: 1,
  is_latest: true,
  is_cross_lot: false,

  // Quality
  row_quality: 'OK',
  duplicate_flag: 'UNIQUE',

  // Extended identity
  zone: 'Zone A - Électricité générale',
  n_doc: '14-84-001-A',
  type_format: 'DWG',
  ancien: 'D001-v0',
  n_bdx: 'BX-2026-001',
  date_reception: '2026-02-28T14:30:00Z',
  non_recu_papier: null,
  observations: 'Document bien structuré, conforme aux normes NF C 15-100',
  visa_global: 'VAO',
  visa_global_raw: 'VAO - Approuvé',

  // M2 extended
  lifecycle_state: 'READY_TO_ISSUE',
  cross_lot_sheets: null,
  previous_version_key: '14_84::null::LOT 42-PLB-UTB',

  // Full approver list
  approvers: [
    {
      canonical_key: 'approver_elec_01',
      display_name: 'Jean Dupont (VSO)',
      is_assigned: true,
      statut: 'VSO',
      statut_raw: 'Visa Système Ouvrages',
      date: '2026-03-02T09:15:00Z',
      numero_visa: 'VISA-2026-001',
      is_blocking: false,
      is_pending: false,
      is_hm: false,
    },
    {
      canonical_key: 'approver_elec_02',
      display_name: 'Marie Martin (VAO)',
      is_assigned: true,
      statut: 'VAO',
      statut_raw: 'Visa Activités Opérationnelles',
      date: '2026-03-03T11:45:00Z',
      numero_visa: 'VISA-2026-002',
      is_blocking: false,
      is_pending: false,
      is_hm: false,
    },
    {
      canonical_key: 'approver_elec_03',
      display_name: 'Pierre Bernard (REF)',
      is_assigned: true,
      statut: 'REF',
      statut_raw: 'Référent',
      date: '2026-03-04T14:20:00Z',
      numero_visa: 'VISA-2026-003',
      is_blocking: false,
      is_pending: false,
      is_hm: false,
    },
    {
      canonical_key: 'approver_elec_04',
      display_name: 'Luc Devereaux (HM)',
      is_assigned: true,
      statut: 'HM',
      statut_raw: 'Homme de Métier',
      date: null,
      numero_visa: null,
      is_blocking: false,
      is_pending: true,
      is_hm: true,
    },
    {
      canonical_key: 'approver_elec_05',
      display_name: 'Sophie Laurent (null)',
      is_assigned: false,
      statut: null,
      statut_raw: null,
      date: null,
      numero_visa: null,
      is_blocking: false,
      is_pending: false,
      is_hm: false,
    },
  ],
  assigned_approvers: [
    'approver_elec_01',
    'approver_elec_02',
    'approver_elec_03',
    'approver_elec_04',
  ],

  // Score breakdown
  score_components: {
    overdue_points: 35.5,
    deadline_points: 20.0,
    completeness_points: 25.0,
    revision_points: 15.0,
    deadline_penalty: 0,
  } as ScoreComponents,

  // M5 AI suggestion
  ai_suggestion: {
    doc_version_key: '14_84::A::LOT 42-PLB-UTB',
    pipeline_run_id: 'r42',
    suggested_action: 'ISSUE_VISA',
    proposed_visa: 'VAO',
    confidence: 0.92,
    ai_available: true,
    explanation_text:
      'Tous les approbateurs ont donné leur avis positif (VSO, VAO, REF). Le consensus est unanime (ALL_APPROVE). ' +
      'Le document est en retard de 5 jours mais peut être signé immédiatement. Recommandation: émettre le visa sans attendre.',
    explanation_template: 'default_approve',
    draft_actions: [
      {
        type: 'relance',
        content: null,
        template: 'visa_approved_notification',
      },
      {
        type: 'synthesis',
        content:
          'Visa approuvé suite à consensus positif de tous les approbateurs. Le document respecte les normes en vigueur.',
        template: 'standard_synthesis',
      },
    ],
    validation_failed: false,
    guard_reason: null,
  },
};
