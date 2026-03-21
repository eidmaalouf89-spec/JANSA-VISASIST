import type { Revision } from '../types';

export const revisions: Revision[] = [
  {
    doc_version_key: '14_84::null::LOT 42-PLB-UTB',
    ind: null,
    ind_sort_order: 0,
    date_diffusion: '2026-02-15T08:00:00Z',
    date_contractuelle_visa: '2026-02-22T23:59:59Z',
    visa_global: null,
    is_latest: false,
    previous_version_key: null,
    approver_summary: [
      {
        canonical_key: 'approver_elec_01',
        display_name: 'Jean Dupont (VSO)',
        statut: null,
      },
      {
        canonical_key: 'approver_elec_02',
        display_name: 'Marie Martin (VAO)',
        statut: null,
      },
    ],
  },
  {
    doc_version_key: '14_84::A::LOT 42-PLB-UTB',
    ind: 'A',
    ind_sort_order: 1,
    date_diffusion: '2026-03-01T10:00:00Z',
    date_contractuelle_visa: '2026-03-08T23:59:59Z',
    visa_global: null,
    is_latest: false,
    previous_version_key: '14_84::null::LOT 42-PLB-UTB',
    approver_summary: [
      {
        canonical_key: 'approver_elec_01',
        display_name: 'Jean Dupont (VSO)',
        statut: 'VSO',
      },
      {
        canonical_key: 'approver_elec_02',
        display_name: 'Marie Martin (VAO)',
        statut: 'VAO',
      },
      {
        canonical_key: 'approver_elec_03',
        display_name: 'Pierre Bernard (REF)',
        statut: null,
      },
    ],
  },
  {
    doc_version_key: '14_84::A::LOT 42-PLB-UTB',
    ind: 'A',
    ind_sort_order: 1,
    date_diffusion: '2026-03-01T10:00:00Z',
    date_contractuelle_visa: '2026-03-08T23:59:59Z',
    visa_global: 'VAO',
    is_latest: true,
    previous_version_key: '14_84::A::LOT 42-PLB-UTB',
    approver_summary: [
      {
        canonical_key: 'approver_elec_01',
        display_name: 'Jean Dupont (VSO)',
        statut: 'VSO',
      },
      {
        canonical_key: 'approver_elec_02',
        display_name: 'Marie Martin (VAO)',
        statut: 'VAO',
      },
      {
        canonical_key: 'approver_elec_03',
        display_name: 'Pierre Bernard (REF)',
        statut: 'REF',
      },
    ],
  },
];
