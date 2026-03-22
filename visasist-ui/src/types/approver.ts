export interface Approver {
  canonical_key: string;
  display_name: string;
  is_assigned: boolean;
  statut: 'VSO' | 'VAO' | 'REF' | 'HM' | 'SUS' | 'DEF' | 'FAV' | null;
  statut_raw: string | null;
  date: string | null;
  numero_visa: string | null;
  is_blocking: boolean;
  is_pending: boolean;
  is_hm: boolean;
}
