export interface RevisionApproverSummary {
  canonical_key: string;
  display_name: string;
  statut: string | null;
}

export interface Revision {
  doc_version_key: string;
  ind: string | null;
  ind_sort_order: number;
  date_diffusion: string | null;
  date_contractuelle_visa: string | null;
  visa_global: string | null;
  is_latest: boolean;
  previous_version_key: string | null;
  approver_summary: RevisionApproverSummary[];
}
