import type { QueueItem } from '../../types';

// TEMPORARY: remove score_band derivation when M3 emits this field directly
function deriveScoreBand(score: number): QueueItem['score_band'] {
  if (score >= 80) return 'CRITICAL';
  if (score >= 60) return 'HIGH';
  if (score >= 40) return 'MEDIUM';
  return 'LOW';
}

// TEMPORARY: remove suggested_action derivation when M3 emits this field directly
function deriveSuggestedAction(category: string): QueueItem['suggested_action'] {
  switch (category) {
    case 'EASY_WIN_APPROVE': return 'ISSUE_VISA';
    case 'BLOCKED':          return 'ESCALATE';
    case 'FAST_REJECT':      return 'ISSUE_VISA';
    case 'CONFLICT':         return 'ARBITRATE';
    case 'WAITING':          return 'CHASE_APPROVERS';
    default:                 return 'HOLD';
  }
}

export function adaptM3Row(r: Record<string, unknown>): QueueItem {
  const score = r.priority_score as number;
  const category = r.category as QueueItem['category'];
  return {
    // Traceability
    doc_version_key:      r.doc_version_key as string,
    doc_family_key:       r.doc_family_key as string,
    row_id:               r.row_id as string,
    source_sheet:         r.source_sheet as string,
    source_row:           r.source_row as number,
    // Document identity
    document:             r.document as string,
    document_raw:         r.document_raw as string,
    titre:                (r.titre as string | null) ?? null,
    lot:                  (r.lot as string | null) ?? null,
    type_doc:             (r.type_doc as string | null) ?? null,
    niv:                  (r.niv as string | null) ?? null,
    ind:                  (r.ind as string | null) ?? null,
    ind_sort_order:       r.ind_sort_order as number,
    // Dates
    date_diffusion:       (r.date_diffusion as string | null) ?? null,
    date_contractuelle_visa: (r.date_contractuelle_visa as string | null) ?? null,
    days_since_diffusion: (r.days_since_diffusion as number | null) ?? null,
    days_until_deadline:  (r.days_until_deadline as number | null) ?? null,
    days_overdue:         r.days_overdue as number,
    is_overdue:           r.is_overdue as boolean,
    // M3 scoring — used directly from backend
    priority_score:       score,
    // TEMPORARY: remove when M3 emits score_band directly
    score_band:           deriveScoreBand(score),
    category,
    consensus_type:       r.consensus_type as QueueItem['consensus_type'],
    missing_approvers:    r.missing_approvers as string[],
    blocking_approvers:   r.blocking_approvers as string[],
    // TEMPORARY: remove when M3 emits suggested_action directly
    suggested_action:     deriveSuggestedAction(category as string),
    // M2 revision info — M3 queue only contains latest versions
    revision_count:       r.revision_count as number,
    is_latest:            true,
    is_cross_lot:         r.is_cross_lot as boolean,
    // Quality
    row_quality:          (r.row_quality as QueueItem['row_quality']) ?? 'OK',
    duplicate_flag:       (r.duplicate_flag as QueueItem['duplicate_flag']) ?? 'UNIQUE',
  };
}
