const VALID_CATEGORIES = [
  'EASY_WIN_APPROVE', 'BLOCKED', 'FAST_REJECT',
  'CONFLICT', 'WAITING', 'NOT_STARTED',
] as const;

const VALID_CONSENSUS = [
  'ALL_APPROVE', 'ALL_REJECT', 'MIXED',
  'INCOMPLETE', 'NOT_STARTED', 'ALL_HM',
] as const;

export type ValidationResult =
  | { valid: true }
  | { valid: false; reason: string };

export function validateQueueRow(raw: unknown): ValidationResult {
  if (typeof raw !== 'object' || raw === null) {
    return { valid: false, reason: 'row is not an object' };
  }
  const r = raw as Record<string, unknown>;

  if (typeof r.doc_version_key !== 'string' || !r.doc_version_key)
    return { valid: false, reason: 'missing doc_version_key' };
  if (typeof r.source_sheet !== 'string' || !r.source_sheet)
    return { valid: false, reason: 'missing source_sheet' };
  if (typeof r.source_row !== 'number')
    return { valid: false, reason: 'source_row must be number' };
  if (!VALID_CATEGORIES.includes(r.category as never))
    return { valid: false, reason: `invalid category: ${r.category}` };
  if (typeof r.priority_score !== 'number')
    return { valid: false, reason: 'priority_score must be number' };
  if (!VALID_CONSENSUS.includes(r.consensus_type as never))
    return { valid: false, reason: `invalid consensus_type: ${r.consensus_type}` };
  if (!Array.isArray(r.missing_approvers))
    return { valid: false, reason: 'missing_approvers must be array' };
  if (!Array.isArray(r.blocking_approvers))
    return { valid: false, reason: 'blocking_approvers must be array' };

  return { valid: true };
}
