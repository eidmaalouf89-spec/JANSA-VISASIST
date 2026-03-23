import type { ValidationResult } from './queue-validator';

export type { ValidationResult };

export function validateDocumentRow(raw: unknown): ValidationResult {
  if (typeof raw !== 'object' || raw === null)
    return { valid: false, reason: 'row is not an object' };
  const r = raw as Record<string, unknown>;

  if (typeof r.doc_version_key !== 'string' || !r.doc_version_key)
    return { valid: false, reason: 'missing doc_version_key' };
  if (typeof r.doc_family_key !== 'string' || !r.doc_family_key)
    return { valid: false, reason: 'missing doc_family_key' };
  if (typeof r.source_sheet !== 'string' || !r.source_sheet)
    return { valid: false, reason: 'missing source_sheet' };
  if (typeof r.source_row !== 'number')
    return { valid: false, reason: 'source_row must be number' };
  if (typeof r.document !== 'string' || !r.document)
    return { valid: false, reason: 'missing document' };

  return { valid: true };
}
