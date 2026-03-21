import type { Document } from '../types/document';
import type { Revision } from '../types/revision';
import type { AnomalyLog } from '../types/anomaly-log';
import type { Approver } from '../types/approver';

export interface ApproverCounts {
  total: number;
  responded: number;  // statut !== null && !is_pending
  blocking: number;   // is_blocking === true
  pending: number;    // is_pending === true
  hm: number;         // is_hm === true
}

/** Exact === match on doc_version_key */
export function getDocumentByKey(
  documents: Document[],
  docVersionKey: string,
): Document | undefined {
  return documents.find((d) => d.doc_version_key === docVersionKey);
}

/** r.doc_family_key === doc.doc_family_key — never startsWith */
export function getRevisionsForDocument(
  revisions: Revision[],
  doc: Document,
): Revision[] {
  return revisions.filter((r) => r.doc_family_key === doc.doc_family_key);
}

/** log.sheet === doc.source_sheet && log.row === doc.source_row */
export function getAnomalyLogsForDocument(
  logs: AnomalyLog[],
  doc: Document,
): AnomalyLog[] {
  return logs.filter((l) => l.sheet === doc.source_sheet && l.row === doc.source_row);
}

/** Single source of truth for all approver counts — components never derive inline */
export function computeApproverCounts(approvers: Approver[]): ApproverCounts {
  let total = 0;
  let responded = 0;
  let blocking = 0;
  let pending = 0;
  let hm = 0;

  for (const a of approvers) {
    total++;
    if (a.statut !== null && !a.is_pending) responded++;
    if (a.is_blocking) blocking++;
    if (a.is_pending) pending++;
    if (a.is_hm) hm++;
  }

  return { total, responded, blocking, pending, hm };
}
