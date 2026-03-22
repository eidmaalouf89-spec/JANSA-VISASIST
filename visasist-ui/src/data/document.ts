import type { Document } from '../types';
import { DATA_MODE, IntegrationError, apiFetch } from './config';
import { validateDocumentRow } from './validators/document-validator';
import { adaptM2RowToDocument } from './adapters/document-adapter';

/**
 * Fetch a single document by its version key.
 * In mock mode: returns mock data.
 * In hybrid/strict mode: fetches real M2 enriched dataset, validates, and adapts.
 */
export async function getDocument(docVersionKey: string): Promise<Document | undefined> {
  // Mock mode — no fetches
  if (DATA_MODE === 'mock') {
    const { documentDetail } = await import('../mock/document-detail');
    return documentDetail;
  }

  // Fetch real M2 enriched dataset
  let rawRows: unknown[];
  try {
    if (DATA_MODE === 'api') {
      // API mode: fetch individual document from Flask
      try {
        const row = await apiFetch<Record<string, unknown>>(
          `/api/documents/${encodeURIComponent(docVersionKey)}`,
        );
        rawRows = [row]; // Wrap single result for uniform handling below
      } catch (apiErr) {
        // 404 = document not found — not an error
        if (apiErr instanceof IntegrationError && apiErr.message.includes('404')) {
          return undefined;
        }
        throw apiErr;
      }
    } else {
      const response = await fetch('/output/m2/enriched_master_dataset.json');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      rawRows = await response.json();
    }
  } catch (err) {
    if (DATA_MODE === 'hybrid' || DATA_MODE === 'api') {
      console.warn('[VISASIST:schema] document fetch failed, attempting mock fallback:', err);
      // Key-aware fallback: only return mock if key matches mock document
      const { documentDetail } = await import('../mock/document-detail');
      if (docVersionKey === documentDetail.doc_version_key) return documentDetail;
      return undefined;
    }
    throw new IntegrationError(
      `Failed to fetch document data: ${err}`,
      '/output/m2/enriched_master_dataset.json',
    );
  }

  if (!Array.isArray(rawRows)) {
    if (DATA_MODE === 'hybrid' || DATA_MODE === 'api') {
      console.warn('[VISASIST:schema] document response is not an array');
      const { documentDetail } = await import('../mock/document-detail');
      if (docVersionKey === documentDetail.doc_version_key) return documentDetail;
      return undefined;
    }
    throw new IntegrationError(
      'Document response is not an array',
      '/output/m2/enriched_master_dataset.json',
    );
  }

  // Find row by exact doc_version_key match
  const raw = rawRows.find(
    (row) =>
      typeof row === 'object' &&
      row !== null &&
      (row as Record<string, unknown>).doc_version_key === docVersionKey,
  );

  if (!raw) {
    // Not found — triggers not-found UI in WorkspaceScreen
    return undefined;
  }

  // Validate
  const validation = validateDocumentRow(raw);
  if (!validation.valid) {
    console.warn(
      `[VISASIST:schema] document ${docVersionKey}: ${validation.reason}`,
    );
    if (DATA_MODE === 'hybrid' || DATA_MODE === 'api') {
      const { documentDetail } = await import('../mock/document-detail');
      if (docVersionKey === documentDetail.doc_version_key) return documentDetail;
      return undefined;
    }
    throw new IntegrationError(
      `Document validation failed: ${validation.reason}`,
      '/output/m2/enriched_master_dataset.json',
    );
  }

  // HYBRID P3.5: revision chain from mock — replace when M2 emits revision chain file
  const { revisions } = await import('../mock/revisions');
  // HYBRID P3.5: anomaly logs from mock — replace when import_log.json is populated
  const { anomalyLogs } = await import('../mock/anomaly-logs');

  return adaptM2RowToDocument(raw as Record<string, unknown>, revisions, anomalyLogs);
}
