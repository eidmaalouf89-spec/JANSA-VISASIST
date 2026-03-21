import type { Document } from '../types';

/**
 * Fetch a single document by its version key.
 * Currently resolves mock data. Replace internals with API calls later.
 */
export async function getDocument(_docVersionKey: string): Promise<Document> {
  const { documentDetail } = await import('../mock/document-detail');
  return documentDetail;
}
