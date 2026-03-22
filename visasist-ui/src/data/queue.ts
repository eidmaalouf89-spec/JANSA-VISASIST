import type { QueueItem } from '../types';
import { DATA_MODE, IntegrationError, apiFetch } from './config';
import { validateQueueRow } from './validators/queue-validator';
import { adaptM3Row } from './adapters/queue-adapter';

/**
 * Fetch the full priority queue.
 * In mock mode: returns mock data.
 * In hybrid/strict mode: fetches real M3 output, validates, and adapts.
 */
export async function getQueueItems(): Promise<QueueItem[]> {
  // Mock mode — no fetches
  if (DATA_MODE === 'mock') {
    const { queueItems } = await import('../mock/queue-items');
    return queueItems;
  }

  // Fetch real M3 priority queue
  let rawRows: unknown[];
  try {
    if (DATA_MODE === 'api') {
      rawRows = await apiFetch<unknown[]>('/api/queue');
    } else {
      const response = await fetch('/output/m3/m3_priority_queue.json');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      rawRows = await response.json();
    }
  } catch (err) {
    if (DATA_MODE === 'hybrid' || DATA_MODE === 'api') {
      console.warn('[VISASIST:schema] queue fetch failed, falling back to mock:', err);
      const { queueItems } = await import('../mock/queue-items');
      return queueItems;
    }
    throw new IntegrationError(
      `Failed to fetch queue data: ${err}`,
      '/output/m3/m3_priority_queue.json',
    );
  }

  if (!Array.isArray(rawRows)) {
    if (DATA_MODE === 'hybrid' || DATA_MODE === 'api') {
      console.warn('[VISASIST:schema] queue response is not an array, falling back to mock');
      const { queueItems } = await import('../mock/queue-items');
      return queueItems;
    }
    throw new IntegrationError(
      'Queue response is not an array',
      '/output/m3/m3_priority_queue.json',
    );
  }

  // Validate + adapt each row
  const result: QueueItem[] = [];
  let invalidCount = 0;

  for (const raw of rawRows) {
    const validation = validateQueueRow(raw);
    if (!validation.valid) {
      invalidCount += 1;
      const rowId = typeof raw === 'object' && raw !== null
        ? (raw as Record<string, unknown>).row_id ?? 'unknown'
        : 'unknown';
      console.warn(
        `[VISASIST:schema] queue row ${rowId}: ${validation.reason} — skipped`,
      );
      continue;
    }
    result.push(adaptM3Row(raw as Record<string, unknown>));
  }

  // Schema drift check (strict mode)
  if (
    invalidCount > 0 &&
    invalidCount / rawRows.length > 0.1 &&
    DATA_MODE === 'strict'
  ) {
    throw new IntegrationError(
      `Schema drift: >10% of queue rows failed validation (${invalidCount}/${rawRows.length})`,
      '/output/m3/m3_priority_queue.json',
    );
  }

  // Empty result fallback
  if (result.length === 0) {
    if (DATA_MODE === 'hybrid' || DATA_MODE === 'api') {
      console.warn('[VISASIST:schema] no valid queue rows after validation, falling back to mock');
      const { queueItems } = await import('../mock/queue-items');
      return queueItems;
    }
    throw new IntegrationError(
      'No valid queue rows after validation',
      '/output/m3/m3_priority_queue.json',
    );
  }

  return result;
}
