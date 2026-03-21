import type { QueueItem } from '../types';

/**
 * Fetch the full priority queue.
 * Currently resolves mock data. Replace internals with API calls later.
 */
export async function getQueueItems(): Promise<QueueItem[]> {
  const { queueItems } = await import('../mock/queue-items');
  return queueItems;
}
