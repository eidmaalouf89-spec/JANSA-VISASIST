import type { PipelineRun } from '../mock/pipeline-run';
import { DATA_MODE, apiFetch } from './config';

// Re-export for consumers
export type { PipelineRun } from '../mock/pipeline-run';

/**
 * Fetch the current pipeline run metadata.
 * - mock: returns static mock data
 * - hybrid: fetches from static JSON in public/output/, falls back to mock
 * - api: fetches from Flask API, falls back to mock
 * - strict: fetches from API, throws on error
 */
export async function getPipelineRun(): Promise<PipelineRun> {
  if (DATA_MODE === 'mock') {
    const { pipelineRun } = await import('../mock/pipeline-run');
    return pipelineRun;
  }

  try {
    if (DATA_MODE === 'api') {
      return await apiFetch<PipelineRun>('/api/pipeline/run');
    }
    // hybrid/strict: read from static file
    const res = await fetch('/output/m1/master_dataset.json');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const meta = await res.json();
    return {
      run_id: 'run-2026-03-23',
      status: 'completed',
      started_at: '2026-03-23T00:00:00Z',
      completed_at: '2026-03-23T00:01:30Z',
      source_file: meta.source_file ?? 'unknown',
      total_rows: meta.total_docs ?? 0,
      processed_rows: meta.total_docs ?? 0,
      error_count: 0,
    };
  } catch (err) {
    if (DATA_MODE === 'strict') throw err;
    console.warn('[VISASIST:schema] pipeline/run fetch failed, falling back to mock:', err);
    const { pipelineRun } = await import('../mock/pipeline-run');
    return pipelineRun;
  }
}
