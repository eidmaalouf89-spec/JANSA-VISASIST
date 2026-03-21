import type { PipelineRun } from '../mock/pipeline-run';

// Re-export for consumers
export type { PipelineRun } from '../mock/pipeline-run';

/**
 * Fetch the current pipeline run metadata.
 * Currently resolves mock data. Replace internals with API calls later.
 */
export async function getPipelineRun(): Promise<PipelineRun> {
  const { pipelineRun } = await import('../mock/pipeline-run');
  return pipelineRun;
}
