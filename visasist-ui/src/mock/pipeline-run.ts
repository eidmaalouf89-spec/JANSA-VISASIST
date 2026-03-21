export interface PipelineRun {
  run_id: string;
  run_at: string;
  status: 'running' | 'completed' | 'failed';
  lot_count: number;
  doc_count: number;
}

export const pipelineRun: PipelineRun = {
  run_id: 'r42',
  run_at: '2026-03-21T08:00:00Z',
  status: 'completed',
  lot_count: 25,
  doc_count: 4108,
};
