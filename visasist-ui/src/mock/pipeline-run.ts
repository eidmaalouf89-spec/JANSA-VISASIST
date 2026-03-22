export interface PipelineRun {
  run_id: string; status: string; started_at: string; completed_at: string;
  source_file: string; total_rows: number; processed_rows: number; error_count: number;
}

export const pipelineRun: PipelineRun = {
  run_id: 'run-2026-03-23',
  status: 'completed',
  started_at: '2026-03-23T00:00:00Z',
  completed_at: '2026-03-23T00:01:30Z',
  source_file: '17&CO Tranche 2 du 23 mars 2026 07_45.xlsx',
  total_rows: 2392,
  processed_rows: 2392,
  error_count: 0,
};
