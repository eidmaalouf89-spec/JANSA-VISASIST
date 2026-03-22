export interface PipelineRunHistoryPoint {
  run_id: string;
  run_at: string;
  pending_count: number;
}

export const pipelineRunsHistory: PipelineRunHistoryPoint[] = [
  {
    run_id: 'run-2026-03-23',
    run_at: '2026-03-23T00:00:00Z',
    pending_count: 1852,
  },
];
