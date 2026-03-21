export interface PipelineRunHistoryPoint {
  run_id: string;
  run_at: string;
  pending_count: number;
}

export const pipelineRunsHistory: PipelineRunHistoryPoint[] = [
  {
    run_id: 'r36',
    run_at: '2026-03-07T08:00:00Z',
    pending_count: 1345,
  },
  {
    run_id: 'r37',
    run_at: '2026-03-08T08:00:00Z',
    pending_count: 1328,
  },
  {
    run_id: 'r38',
    run_at: '2026-03-09T08:00:00Z',
    pending_count: 1312,
  },
  {
    run_id: 'r39',
    run_at: '2026-03-10T08:00:00Z',
    pending_count: 1289,
  },
  {
    run_id: 'r40',
    run_at: '2026-03-15T08:00:00Z',
    pending_count: 1274,
  },
  {
    run_id: 'r41',
    run_at: '2026-03-18T08:00:00Z',
    pending_count: 1256,
  },
  {
    run_id: 'r42',
    run_at: '2026-03-21T08:00:00Z',
    pending_count: 1247,
  },
  {
    run_id: 'r43',
    run_at: '2026-03-22T08:00:00Z',
    pending_count: 1235,
  },
];
