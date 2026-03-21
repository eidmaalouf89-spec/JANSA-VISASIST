export interface LotHealth {
  lot: string;
  total: number;
  overdue: number;
  overdue_pct: number;
}

export interface DashboardSummary {
  pipeline_run_id: string;
  run_at: string;
  lot_count: number;
  doc_count: number;
  kpis: {
    total_docs: number;
    pending_visa: number;
    overdue: number;
    easy_wins: number;
    blocked: number;
    conflicts: number;
  };
  kpi_deltas: {
    pending_visa: number;
    overdue: number;
    easy_wins: number;
    blocked: number;
    conflicts: number;
  };
  category_counts: Record<string, number>;
  lot_health_top5: LotHealth[];
  main_blocker: string;
  urgent_item_ids: string[];
  recent_anomaly_ids: number[];
  top_recommendation_ids: string[];
}

export const dashboardSummary: DashboardSummary = {
  pipeline_run_id: 'r42',
  run_at: '2026-03-21T08:00:00Z',
  lot_count: 25,
  doc_count: 4108,
  kpis: {
    total_docs: 4108,
    pending_visa: 1247,
    overdue: 98,
    easy_wins: 142,
    blocked: 45,
    conflicts: 67,
  },
  // TODO(P2): Replace mock KPI deltas with real run-to-run delta computation
  // once multiple pipeline runs are available. Currently these are static
  // fixture values because only one pipeline iteration exists.
  kpi_deltas: {
    pending_visa: -53,
    overdue: 12,
    easy_wins: 8,
    blocked: -3,
    conflicts: 2,
  },
  category_counts: {
    EASY_WIN_APPROVE: 142,
    BLOCKED: 45,
    FAST_REJECT: 68,
    CONFLICT: 67,
    WAITING: 158,
    NOT_STARTED: 225,
  },
  lot_health_top5: [
    { lot: 'LOT 42-PLB-UTB', total: 187, overdue: 59, overdue_pct: 0.316 },
    { lot: 'LOT 14-STR',     total: 245, overdue: 64, overdue_pct: 0.261 },
    { lot: 'LOT 07-ELE',     total: 198, overdue: 39, overdue_pct: 0.197 },
    { lot: 'LOT 21-HVC',     total: 156, overdue: 23, overdue_pct: 0.147 },
    { lot: 'LOT 35-PLB',     total: 178, overdue: 20, overdue_pct: 0.112 },
  ],
  main_blocker: 'approver_hvac_01',
  urgent_item_ids: [
    '14_84::A::LOT 42-PLB-UTB',
    '21_88::H::LOT 05-ARC',
    '17_45::D::LOT 21-HVC',
    '18_67::E::LOT 35-PLB',
    '31_47::R::LOT 09-FIN',
    '32_58::S::LOT 24-MAR',
  ],
  recent_anomaly_ids: [1, 2, 3, 4, 5],
  top_recommendation_ids: ['r42-item-001', 'r42-item-002', 'r42-item-003'],
};
