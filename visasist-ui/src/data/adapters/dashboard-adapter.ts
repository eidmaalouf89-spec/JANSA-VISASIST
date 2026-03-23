import type { QueueItem } from '../../types';
import type { DashboardSummary, LotHealth } from '../../mock/dashboard-summary';

interface CategorySummaryRow {
  group_type: string;
  group_value: string;
  count: number;
}

interface PipelineReportSubset {
  reference_date: string;
  input_rows: number;
  pending_count: number;
  overdue_count: number;
  category_distribution?: Record<string, number>;
}

function getCategoryCount(
  rows: CategorySummaryRow[],
  value: string,
): number {
  const entry = rows.find(
    (r) => r.group_type === 'category' && r.group_value === value,
  );
  return entry?.count ?? 0;
}

export function adaptM3SummaryToDashboard(
  categorySummary: CategorySummaryRow[],
  pipelineReport: PipelineReportSubset,
  adaptedQueueItems: QueueItem[],
): DashboardSummary {
  // ── KPIs ──────────────────────────────────────────────────────────
  // TEMPORARY: input_rows used as approximation — confirm total_docs semantics once M4 defines KPI contracts
  const total_docs = pipelineReport.input_rows;
  const pending_visa = pipelineReport.pending_count;
  const overdue = pipelineReport.overdue_count;
  const easy_wins = getCategoryCount(categorySummary, 'EASY_WIN_APPROVE');
  const blocked = getCategoryCount(categorySummary, 'BLOCKED');
  const conflicts = getCategoryCount(categorySummary, 'CONFLICT');

  // ── Category counts ───────────────────────────────────────────────
  const category_counts: Record<string, number> = {};
  for (const row of categorySummary) {
    if (row.group_type === 'category') {
      category_counts[row.group_value] = row.count;
    }
  }

  // ── Lot health top 5 ─────────────────────────────────────────────
  const lotGroups = new Map<string, { total: number; overdueCount: number }>();
  for (const item of adaptedQueueItems) {
    const sheet = item.source_sheet;
    const group = lotGroups.get(sheet) ?? { total: 0, overdueCount: 0 };
    group.total += 1;
    if (item.is_overdue) group.overdueCount += 1;
    lotGroups.set(sheet, group);
  }
  const lot_health_top5: LotHealth[] = Array.from(lotGroups.entries())
    .map(([lot, g]) => ({
      lot,
      total: g.total,
      overdue: g.overdueCount,
      overdue_pct: g.total > 0 ? g.overdueCount / g.total : 0,
    }))
    .sort((a, b) => b.overdue_pct - a.overdue_pct)
    .slice(0, 5);

  // ── Lot count (number of distinct source_sheets) ──────────────────
  const lotCount = lotGroups.size;

  // ── Urgent item IDs: top 5 by priority_score desc ─────────────────
  const urgent_item_ids = adaptedQueueItems
    .slice(0, 5)
    .map((item) => item.doc_version_key);

  // ── Main blocker: most common canonical key in blocking_approvers ──
  const blockerCounts = new Map<string, number>();
  for (const item of adaptedQueueItems) {
    for (const key of item.blocking_approvers) {
      blockerCounts.set(key, (blockerCounts.get(key) ?? 0) + 1);
    }
  }
  let main_blocker = '';
  let maxBlockerCount = 0;
  for (const [key, count] of blockerCounts) {
    if (count > maxBlockerCount) {
      main_blocker = key;
      maxBlockerCount = count;
    }
  }

  return {
    // TEMPORARY: reference_date used as run_at approximation — replace when backend emits true pipeline execution timestamp
    pipeline_run_id: 'real',
    run_at: pipelineReport.reference_date,
    lot_count: lotCount,
    doc_count: total_docs,
    kpis: {
      total_docs,
      pending_visa,
      overdue,
      easy_wins,
      blocked,
      conflicts,
    },
    // All KPI deltas — null (M4 not available), but DashboardSummary expects numbers
    // Use 0 as neutral delta until M4 provides run-to-run comparison
    kpi_deltas: {
      pending_visa: 0,
      overdue: 0,
      easy_wins: 0,
      blocked: 0,
      conflicts: 0,
    },
    category_counts,
    lot_health_top5,
    main_blocker: main_blocker || '',
    urgent_item_ids,
    recent_anomaly_ids: [],
    top_recommendation_ids: [],
  };
}
