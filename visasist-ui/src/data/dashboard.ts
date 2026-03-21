import type { DashboardSummary } from '../mock/dashboard-summary';
import type { QueueItem, AnomalyLog, AISuggestion } from '../types';
import type { PipelineRunHistoryPoint } from '../mock/pipeline-runs-history';

// Re-export types for consumers
export type { DashboardSummary } from '../mock/dashboard-summary';
export type { LotHealth } from '../mock/dashboard-summary';
export type { PipelineRunHistoryPoint } from '../mock/pipeline-runs-history';

/**
 * All data needed to render the Dashboard screen.
 * Aggregated into a single object so the screen can load once.
 */
export interface DashboardData {
  summary: DashboardSummary;
  urgentItems: QueueItem[];
  recentAnomalies: AnomalyLog[];
  recommendations: AISuggestion[];
  recommendationFallback: AISuggestion;
  trendHistory: PipelineRunHistoryPoint[];
}

/**
 * Fetch all data required by the Dashboard screen.
 * Currently resolves mock data. Replace internals with API calls later.
 */
export async function getDashboardData(): Promise<DashboardData> {
  // Dynamic import keeps the mock boundary clear.
  // When the API is ready, replace these with fetch() calls.
  const [
    { dashboardSummary },
    { queueItems },
    { anomalyLogs },
    { aiSuggestion, aiSuggestionFallback },
    { pipelineRunsHistory },
  ] = await Promise.all([
    import('../mock/dashboard-summary'),
    import('../mock/queue-items'),
    import('../mock/anomaly-logs'),
    import('../mock/ai-suggestion'),
    import('../mock/pipeline-runs-history'),
  ]);

  // Resolve urgent items from IDs
  const urgentIds = new Set(dashboardSummary.urgent_item_ids);
  const urgentItems = queueItems
    .filter((item) => urgentIds.has(item.doc_version_key))
    .sort((a, b) => {
      if (b.priority_score !== a.priority_score) return b.priority_score - a.priority_score;
      return b.days_overdue - a.days_overdue;
    })
    .slice(0, 6);

  // Resolve recent anomalies from IDs
  const anomalyIds = new Set(dashboardSummary.recent_anomaly_ids);
  const recentAnomalies = anomalyLogs.filter((log) => anomalyIds.has(log.log_id));

  // Resolve recommendations (3 cards: first two use full AI, last uses fallback)
  const recommendations = dashboardSummary.top_recommendation_ids.map((_id, i) =>
    i === 2 ? aiSuggestionFallback : aiSuggestion,
  );

  return {
    summary: dashboardSummary,
    urgentItems,
    recentAnomalies,
    recommendations,
    recommendationFallback: aiSuggestionFallback,
    trendHistory: pipelineRunsHistory,
  };
}
