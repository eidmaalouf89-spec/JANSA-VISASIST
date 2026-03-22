import type { DashboardSummary } from '../mock/dashboard-summary';
import type { QueueItem, AnomalyLog, AISuggestion } from '../types';
import type { PipelineRunHistoryPoint } from '../mock/pipeline-runs-history';
import { DATA_MODE, IntegrationError, apiFetch } from './config';
import { getQueueItems } from './queue';
import { adaptM3SummaryToDashboard } from './adapters/dashboard-adapter';

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
 * In mock mode: resolves mock data.
 * In hybrid/strict mode: fetches real M3 outputs and composes dashboard.
 */
export async function getDashboardData(): Promise<DashboardData> {
  // ── Mock mode — full mock pipeline ────────────────────────────────
  if (DATA_MODE === 'mock') {
    return getMockDashboard();
  }

  // ── Hybrid/Strict/API mode — fetch real data ─────────────────────
  try {
    let categorySummary: unknown;
    let pipelineReport: unknown;
    let queueItems: QueueItem[];

    if (DATA_MODE === 'api') {
      // API mode: fetch from Flask API (single composite endpoint)
      const [dashRes, qItems] = await Promise.all([
        apiFetch<{ category_summary: unknown; pipeline_report: unknown }>('/api/dashboard/summary'),
        getQueueItems(),
      ]);
      categorySummary = dashRes.category_summary;
      pipelineReport = dashRes.pipeline_report;
      queueItems = qItems;
    } else {
      // hybrid/strict: fetch static files served by Vite
      const [categorySummaryRes, pipelineReportRes, qItems] = await Promise.all([
        fetch('/output/m3/m3_category_summary.json'),
        fetch('/output/m3/m3_pipeline_report.json'),
        getQueueItems(),
      ]);

      if (!categorySummaryRes.ok || !pipelineReportRes.ok) {
        throw new Error(
          `HTTP error: category=${categorySummaryRes.status}, pipeline=${pipelineReportRes.status}`,
        );
      }

      categorySummary = await categorySummaryRes.json();
      pipelineReport = await pipelineReportRes.json();
      queueItems = qItems;
    }

    const summary = adaptM3SummaryToDashboard(
      categorySummary as { group_type: string; group_value: string; count: number }[],
      pipelineReport as { reference_date: string; input_rows: number; pending_count: number; overdue_count: number },
      queueItems,
    );

    // Urgent items: first 5 of already-sorted queue (M3 sorted by priority_score desc)
    const urgentItems = queueItems.slice(0, 5);

    // HYBRID P3.5: anomaly logs from mock — replace when import_log.json is populated
    const { anomalyLogs } = await import('../mock/anomaly-logs');
    const recentAnomalies = anomalyLogs.slice(0, 5);

    // Recommendations: mock (M5 out of scope)
    const { aiSuggestion, aiSuggestionFallback } = await import('../mock/ai-suggestion');
    const recommendations = [aiSuggestion, aiSuggestion, aiSuggestionFallback];

    // Trend history: mock (no multi-run history yet)
    const { pipelineRunsHistory } = await import('../mock/pipeline-runs-history');

    return {
      summary,
      urgentItems,
      recentAnomalies,
      recommendations,
      recommendationFallback: aiSuggestionFallback,
      trendHistory: pipelineRunsHistory,
    };
  } catch (err) {
    if (DATA_MODE === 'hybrid' || DATA_MODE === 'api') {
      console.warn('[VISASIST:schema] dashboard fetch failed, falling back to mock:', err);
      return getMockDashboard();
    }
    throw new IntegrationError(
      `Failed to fetch dashboard data: ${err}`,
      'dashboard',
    );
  }
}

// ─── Internal mock loader ───────────────────────────────────────────

async function getMockDashboard(): Promise<DashboardData> {
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
