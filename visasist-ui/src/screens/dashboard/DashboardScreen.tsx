import { useState, useEffect } from 'react';
import { useTranslation } from '../../i18n/use-translation';
import { formatRunDate } from '../../i18n/format-date';
import { ROUTES } from '../../routes';
import { getDashboardData } from '../../data/dashboard';
import type { DashboardData } from '../../data/dashboard';
import Badge from '../../components/Badge';
import KPICard from '../../components/KPICard';
import CategoryBreakdown from '../../components/CategoryBreakdown';
import PendingTrendSparkline from '../../components/PendingTrendSparkline';
import LotHealthChart from '../../components/LotHealthChart';
import UrgentItemsList from '../../components/UrgentItemsList';
import RecentAnomalies from '../../components/RecentAnomalies';
import RecommendationsStrip from '../../components/RecommendationsStrip';

export default function DashboardScreen() {
  const { t, lang } = useTranslation();
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    getDashboardData()
      .then((result) => {
        if (!cancelled) {
          setData(result);
          setLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setData(null);
          setLoading(false);
        }
      });
    return () => { cancelled = true; };
  }, []);

  // Loading state
  if (loading) {
    return (
      <div style={{ padding: '24px', maxWidth: 1400, margin: '0 auto' }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: 300,
          color: 'var(--text-secondary)',
          fontSize: 14,
        }}>
          {t('dashboard_loading')}
        </div>
      </div>
    );
  }

  // Error / no-data fallback
  if (!data) {
    return (
      <div style={{ padding: '24px', maxWidth: 1400, margin: '0 auto' }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: 300,
          color: 'var(--text-tertiary)',
          fontSize: 14,
        }}>
          {t('dashboard_error')}
        </div>
      </div>
    );
  }

  const { summary } = data;
  const runDate = formatRunDate(summary.run_at, lang);

  return (
    <div style={{ padding: '24px', maxWidth: 1400, margin: '0 auto' }}>
      {/* Page header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <h1 style={{ fontSize: 18, fontWeight: 700, color: 'var(--text-primary)' }}>
          {t('dashboard_page_header')}
        </h1>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {summary.lot_count} {t('dashboard_lots')} · {summary.doc_count.toLocaleString()} {t('dashboard_documents')}
        </span>
        <span style={{ fontSize: 11, color: 'var(--text-tertiary)' }}>
          {t('dashboard_last_run')}: {runDate}
        </span>
        <Badge label={t('badge_system')} variant="system" />
      </div>

      {/* KPI Grid — 6 cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: 12, marginBottom: 20 }}>
        <KPICard
          label={t('kpi_total_docs')}
          value={summary.kpis.total_docs}
          delta={null}
          accentColor="var(--color-neutral)"
          destination={ROUTES.queue}
        />
        <KPICard
          label={t('kpi_pending')}
          value={summary.kpis.pending_visa}
          delta={summary.kpi_deltas.pending_visa}
          accentColor="var(--color-info)"
          destination={ROUTES.queue}
        />
        <KPICard
          label={t('kpi_overdue')}
          value={summary.kpis.overdue}
          delta={summary.kpi_deltas.overdue}
          accentColor="var(--color-danger)"
          destination={`${ROUTES.queue}?overdue=1`}
        />
        <KPICard
          label={t('kpi_easy_wins')}
          value={summary.kpis.easy_wins}
          delta={summary.kpi_deltas.easy_wins}
          accentColor="var(--color-success)"
          destination={`${ROUTES.queue}?cat=EASY_WIN_APPROVE`}
        />
        <KPICard
          label={t('kpi_blocked')}
          value={summary.kpis.blocked}
          delta={summary.kpi_deltas.blocked}
          accentColor="var(--color-danger)"
          destination={`${ROUTES.queue}?cat=BLOCKED`}
        />
        <KPICard
          label={t('kpi_conflicts')}
          value={summary.kpis.conflicts}
          delta={summary.kpi_deltas.conflicts}
          accentColor="var(--color-arbitration)"
          destination={`${ROUTES.queue}?cat=CONFLICT`}
        />
      </div>

      {/* Row 2: Category | Sparkline | LotHealth — 3-column equal grid */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12, marginBottom: 20 }}>
        <CategoryBreakdown categoryCounts={summary.category_counts} />
        <PendingTrendSparkline history={data.trendHistory} />
        <LotHealthChart lots={summary.lot_health_top5} mainBlocker={summary.main_blocker} />
      </div>

      {/* Row 3: Urgent Items | Anomalies — 60/40 */}
      <div style={{ display: 'grid', gridTemplateColumns: '3fr 2fr', gap: 12, marginBottom: 20 }}>
        <UrgentItemsList items={data.urgentItems} />
        <RecentAnomalies anomalies={data.recentAnomalies} />
      </div>

      {/* Recommendations strip — full width */}
      <RecommendationsStrip
        recommendationIds={summary.top_recommendation_ids}
        recommendations={data.recommendations}
      />
    </div>
  );
}
