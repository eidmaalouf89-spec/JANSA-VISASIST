import { useTranslation } from '../i18n/use-translation';
import type { PipelineRunHistoryPoint } from '../data/dashboard';

interface PendingTrendSparklineProps {
  history: PipelineRunHistoryPoint[];
}

export default function PendingTrendSparkline({ history }: PendingTrendSparklineProps) {
  const { t } = useTranslation();

  if (history.length === 0) return null;

  const counts = history.map((d) => d.pending_count);
  const minVal = Math.min(...counts);
  const maxVal = Math.max(...counts);
  const range = maxVal - minVal || 1;

  const width = 280;
  const height = 80;
  const paddingX = 10;
  const paddingY = 10;

  const chartW = width - paddingX * 2;
  const chartH = height - paddingY * 2;

  const points = counts.map((val, i) => {
    const x = paddingX + (history.length <= 1 ? chartW / 2 : (i / (history.length - 1)) * chartW);
    const y = paddingY + chartH - ((val - minVal) / range) * chartH;
    return { x, y };
  });

  const polylinePoints = points.map((p) => `${p.x},${p.y}`).join(' ');

  // Area fill path
  const areaPath = points.length >= 2
    ? `M${points[0].x},${paddingY + chartH} ` +
      points.map((p) => `L${p.x},${p.y}`).join(' ') +
      ` L${points[points.length - 1].x},${paddingY + chartH} Z`
    : '';

  const lastPoint = points[points.length - 1];
  const lastValue = counts[counts.length - 1];

  return (
    <div
      style={{
        background: 'var(--bg-secondary)',
        borderRadius: 'var(--radius-card)',
        border: '1px solid var(--border-default)',
        padding: '16px',
      }}
    >
      <h3 style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
        {t('dashboard_pending_trend')}
      </h3>

      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} style={{ display: 'block' }}>
        {/* Area fill */}
        {areaPath && (
          <path
            d={areaPath}
            fill="color-mix(in srgb, var(--color-accent) 15%, transparent)"
          />
        )}

        {/* Line */}
        <polyline
          points={polylinePoints}
          fill="none"
          stroke="var(--color-accent)"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* Last point dot */}
        {lastPoint && (
          <circle
            cx={lastPoint.x}
            cy={lastPoint.y}
            r={4}
            fill="var(--color-accent)"
          />
        )}
      </svg>

      <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--text-primary)', marginTop: 6 }}>
        {lastValue.toLocaleString()}
      </div>
    </div>
  );
}
