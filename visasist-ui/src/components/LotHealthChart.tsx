import { useNavigate } from 'react-router-dom';
import { useTranslation } from '../i18n/use-translation';
import { ROUTES } from '../routes';
import { getMainBlockerDisplayName, getLotBarColor } from '../selectors/dashboard-selectors';
import type { LotHealth } from '../data/dashboard';

interface LotHealthChartProps {
  lots: LotHealth[];
  mainBlocker: string | null;
}

/**
 * Compute overdue ratio from the raw fields, not from the pre-computed overdue_pct.
 * Returns a value between 0 and 1. Safe if total is 0.
 */
function computeOverdueRatio(lot: LotHealth): number {
  if (lot.total <= 0) return 0;
  return Math.min(lot.overdue / lot.total, 1);
}

export default function LotHealthChart({ lots, mainBlocker }: LotHealthChartProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  // TODO(P2): Compute main blocker from actual blocking approver frequency
  // per lot instead of fixture-provided simplified value.
  const blockerName = getMainBlockerDisplayName(mainBlocker);

  // Compute ratios from raw overdue/total for each lot
  const lotsWithRatio = lots.map((lot) => ({
    ...lot,
    ratio: computeOverdueRatio(lot),
  }));
  const maxRatio = Math.max(...lotsWithRatio.map((l) => l.ratio), 0.01);

  return (
    <div
      style={{
        background: 'var(--bg-secondary)',
        borderRadius: 'var(--radius-card)',
        border: '1px solid var(--border-default)',
        padding: '16px',
      }}
    >
      <h3 style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 14, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
        {t('dashboard_lot_health')}
      </h3>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        {lotsWithRatio.map((lot) => {
          const barWidth = (lot.ratio / maxRatio) * 100;
          const barColor = getLotBarColor(lot.ratio);
          const displayPct = (lot.ratio * 100).toFixed(1);

          return (
            <a
              key={lot.lot}
              href={`${ROUTES.queue}?lot=${encodeURIComponent(lot.lot)}`}
              onClick={(e) => {
                e.preventDefault();
                navigate(`${ROUTES.queue}?lot=${encodeURIComponent(lot.lot)}`);
              }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                padding: '4px 6px',
                borderRadius: 6,
                textDecoration: 'none',
                cursor: 'pointer',
                transition: 'background 120ms ease',
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-tertiary)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
            >
              <span style={{ fontSize: 11, color: 'var(--text-secondary)', width: 110, flexShrink: 0, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {lot.lot}
              </span>

              <div style={{ flex: 1, height: 8, background: 'var(--bg-tertiary)', borderRadius: 4, overflow: 'hidden' }}>
                <div
                  style={{
                    width: `${barWidth}%`,
                    height: '100%',
                    borderRadius: 4,
                    background: barColor,
                    transition: 'width 300ms ease',
                  }}
                />
              </div>

              <span style={{ fontSize: 11, color: 'var(--text-secondary)', width: 50, textAlign: 'right' }}>
                {lot.overdue}/{lot.total}
              </span>

              <span style={{ fontSize: 11, color: 'var(--text-tertiary)', width: 45, textAlign: 'right' }}>
                {displayPct}%
              </span>
            </a>
          );
        })}
      </div>

      {blockerName && (
        <div style={{ marginTop: 12, fontSize: 11, color: 'var(--text-tertiary)' }}>
          {t('dashboard_main_blocker')}: <span style={{ color: 'var(--color-danger)', fontWeight: 600 }}>{blockerName}</span>
        </div>
      )}
    </div>
  );
}
