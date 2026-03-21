import { useNavigate } from 'react-router-dom';
import { useTranslation } from '../i18n/use-translation';
import { ROUTES } from '../routes';
import Badge from './Badge';
import { getCategoryBreakdown, getCategoryMaxCount } from '../selectors/dashboard-selectors';
import { CATEGORY_LABEL_MAP, getCategoryColor } from '../selectors/category-constants';

interface CategoryBreakdownProps {
  categoryCounts: Record<string, number>;
}

export default function CategoryBreakdown({ categoryCounts }: CategoryBreakdownProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const rows = getCategoryBreakdown(categoryCounts);
  const maxCount = getCategoryMaxCount(categoryCounts);

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
        {t('dashboard_category_breakdown')}
      </h3>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {rows.map((row) => {
          const barWidth = maxCount > 0 ? (row.count / maxCount) * 100 : 0;
          return (
            <a
              key={row.category}
              href={`${ROUTES.queue}?cat=${row.category}`}
              onClick={(e) => {
                e.preventDefault();
                navigate(`${ROUTES.queue}?cat=${row.category}`);
              }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                padding: '6px 8px',
                borderRadius: 6,
                textDecoration: 'none',
                transition: 'background 120ms ease',
                cursor: 'pointer',
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-tertiary)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
            >
              <div style={{ width: 90, flexShrink: 0 }}>
                <Badge
                  label={t(CATEGORY_LABEL_MAP[row.category])}
                  variant="category"
                  category={row.category}
                />
              </div>

              <div style={{ flex: 1, height: 6, background: 'var(--bg-tertiary)', borderRadius: 3, overflow: 'hidden' }}>
                <div
                  style={{
                    width: `${barWidth}%`,
                    height: '100%',
                    borderRadius: 3,
                    transition: 'width 300ms ease',
                    background: getCategoryColor(row.category),
                  }}
                />
              </div>

              <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', width: 40, textAlign: 'right' }}>
                {row.count}
              </span>

              <span style={{ fontSize: 11, color: 'var(--text-tertiary)', width: 40, textAlign: 'right' }}>
                {row.percentage.toFixed(1)}%
              </span>
            </a>
          );
        })}
      </div>
    </div>
  );
}
