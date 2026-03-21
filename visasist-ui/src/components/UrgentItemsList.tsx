import { useNavigate } from 'react-router-dom';
import { useTranslation } from '../i18n/use-translation';
import { workspaceRoute } from '../routes';
import Badge from './Badge';
import { truncate } from '../selectors/dashboard-selectors';
import { CATEGORY_LABEL_MAP } from '../selectors/category-constants';
import type { QueueItem } from '../types';

interface UrgentItemsListProps {
  items: QueueItem[];
}

export default function UrgentItemsList({ items }: UrgentItemsListProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();

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
        {t('dashboard_urgent_items')}
      </h3>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {items.map((item) => (
          <a
            key={item.doc_version_key}
            href={workspaceRoute(item.doc_version_key)}
            onClick={(e) => {
              e.preventDefault();
              navigate(workspaceRoute(item.doc_version_key));
            }}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              padding: '8px',
              borderRadius: 6,
              textDecoration: 'none',
              cursor: 'pointer',
              transition: 'background 120ms ease',
            }}
            onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-tertiary)'; }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
          >
            {/* Document reference */}
            <span style={{ fontSize: 12, color: 'var(--text-primary)', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {truncate(item.titre ?? item.document, 40)}
            </span>

            {/* Lot badge */}
            {item.lot && (
              <Badge label={item.lot} variant="lot" />
            )}

            {/* Category badge */}
            <Badge
              label={t(CATEGORY_LABEL_MAP[item.category])}
              variant="category"
              category={item.category}
            />

            {/* Score badge */}
            <Badge
              label={String(item.priority_score)}
              variant="score"
              scoreBand={item.score_band}
            />

            {/* Overdue label */}
            {item.days_overdue > 0 && (
              <span style={{ fontSize: 11, color: 'var(--color-danger)', fontWeight: 600, whiteSpace: 'nowrap' }}>
                {item.days_overdue}{t('dashboard_days_overdue')}
              </span>
            )}
          </a>
        ))}
      </div>
    </div>
  );
}
