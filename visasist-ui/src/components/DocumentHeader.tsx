import type { Document } from '../types/document';
import type { LifecycleState } from '../types';
import { useTranslation } from '../i18n/use-translation';
import { formatRunDate } from '../i18n/format-date';
import { CATEGORY_LABEL_MAP } from '../selectors/category-constants';
import Badge from './Badge';
import ScoreRing from './ScoreRing';

interface DocumentHeaderProps {
  doc: Document;
  onBack: () => void;
}

function getLifecycleAccentColor(lc: LifecycleState): string {
  switch (lc) {
    case 'READY_TO_ISSUE':
      return 'var(--color-success)';
    case 'READY_TO_REJECT':
    case 'CHRONIC_BLOCKED':
      return 'var(--color-danger)';
    case 'NEEDS_ARBITRATION':
      return 'var(--color-arbitration)';
    case 'WAITING_RESPONSES':
      return 'var(--color-info)';
    case 'ON_HOLD':
      return 'var(--color-warning)';
    case 'NOT_STARTED':
    case 'EXCLUDED':
    case 'SYNTHESIS_ISSUED':
      return 'var(--color-neutral)';
    case 'CONFLICT':
      return 'var(--color-arbitration)';
    case 'SAS_BLOCKED':
    case 'BLOCKED':
      return 'var(--color-danger)';
    case 'SAS_PENDING':
      return 'var(--color-warning)';
  }
}

export default function DocumentHeader({ doc, onBack }: DocumentHeaderProps) {
  const { t, lang } = useTranslation();

  return (
    <div style={{ padding: '12px 20px', flexShrink: 0 }}>
      {/* Row 1 — Breadcrumb */}
      <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 6 }}>
        <button
          onClick={onBack}
          style={{
            background: 'none',
            border: 'none',
            color: 'var(--text-secondary)',
            fontSize: 12,
            cursor: 'pointer',
            padding: 0,
            textDecoration: 'none',
          }}
          onMouseEnter={(e) => {
            (e.target as HTMLButtonElement).style.color = 'var(--color-accent)';
          }}
          onMouseLeave={(e) => {
            (e.target as HTMLButtonElement).style.color = 'var(--text-secondary)';
          }}
        >
          {t('ws_breadcrumb_queue')}
        </button>
        <span style={{ margin: '0 4px' }}>&rsaquo;</span>
        <span>{doc.source_sheet}</span>
        <span style={{ margin: '0 4px' }}>&rsaquo;</span>
        <span
          style={{
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {doc.document}
        </span>
      </div>

      {/* Row 2 — Full document reference */}
      <div
        style={{
          fontFamily: 'var(--font-mono)',
          fontSize: 15,
          fontWeight: 600,
          color: 'var(--text-primary)',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title={doc.document}
      >
        {doc.document}
      </div>
      <div
        style={{
          fontSize: 12,
          color: 'var(--text-secondary)',
          marginBottom: 8,
        }}
      >
        {doc.titre ?? '—'}
      </div>

      {/* Row 3 — Badge row */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 6,
          flexWrap: 'wrap',
          marginBottom: 8,
        }}
      >
        <Badge
          variant="category"
          category={doc.category}
          label={t(CATEGORY_LABEL_MAP[doc.category])}
        />
        {/* Lifecycle state badge with border-left accent */}
        <span
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            padding: '2px 8px',
            borderRadius: 'var(--radius-badge)',
            background: 'var(--bg-tertiary)',
            color: 'var(--text-secondary)',
            fontSize: 11,
            fontWeight: 600,
            lineHeight: '18px',
            whiteSpace: 'nowrap',
            letterSpacing: '0.02em',
            borderLeft: `3px solid ${getLifecycleAccentColor(doc.lifecycle_state)}`,
          }}
        >
          {doc.lifecycle_state}
        </span>
        {doc.is_overdue && (
          <Badge variant="score" scoreBand="CRITICAL" label="OVERDUE" />
        )}
        <Badge variant="system" label={`IND ${doc.ind ?? '—'}`} />
        <Badge variant="lot" label={doc.lot ?? '—'} />
        {doc.is_cross_lot && (
          <Badge variant="system" label="CROSS-LOT" />
        )}
      </div>

      {/* Row 4 — Score row */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 10,
        }}
      >
        <ScoreRing score={doc.priority_score} band={doc.score_band} size={44} />
        <span
          style={{
            fontSize: 13,
            fontWeight: 600,
            color: 'var(--text-primary)',
          }}
        >
          {doc.score_band}
        </span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          Rev {doc.revision_count}
        </span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {doc.date_diffusion
            ? formatRunDate(doc.date_diffusion, lang)
            : '—'}
        </span>
      </div>
    </div>
  );
}
