import type { Document } from '../types/document';
import { useTranslation } from '../i18n/use-translation';
import Badge from './Badge';
import ScoreRing from './ScoreRing';
import type { TranslationKey } from '../i18n/en';

interface ScoreBreakdownSidebarProps {
  doc: Document;
}

interface BarRow {
  labelKey: TranslationKey;
  value: number;
  max: number;
}

export default function ScoreBreakdownSidebar({ doc }: ScoreBreakdownSidebarProps) {
  const { t } = useTranslation();

  const bars: BarRow[] = [
    { labelKey: 'ws_score_overdue', value: doc.score_components.overdue_points, max: 40 },
    { labelKey: 'ws_score_deadline', value: doc.score_components.deadline_points, max: 25 },
    { labelKey: 'ws_score_completeness', value: doc.score_components.completeness_points, max: 20 },
    { labelKey: 'ws_score_revision', value: doc.score_components.revision_points, max: 5 },
  ];

  return (
    <div
      style={{
        background: 'var(--bg-tertiary)',
        borderRadius: 6,
        padding: 10,
      }}
    >
      {/* Header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 6,
          marginBottom: 8,
        }}
      >
        <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-primary)' }}>
          {t('ws_score_total')}
        </span>
        <Badge variant="system" label="M3" />
      </div>

      {/* Score ring + band label */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          marginBottom: 10,
        }}
      >
        <ScoreRing score={doc.priority_score} band={doc.score_band} size={36} />
        <div>
          <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>
            {doc.score_band}
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
            {doc.priority_score}
          </div>
        </div>
      </div>

      {/* Bar rows */}
      {bars.map((bar) => {
        const pct = Math.min(100, (bar.value / bar.max) * 100);
        return (
          <div key={bar.labelKey} style={{ marginBottom: 6 }}>
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                fontSize: 10,
                color: 'var(--text-secondary)',
                marginBottom: 2,
              }}
            >
              <span>{t(bar.labelKey)}</span>
              <span>{bar.value}/{bar.max}</span>
            </div>
            <div
              style={{
                height: 4,
                borderRadius: 2,
                background: 'var(--border-default)',
              }}
            >
              <div
                style={{
                  height: '100%',
                  width: `${pct}%`,
                  borderRadius: 2,
                  background: 'var(--color-accent)',
                }}
              />
            </div>
          </div>
        );
      })}

      {/* Penalty */}
      {doc.score_components.deadline_penalty !== 0 && (
        <div
          style={{
            fontSize: 10,
            color: 'var(--color-danger)',
            marginTop: 4,
          }}
        >
          {t('ws_score_penalty')}: {doc.score_components.deadline_penalty}
        </div>
      )}

      {/* Total */}
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          color: 'var(--text-primary)',
          marginTop: 6,
          borderTop: '1px solid var(--border-default)',
          paddingTop: 6,
        }}
      >
        {t('ws_score_total')}: {doc.priority_score}
      </div>
    </div>
  );
}
