import { useState } from 'react';
import type { Document } from '../types/document';
import type { DraftAction } from '../types/ai-suggestion';
import { useTranslation } from '../i18n/use-translation';
import type { TranslationKey } from '../i18n/en';
import Badge from './Badge';
import ScoreRing from './ScoreRing';
import AIAdvisoryBox from './AIAdvisoryBox';

interface WorkspaceAnalysisTabProps {
  doc: Document;
}

interface BarRow {
  labelKey: TranslationKey;
  value: number;
  max: number;
}

function getDraftTitle(type: DraftAction['type'], t: (k: TranslationKey) => string): string {
  switch (type) {
    case 'synthesis':
      return t('ws_draft_synthesis');
    case 'relance':
      return t('ws_draft_relance');
    case 'note':
      return t('ws_draft_note');
  }
}

export default function WorkspaceAnalysisTab({ doc }: WorkspaceAnalysisTabProps) {
  const { t } = useTranslation();
  const [expandedDrafts, setExpandedDrafts] = useState<Record<number, boolean>>({});

  const bars: BarRow[] = [
    { labelKey: 'ws_score_overdue', value: doc.score_components.overdue_points, max: 40 },
    { labelKey: 'ws_score_deadline', value: doc.score_components.deadline_points, max: 25 },
    { labelKey: 'ws_score_completeness', value: doc.score_components.completeness_points, max: 20 },
    { labelKey: 'ws_score_revision', value: doc.score_components.revision_points, max: 5 },
  ];

  const stubFn = () => {
    /* P5 scope */
  };

  // Determine AI block rendering
  const ai = doc.ai_suggestion;
  let aiBlock: React.ReactNode;

  if (ai === null || ai.validation_failed) {
    aiBlock = (
      <AIAdvisoryBox
        body={t('empty_no_ai')}
        isTemplate={true}
        advisoryMode={true}
      />
    );
  } else if (ai.ai_available && ai.explanation_text !== null) {
    aiBlock = (
      <AIAdvisoryBox
        body={ai.explanation_text}
        isTemplate={false}
        advisoryMode={true}
        title={t(('suggested_' + ai.suggested_action.toLowerCase()) as TranslationKey)}
        actions={[{ label: t('ws_regenerate'), onClick: stubFn, variant: 'ghost' }]}
      />
    );
  } else {
    // ai_available === false (template fallback)
    aiBlock = (
      <AIAdvisoryBox
        body={t(ai.explanation_template as TranslationKey)}
        isTemplate={true}
        advisoryMode={true}
        actions={[{ label: t('ws_regenerate'), onClick: stubFn, variant: 'ghost' }]}
      />
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Block 1 — Score breakdown */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
          <ScoreRing score={doc.priority_score} band={doc.score_band} size={44} />
          <div>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>
              {doc.score_band}
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
              {doc.priority_score}
            </div>
          </div>
          <Badge variant="system" label="M3" />
        </div>

        {bars.map((bar) => {
          const pct = Math.min(100, (bar.value / bar.max) * 100);
          return (
            <div key={bar.labelKey} style={{ marginBottom: 8 }}>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  fontSize: 12,
                  color: 'var(--text-secondary)',
                  marginBottom: 3,
                }}
              >
                <span>{t(bar.labelKey)}</span>
                <span>
                  {bar.value} / {bar.max}
                </span>
              </div>
              <div
                style={{
                  height: 6,
                  borderRadius: 3,
                  background: 'var(--border-default)',
                }}
              >
                <div
                  style={{
                    height: '100%',
                    width: `${pct}%`,
                    borderRadius: 3,
                    background: 'var(--color-accent)',
                  }}
                />
              </div>
            </div>
          );
        })}

        {doc.score_components.deadline_penalty !== 0 && (
          <div style={{ fontSize: 12, color: 'var(--color-danger)', marginTop: 4 }}>
            {t('ws_score_penalty')}: {doc.score_components.deadline_penalty}
          </div>
        )}

        <div
          style={{
            fontSize: 13,
            fontWeight: 600,
            color: 'var(--text-primary)',
            marginTop: 8,
            borderTop: '1px solid var(--border-default)',
            paddingTop: 8,
          }}
        >
          {t('ws_score_total')}: {doc.priority_score}
        </div>
      </div>

      {/* Block 2 — AI analysis (advisory) */}
      <div>{aiBlock}</div>

      {/* Block 3 — Draft actions */}
      {ai !== null && ai.draft_actions && ai.draft_actions.length > 0 && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
            <Badge variant="ai" label={t('badge_ai_m5')} />
          </div>

          {ai.draft_actions.map((action, i) => {
            const isExpanded = expandedDrafts[i] ?? false;
            const title = getDraftTitle(action.type, t);
            const body = action.content ?? action.template;
            const isPlaceholder = action.content === null;

            return (
              <div
                key={i}
                style={{
                  border: '1px solid var(--border-default)',
                  borderRadius: 6,
                  marginBottom: 8,
                  overflow: 'hidden',
                }}
              >
                <button
                  onClick={() =>
                    setExpandedDrafts((prev) => ({ ...prev, [i]: !isExpanded }))
                  }
                  style={{
                    width: '100%',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '8px 12px',
                    background: 'var(--bg-tertiary)',
                    border: 'none',
                    cursor: 'pointer',
                    fontSize: 12,
                    fontWeight: 600,
                    color: 'var(--text-primary)',
                  }}
                >
                  <span>{title}</span>
                  <span style={{ fontSize: 10, color: 'var(--text-tertiary)' }}>
                    {isExpanded ? '▲' : '▼'}
                  </span>
                </button>
                {isExpanded && (
                  <div
                    style={{
                      padding: '8px 12px',
                      fontSize: 12,
                      color: isPlaceholder
                        ? 'var(--text-tertiary)'
                        : 'var(--text-secondary)',
                      lineHeight: 1.5,
                    }}
                  >
                    {body}
                  </div>
                )}
              </div>
            );
          })}

          {/* Confidence indicator */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              marginTop: 4,
            }}
          >
            <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
              {ai.ai_available
                ? `${Math.round(ai.confidence * 100)}% ${t('ws_confidence')}`
                : `0% — AI unavailable`}
            </span>
            <Badge variant="system" label="M5" />
          </div>
        </div>
      )}
    </div>
  );
}
