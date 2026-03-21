import { useNavigate } from 'react-router-dom';
import { useTranslation } from '../i18n/use-translation';
import { ROUTES } from '../routes';
import Badge from './Badge';
import { ACTION_TO_CATEGORY } from '../selectors/category-constants';
import type { TranslationKey } from '../i18n/en';
import type { SuggestedAction, AISuggestion } from '../types';

const SUGGESTED_ACTION_LABELS: Record<SuggestedAction, TranslationKey> = {
  ISSUE_VISA: 'suggested_issue_visa',
  ESCALATE: 'suggested_escalate',
  ARBITRATE: 'suggested_arbitrate',
  CHASE_APPROVERS: 'suggested_chase_approvers',
  HOLD: 'suggested_hold',
};

/**
 * Fallback template i18n keys per action.
 * Used when explanation_template is missing or does not match any known key.
 */
const ACTION_TEMPLATE_KEYS: Record<SuggestedAction, TranslationKey> = {
  ISSUE_VISA: 'rec_template_issue_visa',
  ESCALATE: 'rec_template_escalate',
  ARBITRATE: 'rec_template_arbitrate',
  CHASE_APPROVERS: 'rec_template_chase_approvers',
  HOLD: 'rec_template_hold',
};

/**
 * Known i18n template keys. Used to check if explanation_template
 * can be resolved through the translation system.
 */
const KNOWN_TEMPLATE_KEYS = new Set<string>(Object.values(ACTION_TEMPLATE_KEYS));

interface RecommendationsStripProps {
  recommendationIds: string[];
  recommendations: AISuggestion[];
}

export default function RecommendationsStrip({ recommendationIds, recommendations }: RecommendationsStripProps) {
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
        {t('dashboard_recommendations')}
      </h3>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
        {recommendationIds.map((id, i) => {
          const suggestion = recommendations[i];
          if (!suggestion) return null;

          const isAI = suggestion.ai_available;
          const action = suggestion.suggested_action;
          const category = ACTION_TO_CATEGORY[action];

          // Resolve body text:
          // - AI available + explanation_text present → use AI prose directly
          // - Otherwise → resolve explanation_template through i18n
          //   If template key is known → t(key)
          //   If unknown → fall back to action-level template key
          let body: string;
          if (isAI && suggestion.explanation_text) {
            body = suggestion.explanation_text;
          } else {
            const templateKey = suggestion.explanation_template;
            if (KNOWN_TEMPLATE_KEYS.has(templateKey)) {
              body = t(templateKey as TranslationKey);
            } else {
              body = t(ACTION_TEMPLATE_KEYS[action]);
            }
          }

          return (
            <div
              key={id}
              style={{
                background: 'var(--bg-tertiary)',
                borderRadius: 8,
                padding: '14px',
                display: 'flex',
                flexDirection: 'column',
                gap: 10,
                border: '1px solid var(--border-default)',
              }}
            >
              {/* Header: number + badges */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span
                  style={{
                    width: 22,
                    height: 22,
                    borderRadius: '50%',
                    background: 'color-mix(in srgb, var(--color-accent) 15%, transparent)',
                    color: 'var(--color-accent)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: 11,
                    fontWeight: 700,
                    flexShrink: 0,
                  }}
                >
                  {i + 1}
                </span>

                {isAI ? (
                  <Badge label={t('badge_ai_m5')} variant="ai" />
                ) : (
                  <Badge label={t('badge_template_m5')} variant="system" />
                )}
                <Badge label={t('badge_advisory')} variant="ai" />
              </div>

              {/* Title */}
              <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
                {t(SUGGESTED_ACTION_LABELS[action])}
              </div>

              {/* Body */}
              <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5, flex: 1 }}>
                {body}
              </div>

              {/* Actions */}
              <div style={{ display: 'flex', gap: 8, marginTop: 'auto' }}>
                <button
                  onClick={() => navigate(`${ROUTES.queue}?cat=${category}`)}
                  style={{
                    padding: '5px 10px',
                    fontSize: 11,
                    borderRadius: 'var(--radius-badge)',
                    border: '1px solid var(--border-default)',
                    color: 'var(--text-secondary)',
                    transition: 'background 120ms ease',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-secondary)'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                >
                  {t('dashboard_view_in_queue')}
                </button>
                <button
                  onClick={() => navigate(ROUTES.aiAssistant)}
                  style={{
                    padding: '5px 10px',
                    fontSize: 11,
                    borderRadius: 'var(--radius-badge)',
                    border: '1px solid color-mix(in srgb, var(--color-accent) 40%, transparent)',
                    color: 'var(--color-accent)',
                    transition: 'background 120ms ease',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.background = 'color-mix(in srgb, var(--color-accent) 10%, transparent)'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                >
                  {t('dashboard_ask_ai')}
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
