import { useTranslation } from '../i18n/use-translation';
import Badge from './Badge';

interface AIAdvisoryBoxProps {
  badgeLabel?: string;
  advisoryMode?: boolean;
  title?: string;
  body: string;
  isTemplate?: boolean;
  actions?: Array<{
    label: string;
    onClick: () => void;
    variant?: 'primary' | 'ghost';
  }>;
}

export default function AIAdvisoryBox({
  badgeLabel,
  advisoryMode = true,
  title,
  body,
  isTemplate,
  actions,
}: AIAdvisoryBoxProps) {
  const { t } = useTranslation();

  const displayBadgeLabel = isTemplate
    ? t('badge_template_m5')
    : (badgeLabel ?? t('badge_ai_m5'));

  return (
    <div
      style={{
        borderLeft: '3px solid var(--color-accent)',
        background: 'color-mix(in srgb, var(--color-accent) 8%, transparent)',
        padding: '12px 14px',
        borderRadius: '0 6px 6px 0',
      }}
    >
      {/* Header row: badge + advisory disclaimer */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <Badge variant="ai" label={displayBadgeLabel} />
        {advisoryMode && (
          <span
            style={{
              fontSize: 10,
              color: 'var(--text-secondary)',
              background: 'var(--bg-tertiary)',
              padding: '2px 8px',
              borderRadius: 10,
              whiteSpace: 'nowrap',
            }}
          >
            {t('advisory_disclaimer')}
          </span>
        )}
      </div>

      {/* Optional title */}
      {title && (
        <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}>
          {title}
        </div>
      )}

      {/* Body */}
      <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
        {body}
      </div>

      {/* Actions */}
      {actions && actions.length > 0 && (
        <div style={{ display: 'flex', gap: 8, marginTop: 10 }}>
          {actions.map((action) => (
            <button
              key={action.label}
              onClick={action.onClick}
              style={{
                fontSize: 12,
                fontWeight: 500,
                padding: '4px 12px',
                borderRadius: 'var(--radius-badge)',
                cursor: 'pointer',
                transition: 'background 120ms ease',
                ...(action.variant === 'primary'
                  ? {
                      background: 'var(--color-accent)',
                      color: '#fff',
                      border: 'none',
                    }
                  : {
                      background: 'transparent',
                      color: 'var(--color-accent)',
                      border: '1px solid var(--color-accent)',
                    }),
              }}
            >
              {action.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
