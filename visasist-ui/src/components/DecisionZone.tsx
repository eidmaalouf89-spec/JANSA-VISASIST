import { useState } from 'react';
import type { Document } from '../types/document';
import { useTranslation } from '../i18n/use-translation';
import type { TranslationKey } from '../i18n/en';
import Badge from './Badge';

interface DecisionZoneProps {
  doc: Document;
  onIssueVAO: () => void;
  onIssueREF: () => void;
  onChase: () => void;
  onHold: () => void;
}

interface ActionButton {
  labelKey: TranslationKey;
  color: string;
  onClick: () => void;
  disabled: boolean;
}

export default function DecisionZone({
  doc,
  onIssueVAO,
  onIssueREF,
  onChase,
  onHold,
}: DecisionZoneProps) {
  const { t } = useTranslation();
  const [feedback, setFeedback] = useState<string | null>(null);

  const buttons: ActionButton[] = [
    {
      labelKey: 'ws_decision_issue_vao',
      color: 'var(--color-success)',
      onClick: () => {
        setFeedback('VAO');
        onIssueVAO();
        setTimeout(() => setFeedback(null), 2000);
      },
      disabled: doc.visa_global !== null,
    },
    {
      labelKey: 'ws_decision_issue_ref',
      color: 'var(--color-danger)',
      onClick: () => {
        setFeedback('REF');
        onIssueREF();
        setTimeout(() => setFeedback(null), 2000);
      },
      disabled: doc.visa_global !== null,
    },
    {
      labelKey: 'ws_decision_chase',
      color: 'var(--color-info)',
      onClick: () => {
        setFeedback('Chase sent');
        onChase();
        setTimeout(() => setFeedback(null), 2000);
      },
      disabled: doc.missing_approvers.length === 0,
    },
    {
      labelKey: 'ws_decision_hold',
      color: 'var(--color-neutral)',
      onClick: () => {
        setFeedback('Hold');
        onHold();
        setTimeout(() => setFeedback(null), 2000);
      },
      disabled: false,
    },
  ];

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        background: 'var(--bg-tertiary)',
        borderBottom: '1px solid var(--border-default)',
        padding: '12px 20px',
        flexShrink: 0,
      }}
    >
      {/* Left side — action buttons */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        {buttons.map((btn) => (
          <DecisionButton
            key={btn.labelKey}
            label={t(btn.labelKey)}
            color={btn.color}
            onClick={btn.onClick}
            disabled={btn.disabled}
          />
        ))}
        {feedback && (
          <span
            style={{
              fontSize: 11,
              color: 'var(--text-secondary)',
              marginLeft: 8,
              fontStyle: 'italic',
            }}
          >
            {feedback}
          </span>
        )}
      </div>

      {/* Right side — confidence indicator */}
      {doc.ai_suggestion !== null && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
            {Math.round(doc.ai_suggestion.confidence * 100)}% {t('ws_confidence')}
          </span>
          <Badge variant="system" label="M3/M5" />
        </div>
      )}
    </div>
  );
}

function DecisionButton({
  label,
  color,
  onClick,
  disabled,
}: {
  label: string;
  color: string;
  onClick: () => void;
  disabled: boolean;
}) {
  const [hovered, setHovered] = useState(false);

  return (
    <button
      onClick={disabled ? undefined : onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      disabled={disabled}
      style={{
        padding: '6px 14px',
        fontSize: 12,
        fontWeight: 600,
        border: `2px solid ${color}`,
        borderRadius: 'var(--radius-badge)',
        background:
          hovered && !disabled
            ? `color-mix(in srgb, ${color} 20%, transparent)`
            : 'transparent',
        color: color,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.4 : 1,
        transition: 'background 120ms ease',
        whiteSpace: 'nowrap',
      }}
    >
      {label}
    </button>
  );
}
