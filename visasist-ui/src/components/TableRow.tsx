import { useState } from 'react';
import type { QueueItem, SuggestedAction } from '../types';
import type { TranslationKey } from '../i18n/en';
import { useTranslation } from '../i18n/use-translation';
import { formatRunDate } from '../i18n/format-date';
import { CATEGORY_LABEL_MAP } from '../selectors/category-constants';
import Badge from './Badge';
import ActionTag from './ActionTag';
import type { ActionType } from './ActionTag';

interface TableRowProps {
  item: QueueItem;
  selected: boolean;
  active: boolean;
  onRowClick: () => void;
  onCheckboxChange: (checked: boolean) => void;
}

const ACTION_MAP: Record<SuggestedAction, ActionType> = {
  ISSUE_VISA: 'issue',
  ESCALATE: 'escalate',
  ARBITRATE: 'arbitrate',
  CHASE_APPROVERS: 'chase',
  HOLD: 'hold',
};

const ACTION_LABEL_MAP: Record<SuggestedAction, TranslationKey> = {
  ISSUE_VISA: 'suggested_issue_visa',
  ESCALATE: 'suggested_escalate',
  ARBITRATE: 'suggested_arbitrate',
  CHASE_APPROVERS: 'suggested_chase_approvers',
  HOLD: 'suggested_hold',
};

function getConsensusColor(consensus: string): string {
  switch (consensus) {
    case 'MIXED': return 'var(--color-arbitration)';
    case 'ALL_APPROVE': return 'var(--color-success)';
    case 'ALL_REJECT': return 'var(--color-danger)';
    case 'INCOMPLETE': return 'var(--color-warning)';
    case 'NOT_STARTED':
    case 'ALL_HM':
    default: return 'var(--color-neutral)';
  }
}

const cellStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  padding: '0 8px',
  fontSize: 13,
  overflow: 'hidden',
  whiteSpace: 'nowrap',
  textOverflow: 'ellipsis',
};

export default function TableRow({ item, selected, active, onRowClick, onCheckboxChange }: TableRowProps) {
  const { t, lang } = useTranslation();
  const [hovered, setHovered] = useState(false);

  const isHighlighted = selected || active;
  const bg = isHighlighted
    ? 'color-mix(in srgb, var(--color-accent) 7%, transparent)'
    : hovered
      ? 'var(--bg-tertiary)'
      : 'transparent';

  // "Why this rank" micro-label
  const whyParts: string[] = [];
  if (item.is_overdue) whyParts.push(`${item.days_overdue}d overdue`);
  whyParts.push(item.consensus_type);
  whyParts.push(`Rev ${item.revision_count}`);
  const whyText = whyParts.join(' \u00b7 ');

  return (
    <div
      onClick={onRowClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        display: 'flex',
        alignItems: 'center',
        width: '100%',
        height: '100%',
        background: bg,
        cursor: 'pointer',
        borderLeft: isHighlighted ? '2px solid var(--color-accent)' : '2px solid transparent',
        borderBottom: '1px solid var(--border-default)',
        boxSizing: 'border-box',
        transition: 'background 80ms ease',
      }}
    >
      {/* Checkbox — 40px */}
      <div style={{ ...cellStyle, width: 40, flexShrink: 0, justifyContent: 'center' }}>
        <input
          type="checkbox"
          checked={selected}
          onChange={(e) => {
            e.stopPropagation();
            onCheckboxChange(e.target.checked);
          }}
          onClick={(e) => e.stopPropagation()}
          style={{ accentColor: 'var(--color-accent)', cursor: 'pointer' }}
        />
      </div>

      {/* Document — 220px */}
      <div style={{ ...cellStyle, width: 220, flexShrink: 0, flexDirection: 'column', alignItems: 'flex-start', justifyContent: 'center', gap: 1 }}>
        <span style={{ fontWeight: 600, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%', fontSize: 12.5 }} title={item.document}>
          {item.document}
        </span>
        <span style={{ fontSize: 11, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>
          {item.titre ?? '\u2014'}
        </span>
        <span style={{ fontSize: 10.5, color: 'var(--text-tertiary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>
          {whyText}
        </span>
      </div>

      {/* Lot — 100px */}
      <div style={{ ...cellStyle, width: 100, flexShrink: 0 }}>
        <Badge variant="lot" label={item.source_sheet} />
      </div>

      {/* Category — 130px */}
      <div style={{ ...cellStyle, width: 130, flexShrink: 0 }}>
        <Badge variant="category" category={item.category} label={t(CATEGORY_LABEL_MAP[item.category])} />
      </div>

      {/* Score — 70px */}
      <div style={{ ...cellStyle, width: 70, flexShrink: 0 }}>
        <Badge variant="score" scoreBand={item.score_band} label={String(item.priority_score)} />
      </div>

      {/* Deadline — 100px */}
      <div style={{ ...cellStyle, width: 100, flexShrink: 0 }}>
        {item.date_contractuelle_visa ? (
          <span style={{ color: item.is_overdue ? 'var(--color-danger)' : 'var(--text-primary)', fontSize: 12 }}>
            {formatRunDate(item.date_contractuelle_visa, lang)}
          </span>
        ) : (
          <span style={{ color: 'var(--text-tertiary)' }}>{'\u2014'}</span>
        )}
      </div>

      {/* Overdue — 70px */}
      <div style={{ ...cellStyle, width: 70, flexShrink: 0 }}>
        {item.is_overdue ? (
          <span style={{ color: 'var(--color-danger)', fontWeight: 600, fontSize: 12 }}>
            \u2191 {item.days_overdue}d
          </span>
        ) : (
          <span style={{ color: 'var(--text-tertiary)' }}>{'\u2014'}</span>
        )}
      </div>

      {/* Consensus — 100px */}
      <div style={{ ...cellStyle, width: 100, flexShrink: 0 }}>
        <span style={{ color: getConsensusColor(item.consensus_type), fontWeight: 500, fontSize: 12 }}>
          {item.consensus_type}
        </span>
      </div>

      {/* Missing — 120px */}
      <div style={{ ...cellStyle, width: 120, flexShrink: 0 }}>
        <span style={{ color: 'var(--color-warning)', fontSize: 12, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>
          {item.missing_approvers.length > 0 ? item.missing_approvers.join(', ') : '\u2014'}
        </span>
      </div>

      {/* Rev — 50px */}
      <div style={{ ...cellStyle, width: 50, flexShrink: 0 }}>
        <span style={{ fontWeight: 700 }}>{item.revision_count}</span>
      </div>

      {/* Action — 130px */}
      <div style={{ ...cellStyle, width: 130, flexShrink: 0 }}>
        <ActionTag
          action={ACTION_MAP[item.suggested_action]}
          label={t(ACTION_LABEL_MAP[item.suggested_action])}
          onClick={() => {/* stub — action handled via detail panel */}}
        />
      </div>
    </div>
  );
}
