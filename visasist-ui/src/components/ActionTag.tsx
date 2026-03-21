import { useState } from 'react';

export type ActionType = 'issue' | 'escalate' | 'arbitrate' | 'chase' | 'hold' | 'reject';

interface ActionTagProps {
  action: ActionType;
  label: string;
  onClick: () => void;
}

function getActionColor(action: ActionType): string {
  switch (action) {
    case 'issue': return 'var(--color-success)';
    case 'reject': return 'var(--color-danger)';
    case 'escalate': return 'var(--color-warning)';
    case 'arbitrate': return 'var(--color-arbitration)';
    case 'chase': return 'var(--color-info)';
    case 'hold': return 'var(--color-neutral)';
  }
}

export default function ActionTag({ action, label, onClick }: ActionTagProps) {
  const [hovered, setHovered] = useState(false);
  const actionColor = getActionColor(action);

  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      tabIndex={0}
      aria-label={label}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        padding: '4px 12px',
        borderRadius: 'var(--radius-badge)',
        border: `1px solid ${actionColor}`,
        background: hovered
          ? `color-mix(in srgb, ${actionColor} 25%, transparent)`
          : 'transparent',
        color: actionColor,
        fontSize: 12,
        fontWeight: 500,
        cursor: 'pointer',
        transition: 'background 120ms ease',
        whiteSpace: 'nowrap',
      }}
    >
      {label}
    </button>
  );
}
