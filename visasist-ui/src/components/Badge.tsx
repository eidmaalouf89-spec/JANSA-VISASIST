import type { Category, ScoreBand, VisaStatus, RowQuality, LifecycleState } from '../types';

export type BadgeVariant =
  | 'system' | 'ai' | 'category' | 'score'
  | 'lot' | 'status' | 'lifecycle' | 'quality';

interface BadgeProps {
  label: string;
  variant: BadgeVariant;
  category?: Category;
  scoreBand?: ScoreBand;
  visaStatus?: VisaStatus | null;
  quality?: RowQuality;
  lifecycle?: LifecycleState;
  onClick?: () => void;
}

function getCategoryColor(cat: Category): string {
  switch (cat) {
    case 'EASY_WIN_APPROVE': return 'var(--color-success)';
    case 'BLOCKED':
    case 'FAST_REJECT': return 'var(--color-danger)';
    case 'CONFLICT': return 'var(--color-arbitration)';
    case 'WAITING': return 'var(--color-info)';
    case 'NOT_STARTED': return 'var(--color-neutral)';
  }
}

function getScoreColor(band: ScoreBand): string {
  switch (band) {
    case 'CRITICAL': return 'var(--color-danger)';
    case 'HIGH': return 'var(--color-warning)';
    case 'MEDIUM': return 'var(--color-info)';
    case 'LOW': return 'var(--color-neutral)';
  }
}

function getVisaColor(status: VisaStatus | null): string {
  if (!status) return 'var(--color-warning)';
  switch (status) {
    case 'VAO':
    case 'VSO': return 'var(--color-success)';
    case 'REF': return 'var(--color-danger)';
    case 'HM': return 'var(--color-neutral)';
    case 'SUS':
    case 'DEF':
    case 'FAV':
    default: return 'var(--color-warning)';
  }
}

function getQualityColor(q: RowQuality): string {
  switch (q) {
    case 'OK': return 'var(--color-success)';
    case 'WARNING': return 'var(--color-warning)';
    case 'ERROR': return 'var(--color-danger)';
  }
}

function getLifecycleColor(lc: LifecycleState): string {
  switch (lc) {
    case 'READY_TO_ISSUE': return 'var(--color-success)';
    case 'READY_TO_REJECT': return 'var(--color-danger)';
    case 'NEEDS_ARBITRATION': return 'var(--color-arbitration)';
    case 'CHRONIC_BLOCKED': return 'var(--color-danger)';
    case 'WAITING_RESPONSES': return 'var(--color-info)';
    case 'ON_HOLD': return 'var(--color-warning)';
    case 'NOT_STARTED': return 'var(--color-neutral)';
    case 'EXCLUDED': return 'var(--color-neutral)';
  }
}

export default function Badge({
  label,
  variant,
  category,
  scoreBand,
  visaStatus,
  quality,
  lifecycle,
  onClick,
}: BadgeProps) {
  let bg: string;
  let color: string;
  let border: string | undefined;

  switch (variant) {
    case 'system':
      bg = 'var(--bg-tertiary)';
      color = 'var(--text-secondary)';
      border = undefined;
      break;
    case 'ai':
      bg = 'color-mix(in srgb, var(--color-accent) 15%, transparent)';
      color = 'var(--color-accent)';
      border = '1px solid color-mix(in srgb, var(--color-accent) 40%, transparent)';
      break;
    case 'category':
      {
        const catColor = category ? getCategoryColor(category) : 'var(--color-neutral)';
        bg = `color-mix(in srgb, ${catColor} 15%, transparent)`;
        color = catColor;
        border = undefined;
      }
      break;
    case 'score':
      {
        const sColor = scoreBand ? getScoreColor(scoreBand) : 'var(--color-neutral)';
        bg = `color-mix(in srgb, ${sColor} 15%, transparent)`;
        color = sColor;
        border = undefined;
      }
      break;
    case 'lot':
      bg = 'var(--bg-tertiary)';
      color = 'var(--text-secondary)';
      border = undefined;
      break;
    case 'status':
      {
        const vColor = getVisaColor(visaStatus ?? null);
        bg = `color-mix(in srgb, ${vColor} 15%, transparent)`;
        color = vColor;
        border = undefined;
      }
      break;
    case 'lifecycle':
      {
        const lcColor = lifecycle ? getLifecycleColor(lifecycle) : 'var(--color-neutral)';
        bg = `color-mix(in srgb, ${lcColor} 15%, transparent)`;
        color = lcColor;
        border = undefined;
      }
      break;
    case 'quality':
      {
        const qColor = quality ? getQualityColor(quality) : 'var(--color-neutral)';
        bg = `color-mix(in srgb, ${qColor} 15%, transparent)`;
        color = qColor;
        border = undefined;
      }
      break;
    default:
      bg = 'var(--bg-tertiary)';
      color = 'var(--text-secondary)';
  }

  const baseStyle: React.CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '2px 8px',
    borderRadius: 'var(--radius-badge)',
    background: bg,
    color,
    border: border ?? 'none',
    fontSize: 11,
    fontWeight: 600,
    lineHeight: '18px',
    whiteSpace: 'nowrap',
    letterSpacing: '0.02em',
    cursor: onClick ? 'pointer' : 'default',
    transition: 'background 120ms ease',
  };

  if (onClick) {
    return (
      <button
        onClick={onClick}
        style={baseStyle}
        tabIndex={0}
        aria-label={label}
      >
        {label}
      </button>
    );
  }

  return <span style={baseStyle}>{label}</span>;
}
