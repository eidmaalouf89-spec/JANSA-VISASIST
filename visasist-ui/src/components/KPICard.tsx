import { useNavigate } from 'react-router-dom';

interface KPICardProps {
  label: string;
  value: number;
  delta: number | null;
  accentColor: string;
  destination: string;
}

export default function KPICard({ label, value, delta, accentColor, destination }: KPICardProps) {
  const navigate = useNavigate();

  const handleClick = () => navigate(destination);
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      navigate(destination);
    }
  };

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      aria-label={`${label}: ${value}`}
      style={{
        background: 'var(--bg-secondary)',
        borderRadius: 'var(--radius-card)',
        padding: '16px',
        cursor: 'pointer',
        position: 'relative',
        overflow: 'hidden',
        border: '1px solid var(--border-default)',
        transition: 'border-color 120ms ease',
      }}
      onMouseEnter={(e) => { e.currentTarget.style.borderColor = accentColor; }}
      onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--border-default)'; }}
    >
      {/* Top accent line */}
      <div
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          height: 2,
          background: accentColor,
        }}
      />

      <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
        {label}
      </div>

      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
        <span style={{ fontSize: 28, fontWeight: 700, color: 'var(--text-primary)', lineHeight: 1 }}>
          {value.toLocaleString()}
        </span>
        {delta !== null && delta !== undefined && (
          <span
            style={{
              fontSize: 12,
              fontWeight: 600,
              color: delta > 0 ? 'var(--color-danger)' : delta < 0 ? 'var(--color-success)' : 'var(--text-tertiary)',
            }}
          >
            {delta > 0 ? `+${delta}` : delta}
          </span>
        )}
      </div>
    </div>
  );
}
