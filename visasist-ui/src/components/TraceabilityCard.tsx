interface TraceabilityCardProps {
  fields: Array<{
    name: string;
    value: string | number | boolean | null;
    highlight?: 'ok' | 'warn' | 'error';
  }>;
}

function getHighlightColor(h?: 'ok' | 'warn' | 'error'): string | undefined {
  if (!h) return undefined;
  switch (h) {
    case 'ok': return 'var(--color-success)';
    case 'warn': return 'var(--color-warning)';
    case 'error': return 'var(--color-danger)';
  }
}

export default function TraceabilityCard({ fields }: TraceabilityCardProps) {
  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'auto 1fr',
        gap: '4px 12px',
        padding: 12,
        background: 'var(--bg-tertiary)',
        borderRadius: 6,
      }}
    >
      {fields.map((f) => {
        const color = getHighlightColor(f.highlight);
        const displayValue = f.value === null || f.value === undefined ? '\u2014' : String(f.value);

        return (
          <div key={f.name} style={{ display: 'contents' }}>
            <span
              style={{
                fontFamily: 'var(--font-mono)',
                fontSize: 11,
                color: 'var(--text-tertiary)',
                whiteSpace: 'nowrap',
              }}
            >
              {f.name}
            </span>
            <span
              style={{
                fontFamily: 'var(--font-mono)',
                fontSize: 11,
                color: color ?? 'var(--text-primary)',
                fontWeight: color ? 600 : 400,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {displayValue}
            </span>
          </div>
        );
      })}
    </div>
  );
}
