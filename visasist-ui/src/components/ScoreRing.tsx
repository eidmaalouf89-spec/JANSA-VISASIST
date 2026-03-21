import type { ScoreBand } from '../types';

interface ScoreRingProps {
  score: number;    // 0–100
  band: ScoreBand;
  size?: number;    // default 44
}

function getBandColor(band: ScoreBand): string {
  switch (band) {
    case 'CRITICAL': return 'var(--color-danger)';
    case 'HIGH': return 'var(--color-warning)';
    case 'MEDIUM': return 'var(--color-info)';
    case 'LOW': return 'var(--color-neutral)';
  }
}

export default function ScoreRing({ score, band, size = 44 }: ScoreRingProps) {
  const color = getBandColor(band);
  const pct = Math.max(0, Math.min(100, score));
  const ringWidth = Math.max(3, size * 0.12);
  const innerSize = size - ringWidth * 2;
  const fontSize = Math.max(10, size * 0.3);

  return (
    <div
      style={{
        position: 'relative',
        width: size,
        height: size,
        borderRadius: '50%',
        background: `conic-gradient(${color} ${pct}%, var(--bg-tertiary) 0)`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexShrink: 0,
      }}
    >
      <div
        style={{
          width: innerSize,
          height: innerSize,
          borderRadius: '50%',
          background: 'var(--bg-secondary)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <span
          style={{
            fontSize,
            fontWeight: 700,
            color,
            lineHeight: 1,
          }}
        >
          {Math.round(score)}
        </span>
      </div>
    </div>
  );
}
