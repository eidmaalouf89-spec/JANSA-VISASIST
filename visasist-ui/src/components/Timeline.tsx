import type { Revision } from '../types';
import { useTranslation } from '../i18n/use-translation';
import { formatRunDate } from '../i18n/format-date';
import Badge from './Badge';
import type { VisaStatus } from '../types';

interface TimelineProps {
  revisions: Revision[];
}

function getNodeColor(rev: Revision): string {
  if (rev.is_latest) return 'var(--color-accent)';
  if (rev.visa_global === 'REF') return 'var(--color-danger)';
  return 'var(--color-neutral)';
}

function getApproverDotColor(statut: string | null): string {
  if (!statut) return 'var(--color-warning)';
  switch (statut) {
    case 'VAO':
    case 'VSO': return 'var(--color-success)';
    case 'REF': return 'var(--color-danger)';
    case 'HM': return 'var(--color-neutral)';
    default: return 'var(--color-warning)';
  }
}

export default function Timeline({ revisions }: TimelineProps) {
  const { t, lang } = useTranslation();

  if (revisions.length === 0) {
    return (
      <div style={{ padding: 16, color: 'var(--text-secondary)', fontSize: 13 }}>
        {t('empty_no_revisions')}
      </div>
    );
  }

  const sorted = [...revisions].sort((a, b) => a.ind_sort_order - b.ind_sort_order);

  return (
    <div style={{ position: 'relative', padding: '8px 0' }}>
      {sorted.map((rev, i) => {
        const nodeColor = getNodeColor(rev);
        const isLast = i === sorted.length - 1;

        return (
          <div key={`${rev.doc_version_key}-${rev.ind_sort_order}-${i}`} style={{ display: 'flex', gap: 12, marginBottom: isLast ? 0 : 12 }}>
            {/* Track line + node */}
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', width: 16, flexShrink: 0 }}>
              <div style={{ width: 10, height: 10, borderRadius: '50%', background: nodeColor, flexShrink: 0 }} />
              {!isLast && (
                <div style={{ width: 2, flex: 1, background: 'var(--border-default)', minHeight: 20 }} />
              )}
            </div>

            {/* Content card */}
            <div style={{ flex: 1, background: 'var(--bg-tertiary)', borderRadius: 6, padding: '8px 10px', minWidth: 0 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <span style={{ fontWeight: 600, fontSize: 12, color: 'var(--text-primary)' }}>
                  IND {rev.ind ?? '—'}
                </span>
                {rev.date_diffusion && (
                  <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                    {formatRunDate(rev.date_diffusion, lang)}
                  </span>
                )}
              </div>
              <div style={{ marginBottom: 6 }}>
                <Badge
                  variant="status"
                  visaStatus={(rev.visa_global as VisaStatus) ?? null}
                  label={rev.visa_global ?? '—'}
                />
              </div>
              {/* Approver mini-badges */}
              {rev.approver_summary.length > 0 && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                  {rev.approver_summary.map((a) => (
                    <span
                      key={a.canonical_key}
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 4,
                        fontSize: 10,
                        color: 'var(--text-secondary)',
                      }}
                    >
                      <span
                        style={{
                          width: 6,
                          height: 6,
                          borderRadius: '50%',
                          background: getApproverDotColor(a.statut),
                          flexShrink: 0,
                        }}
                      />
                      {a.display_name.charAt(0)}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
