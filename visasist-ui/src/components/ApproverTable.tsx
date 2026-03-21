import type { Approver } from '../types/approver';
import type { ApproverCounts } from '../selectors/workspace-selectors';
import { useTranslation } from '../i18n/use-translation';
import { formatRunDate } from '../i18n/format-date';
import Badge from './Badge';

interface ApproverTableProps {
  approvers: Approver[];
  assignedKeys: string[];
  counts: ApproverCounts;
}

function getDotColor(statut: Approver['statut']): string {
  switch (statut) {
    case 'VAO':
    case 'VSO':
      return 'var(--color-success)';
    case 'REF':
      return 'var(--color-danger)';
    case 'HM':
      return 'var(--color-neutral)';
    case null:
      return 'var(--color-warning)';
    default:
      return 'var(--color-warning)';
  }
}

export default function ApproverTable({
  approvers,
  assignedKeys,
  counts,
}: ApproverTableProps) {
  const { t, lang } = useTranslation();

  return (
    <div>
      {/* Summary counts row */}
      <div
        style={{
          display: 'flex',
          gap: 16,
          marginBottom: 12,
          flexWrap: 'wrap',
        }}
      >
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {t('ws_approver_responded')}: <strong>{counts.responded}</strong>
        </span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {t('ws_approver_blocking')}: <strong>{counts.blocking}</strong>
        </span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {t('ws_approver_pending')}: <strong>{counts.pending}</strong>
        </span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          {t('ws_approver_hm')}: <strong>{counts.hm}</strong>
        </span>
      </div>

      {/* Table */}
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
        <thead>
          <tr>
            <th style={thStyle}></th>
            <th style={thStyle}>Name</th>
            <th style={thStyle}>Statut</th>
            <th style={thStyle}>Date</th>
            <th style={thStyle}>Visa #</th>
            <th style={thStyle}>Assigned</th>
          </tr>
        </thead>
        <tbody>
          {approvers.map((a) => {
            const isAssigned = assignedKeys.includes(a.canonical_key);
            const rowStyle: React.CSSProperties = {
              borderLeft: a.is_blocking
                ? '3px solid var(--color-danger)'
                : '3px solid transparent',
              opacity: !isAssigned ? 0.5 : 1,
            };
            const textColor = a.is_hm
              ? 'var(--text-tertiary)'
              : 'var(--text-primary)';

            return (
              <tr key={a.canonical_key} style={rowStyle}>
                <td style={{ ...tdStyle, width: 20 }}>
                  <span
                    style={{
                      display: 'inline-block',
                      width: 7,
                      height: 7,
                      borderRadius: '50%',
                      background: getDotColor(a.statut),
                    }}
                  />
                </td>
                <td style={{ ...tdStyle, color: textColor }}>{a.display_name}</td>
                <td style={tdStyle}>
                  {a.is_pending ? (
                    <Badge variant="status" visaStatus={null} label={t('ws_approver_pending')} />
                  ) : !isAssigned ? (
                    <span style={{ color: 'var(--text-tertiary)', fontSize: 11 }}>
                      {t('ws_approver_not_assigned')}
                    </span>
                  ) : (
                    <Badge
                      variant="status"
                      visaStatus={a.statut ?? null}
                      label={a.statut ?? '—'}
                    />
                  )}
                </td>
                <td style={{ ...tdStyle, color: textColor }}>
                  {a.date ? formatRunDate(a.date, lang) : '—'}
                </td>
                <td style={{ ...tdStyle, color: textColor }}>
                  {a.numero_visa ?? '—'}
                </td>
                <td style={{ ...tdStyle, color: textColor }}>
                  {isAssigned ? 'Yes' : 'No'}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  fontSize: 11,
  fontWeight: 600,
  color: 'var(--text-tertiary)',
  padding: '6px 8px',
  borderBottom: '1px solid var(--border-default)',
};

const tdStyle: React.CSSProperties = {
  padding: '6px 8px',
  fontSize: 12,
  color: 'var(--text-primary)',
  borderBottom: '1px solid var(--border-default)',
};
