import type { Approver } from '../types/approver';
import { useTranslation } from '../i18n/use-translation';
import Badge from './Badge';

interface ApproverSidebarProps {
  approvers: Approver[];
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

const MAX_ROWS = 6;

export default function ApproverSidebar({ approvers }: ApproverSidebarProps) {
  const { t } = useTranslation();

  // Only show assigned approvers
  const assigned = approvers.filter((a) => a.is_assigned);
  const visible = assigned.slice(0, MAX_ROWS);
  const remaining = assigned.length - MAX_ROWS;

  return (
    <div
      style={{
        background: 'var(--bg-tertiary)',
        borderRadius: 6,
        padding: 10,
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 6,
          marginBottom: 8,
        }}
      >
        <span
          style={{
            fontSize: 11,
            fontWeight: 600,
            color: 'var(--text-primary)',
          }}
        >
          {t('ws_approver_responded')}
        </span>
        <Badge variant="system" label="M1" />
      </div>

      {visible.map((a) => (
        <div
          key={a.canonical_key}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            padding: '3px 0',
          }}
        >
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: '50%',
              background: getDotColor(a.statut),
              flexShrink: 0,
            }}
          />
          <span
            style={{
              fontSize: 11,
              color: 'var(--text-primary)',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              flex: 1,
              minWidth: 0,
            }}
          >
            {a.display_name}
          </span>
          <span
            style={{
              fontSize: 10,
              color: 'var(--text-tertiary)',
              flexShrink: 0,
            }}
          >
            {a.statut ?? '—'}
          </span>
        </div>
      ))}

      {remaining > 0 && (
        <div
          style={{
            fontSize: 10,
            color: 'var(--text-tertiary)',
            marginTop: 4,
          }}
        >
          +{remaining} more
        </div>
      )}
    </div>
  );
}
