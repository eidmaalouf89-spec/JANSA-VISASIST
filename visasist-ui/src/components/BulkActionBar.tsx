import { useTranslation } from '../i18n/use-translation';

interface BulkActionBarProps {
  selectedCount: number;
  onIssue: () => void;
  onChase: () => void;
  onExport: () => void;
  onFlag: () => void;
  onClear: () => void;
}

const btnBase: React.CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  padding: '4px 12px',
  fontSize: 12,
  fontWeight: 500,
  borderRadius: 'var(--radius-badge)',
  cursor: 'pointer',
  whiteSpace: 'nowrap',
  transition: 'background 120ms ease',
};

const secondaryBtn: React.CSSProperties = {
  ...btnBase,
  background: 'transparent',
  border: '1px solid var(--border-default)',
  color: 'var(--text-primary)',
};

const dangerBtn: React.CSSProperties = {
  ...btnBase,
  background: 'transparent',
  border: '1px solid var(--color-danger)',
  color: 'var(--color-danger)',
};

const clearBtn: React.CSSProperties = {
  ...btnBase,
  background: 'transparent',
  border: 'none',
  color: 'var(--text-secondary)',
};

export default function BulkActionBar({
  selectedCount,
  onIssue,
  onChase,
  onExport,
  onFlag,
  onClear,
}: BulkActionBarProps) {
  const { t } = useTranslation();

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        padding: '8px 12px',
        background: 'var(--bg-tertiary)',
        borderBottom: '1px solid var(--border-default)',
        flexShrink: 0,
      }}
    >
      <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--color-accent)', marginRight: 4 }}>
        {selectedCount} {t('bulk_selected')}
      </span>
      <button style={secondaryBtn} onClick={onIssue}>{t('bulk_issue')}</button>
      <button style={secondaryBtn} onClick={onChase}>{t('bulk_chase')}</button>
      <button style={secondaryBtn} onClick={onExport}>{t('bulk_export')}</button>
      <button style={dangerBtn} onClick={onFlag}>{t('bulk_flag')}</button>
      <button style={clearBtn} onClick={onClear}>\u2715 {t('bulk_clear')}</button>
    </div>
  );
}
