import type { Document } from '../types/document';
import { useTranslation } from '../i18n/use-translation';
import Badge from './Badge';

interface CrossLotSidebarProps {
  doc: Document;
}

export default function CrossLotSidebar({ doc }: CrossLotSidebarProps) {
  const { t } = useTranslation();

  if (!doc.is_cross_lot || doc.cross_lot_sheets === null) {
    return null;
  }

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
          {t('ws_cross_lot_title')}
        </span>
        <Badge variant="system" label="M2" />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {doc.cross_lot_sheets.map((sheet) => (
          <Badge key={sheet} variant="lot" label={sheet} />
        ))}
      </div>
    </div>
  );
}
