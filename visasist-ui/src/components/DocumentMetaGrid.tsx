import type { Document } from '../types/document';
import { useTranslation } from '../i18n/use-translation';
import { formatRunDate } from '../i18n/format-date';
import type { TranslationKey } from '../i18n/en';

interface DocumentMetaGridProps {
  doc: Document;
}

export default function DocumentMetaGrid({ doc }: DocumentMetaGridProps) {
  const { t, lang } = useTranslation();

  const fields: Array<{
    labelKey: TranslationKey;
    value: string;
    mono?: boolean;
    span2?: boolean;
  }> = [
    { labelKey: 'ws_meta_reference', value: doc.document, mono: true },
    { labelKey: 'ws_meta_titre', value: doc.titre ?? '—' },
    { labelKey: 'ws_meta_lot', value: doc.lot ?? '—' },
    { labelKey: 'ws_meta_type', value: doc.type_doc ?? '—' },
    { labelKey: 'ws_meta_zone', value: doc.zone ?? '—' },
    { labelKey: 'ws_meta_niveau', value: doc.niv ?? '—' },
    { labelKey: 'ws_meta_n_doc', value: doc.n_doc ?? '—' },
    { labelKey: 'ws_meta_format', value: doc.type_format ?? '—' },
    { labelKey: 'ws_meta_n_bdx', value: doc.n_bdx ?? '—' },
    {
      labelKey: 'ws_meta_date_diffusion',
      value: doc.date_diffusion ? formatRunDate(doc.date_diffusion, lang) : '—',
    },
    {
      labelKey: 'ws_meta_date_reception',
      value: doc.date_reception ? formatRunDate(doc.date_reception, lang) : '—',
    },
    {
      labelKey: 'ws_meta_observations',
      value: doc.observations ?? '—',
      span2: doc.observations !== null,
    },
  ];

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '12px 20px',
      }}
    >
      {fields.map((f) => (
        <div
          key={f.labelKey}
          style={f.span2 ? { gridColumn: '1 / -1' } : undefined}
        >
          <div
            style={{
              fontSize: 11,
              color: 'var(--text-tertiary)',
              marginBottom: 2,
            }}
          >
            {t(f.labelKey)}
          </div>
          <div
            style={{
              fontSize: 13,
              color: 'var(--text-primary)',
              fontFamily: f.mono ? 'var(--font-mono)' : undefined,
            }}
          >
            {f.value}
          </div>
        </div>
      ))}
    </div>
  );
}
