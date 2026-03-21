import type { Document } from '../types/document';
import { useTranslation } from '../i18n/use-translation';
import { formatRunDate } from '../i18n/format-date';

interface RawNormGridProps {
  doc: Document;
}

interface RawNormRow {
  field: string;
  raw: string;
  norm: string;
  normIsNull: boolean;
}

export default function RawNormGrid({ doc }: RawNormGridProps) {
  const { t, lang } = useTranslation();

  const rows: RawNormRow[] = [
    {
      field: 'document',
      raw: doc.document_raw,
      norm: doc.document,
      normIsNull: false,
    },
    {
      field: 'visa_global',
      raw: doc.visa_global_raw ?? '—',
      norm: doc.visa_global ?? '—',
      normIsNull: doc.visa_global === null,
    },
    {
      field: 'date_diffusion',
      raw: '—',
      norm: doc.date_diffusion ? formatRunDate(doc.date_diffusion, lang) : '—',
      normIsNull: doc.date_diffusion === null,
    },
    {
      field: 'date_contractuelle_visa',
      raw: '—',
      norm: doc.date_contractuelle_visa
        ? formatRunDate(doc.date_contractuelle_visa, lang)
        : '—',
      normIsNull: doc.date_contractuelle_visa === null,
    },
    {
      field: 'date_reception',
      raw: '—',
      norm: doc.date_reception ? formatRunDate(doc.date_reception, lang) : '—',
      normIsNull: doc.date_reception === null,
    },
  ];

  function isHighlighted(row: RawNormRow): boolean {
    // Row where raw ≠ normalised, or raw is non-null and normalised is null
    if (row.raw !== '—' && row.normIsNull) return true;
    if (row.raw !== '—' && row.raw !== row.norm) return true;
    return false;
  }

  const monoStyle: React.CSSProperties = {
    fontFamily: 'var(--font-mono)',
    fontSize: 12,
    padding: '6px 10px',
  };

  return (
    <table
      style={{
        width: '100%',
        borderCollapse: 'collapse',
        fontSize: 12,
      }}
    >
      <thead>
        <tr>
          <th
            style={{
              ...monoStyle,
              textAlign: 'left',
              color: 'var(--text-tertiary)',
              fontWeight: 600,
              borderBottom: '1px solid var(--border-default)',
            }}
          >
            {t('ws_raw_field')}
          </th>
          <th
            style={{
              ...monoStyle,
              textAlign: 'left',
              color: 'var(--text-tertiary)',
              fontWeight: 600,
              borderBottom: '1px solid var(--border-default)',
            }}
          >
            {t('ws_raw_raw')}
          </th>
          <th
            style={{
              ...monoStyle,
              textAlign: 'left',
              color: 'var(--text-tertiary)',
              fontWeight: 600,
              borderBottom: '1px solid var(--border-default)',
            }}
          >
            {t('ws_raw_norm')}
          </th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const highlight = isHighlighted(row);
          return (
            <tr
              key={row.field}
              style={{
                borderLeft: highlight
                  ? '3px solid var(--color-warning)'
                  : '3px solid transparent',
              }}
            >
              <td style={{ ...monoStyle, color: 'var(--text-primary)' }}>
                {row.field}
              </td>
              <td style={{ ...monoStyle, color: 'var(--text-primary)' }}>
                {row.raw}
              </td>
              <td
                style={{
                  ...monoStyle,
                  color: row.normIsNull
                    ? 'var(--color-danger)'
                    : 'var(--text-primary)',
                }}
              >
                {row.norm}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
