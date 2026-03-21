import { useNavigate } from 'react-router-dom';
import { useTranslation } from '../i18n/use-translation';
import { ROUTES } from '../routes';
import Badge from './Badge';
import { truncate } from '../selectors/dashboard-selectors';
import type { AnomalyLog } from '../types';
import type { AnomalySeverity, AnomalyCategory } from '../types/anomaly-log';
import type { TranslationKey } from '../i18n/en';

function getSeverityColor(severity: AnomalySeverity): string {
  switch (severity) {
    case 'ERROR': return 'var(--color-danger)';
    case 'WARNING': return 'var(--color-warning)';
    case 'INFO': return 'var(--color-info)';
  }
}

function getSeverityIcon(severity: AnomalySeverity): string {
  switch (severity) {
    case 'ERROR': return 'E';
    case 'WARNING': return 'W';
    case 'INFO': return 'I';
  }
}

/**
 * Map anomaly category to its i18n key.
 * The translated message is user-facing; raw technical values (column, raw_value)
 * remain untranslated and are shown separately if needed.
 */
const ANOMALY_MESSAGE_MAP: Record<AnomalyCategory, TranslationKey> = {
  corrupted_date: 'anomaly_corrupted_date',
  unknown_status: 'anomaly_unknown_status',
  missing_field: 'anomaly_missing_field',
  fuzzy_match: 'anomaly_fuzzy_match',
  unparseable_document: 'anomaly_unparseable_document',
  trailing_punctuation: 'anomaly_trailing_punctuation',
  duplicate_row: 'anomaly_duplicate_row',
  missing_sheet: 'anomaly_missing_sheet',
  column_mismatch: 'anomaly_column_mismatch',
  revision_gap: 'anomaly_revision_gap',
};

interface RecentAnomaliesProps {
  anomalies: AnomalyLog[];
}

export default function RecentAnomalies({ anomalies }: RecentAnomaliesProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  return (
    <div
      style={{
        background: 'var(--bg-secondary)',
        borderRadius: 'var(--radius-card)',
        border: '1px solid var(--border-default)',
        padding: '16px',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
        <h3 style={{ fontSize: 12, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
          {t('dashboard_recent_anomalies')}
        </h3>
        <Badge label={t('badge_m1_import')} variant="system" />
      </div>

      {anomalies.length === 0 ? (
        <p style={{ fontSize: 12, color: 'var(--text-tertiary)' }}>{t('empty_no_anomalies')}</p>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          {anomalies.map((log) => {
            // Use i18n key for the message, fall back to raw action_taken if category unknown
            const messageKey = ANOMALY_MESSAGE_MAP[log.category];
            const message = messageKey ? t(messageKey) : log.action_taken;

            return (
              <a
                key={log.log_id}
                href={`${ROUTES.audit}?log=${log.log_id}`}
                onClick={(e) => {
                  e.preventDefault();
                  navigate(`${ROUTES.audit}?log=${log.log_id}`);
                }}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  padding: '6px 8px',
                  borderRadius: 6,
                  textDecoration: 'none',
                  cursor: 'pointer',
                  transition: 'background 120ms ease',
                }}
                onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-tertiary)'; }}
                onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
              >
                {/* Severity icon */}
                <div
                  style={{
                    width: 20,
                    height: 20,
                    borderRadius: '50%',
                    background: `color-mix(in srgb, ${getSeverityColor(log.severity)} 20%, transparent)`,
                    color: getSeverityColor(log.severity),
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: 10,
                    fontWeight: 700,
                    flexShrink: 0,
                  }}
                >
                  {getSeverityIcon(log.severity)}
                </div>

                {/* Translated message */}
                <span style={{ fontSize: 12, color: 'var(--text-primary)', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {truncate(message, 80)}
                </span>

                {/* Source — raw technical values stay untranslated */}
                <span style={{ fontSize: 10, color: 'var(--text-tertiary)', whiteSpace: 'nowrap', flexShrink: 0 }}>
                  {log.sheet ?? '\u2014'} \u00b7 row {log.row}
                </span>
              </a>
            );
          })}
        </div>
      )}
    </div>
  );
}
