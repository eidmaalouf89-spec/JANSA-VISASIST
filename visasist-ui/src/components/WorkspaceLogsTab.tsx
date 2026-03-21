import type { Document } from '../types/document';
import type { AnomalyLog } from '../types/anomaly-log';
import { useTranslation } from '../i18n/use-translation';
import TraceabilityCard from './TraceabilityCard';
import Badge from './Badge';

interface WorkspaceLogsTabProps {
  doc: Document;
  anomalyLogs: AnomalyLog[];
}

function getSeverityIcon(severity: AnomalyLog['severity']): {
  symbol: string;
  color: string;
} {
  switch (severity) {
    case 'ERROR':
      return { symbol: '!', color: 'var(--color-danger)' };
    case 'WARNING':
      return { symbol: '!', color: 'var(--color-warning)' };
    case 'INFO':
      return { symbol: 'i', color: 'var(--color-info)' };
  }
}

export default function WorkspaceLogsTab({
  doc,
  anomalyLogs,
}: WorkspaceLogsTabProps) {
  const { t } = useTranslation();

  const traceFields = [
    { name: 'source_sheet', value: doc.source_sheet },
    { name: 'source_row', value: doc.source_row },
    { name: 'row_id', value: doc.row_id },
    { name: 'doc_family_key', value: doc.doc_family_key },
    { name: 'doc_version_key', value: doc.doc_version_key },
    {
      name: 'row_quality',
      value: doc.row_quality,
      highlight: (doc.row_quality === 'OK'
        ? 'ok'
        : doc.row_quality === 'WARNING'
          ? 'warn'
          : 'error') as 'ok' | 'warn' | 'error',
    },
    {
      name: 'duplicate_flag',
      value: doc.duplicate_flag,
      highlight: (doc.duplicate_flag === 'UNIQUE' ? 'ok' : 'warn') as
        | 'ok'
        | 'warn',
    },
    { name: 'lifecycle_state', value: doc.lifecycle_state },
    { name: 'is_latest', value: String(doc.is_latest) },
    { name: 'is_cross_lot', value: String(doc.is_cross_lot) },
    { name: 'assigned_approvers', value: doc.assigned_approvers.join(', ') },
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Section 1 — Traceability */}
      <TraceabilityCard fields={traceFields} />

      {/* Section 2 — Import anomalies */}
      {anomalyLogs.length > 0 ? (
        <div>
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              marginBottom: 8,
            }}
          >
            <span
              style={{
                fontSize: 13,
                fontWeight: 600,
                color: 'var(--text-primary)',
              }}
            >
              {t('ws_anomaly_title')}
            </span>
            <Badge variant="system" label={t('badge_m1_import')} />
          </div>

          <table
            style={{
              width: '100%',
              borderCollapse: 'collapse',
              fontSize: 12,
            }}
          >
            <thead>
              <tr>
                <th style={thStyle}></th>
                <th style={thStyle}>Category</th>
                <th style={thStyle}>{t('ws_anomaly_column')}</th>
                <th style={thStyle}>{t('ws_anomaly_raw')}</th>
                <th style={thStyle}>{t('ws_anomaly_action')}</th>
                <th style={thStyle}>{t('ws_anomaly_confidence')}</th>
              </tr>
            </thead>
            <tbody>
              {anomalyLogs.map((log) => {
                const icon = getSeverityIcon(log.severity);
                return (
                  <tr key={log.log_id}>
                    <td style={{ ...tdStyle, width: 24, textAlign: 'center' }}>
                      <span
                        style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          width: 16,
                          height: 16,
                          borderRadius: '50%',
                          background: `color-mix(in srgb, ${icon.color} 20%, transparent)`,
                          color: icon.color,
                          fontSize: 9,
                          fontWeight: 700,
                        }}
                      >
                        {icon.symbol}
                      </span>
                    </td>
                    <td style={tdStyle}>{log.category}</td>
                    <td style={tdStyle}>{log.column ?? '—'}</td>
                    <td style={tdStyle}>{log.raw_value ?? '—'}</td>
                    <td style={tdStyle}>{log.action_taken}</td>
                    <td style={tdStyle}>
                      {log.confidence !== null
                        ? `${(log.confidence * 100).toFixed(0)}%`
                        : '—'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            padding: 12,
            color: 'var(--color-success)',
            fontSize: 13,
          }}
        >
          <span style={{ fontSize: 16 }}>✓</span>
          {t('empty_no_anomalies')}
        </div>
      )}
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
