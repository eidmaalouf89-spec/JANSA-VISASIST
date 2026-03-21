import { useState, useEffect, useMemo } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { useTranslation } from '../../i18n/use-translation';
import type { TranslationKey } from '../../i18n/en';
import { documentDetail } from '../../mock/document-detail';
import { revisions } from '../../mock/revisions';
import { anomalyLogs } from '../../mock/anomaly-logs';
import {
  getDocumentByKey,
  getRevisionsForDocument,
  getAnomalyLogsForDocument,
  computeApproverCounts,
} from '../../selectors/workspace-selectors';
import DocumentHeader from '../../components/DocumentHeader';
import DecisionZone from '../../components/DecisionZone';
import DocumentMetaGrid from '../../components/DocumentMetaGrid';
import Timeline from '../../components/Timeline';
import ApproverTable from '../../components/ApproverTable';
import WorkspaceAnalysisTab from '../../components/WorkspaceAnalysisTab';
import RawNormGrid from '../../components/RawNormGrid';
import WorkspaceLogsTab from '../../components/WorkspaceLogsTab';
import ApproverSidebar from '../../components/ApproverSidebar';
import ScoreBreakdownSidebar from '../../components/ScoreBreakdownSidebar';
import CrossLotSidebar from '../../components/CrossLotSidebar';

const TABS: TranslationKey[] = [
  'ws_tab_overview',
  'ws_tab_history',
  'ws_tab_approvers',
  'ws_tab_analysis',
  'ws_tab_raw_norm',
  'ws_tab_logs',
];

type TabIndex = 0 | 1 | 2 | 3 | 4 | 5;

export default function WorkspaceScreen() {
  const { t } = useTranslation();
  const navigate = useNavigate();

  const { docVersionKey: rawKey } = useParams<{ docVersionKey: string }>();
  const docVersionKey = rawKey ? decodeURIComponent(rawKey) : null;

  const [searchParams, setSearchParams] = useSearchParams();
  const [activeTab, setActiveTab] = useState<TabIndex>(() => {
    const tab = searchParams.get('tab');
    const n = Number(tab);
    return [0, 1, 2, 3, 4, 5].includes(n) ? (n as TabIndex) : 0;
  });

  // Derived — selector composition only
  const doc = useMemo(
    () =>
      docVersionKey
        ? getDocumentByKey([documentDetail], docVersionKey)
        : undefined,
    [docVersionKey],
  );
  const docRevisions = useMemo(
    () => (doc ? getRevisionsForDocument(revisions, doc) : []),
    [doc],
  );
  const docLogs = useMemo(
    () => (doc ? getAnomalyLogsForDocument(anomalyLogs, doc) : []),
    [doc],
  );
  const approverCounts = useMemo(
    () => (doc ? computeApproverCounts(doc.approvers) : undefined),
    [doc],
  );

  // URL sync (tab only)
  useEffect(() => {
    const params: Record<string, string> = {};
    if (activeTab !== 0) params.tab = String(activeTab);
    setSearchParams(params, { replace: true });
  }, [activeTab, setSearchParams]);

  // Not-found state
  if (!doc) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <h2 style={{ color: 'var(--text-primary)' }}>{t('ws_not_found')}</h2>
        <p style={{ color: 'var(--text-secondary)' }}>{docVersionKey}</p>
        <button
          onClick={() => navigate('/queue')}
          style={{
            marginTop: 12,
            padding: '6px 16px',
            fontSize: 13,
            cursor: 'pointer',
            background: 'var(--bg-tertiary)',
            border: '1px solid var(--border-default)',
            borderRadius: 'var(--radius-badge)',
            color: 'var(--text-primary)',
          }}
        >
          {t('ws_breadcrumb_queue')}
        </button>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', height: '100%', overflow: 'hidden' }}>
      {/* Main column */}
      <div
        style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        <DocumentHeader doc={doc} onBack={() => navigate('/queue')} />

        <DecisionZone
          doc={doc}
          onIssueVAO={() => {
            /* stub — transient feedback */
          }}
          onIssueREF={() => {
            /* stub */
          }}
          onChase={() => {
            /* stub */
          }}
          onHold={() => {
            /* stub */
          }}
        />

        {/* Tab bar */}
        <div
          style={{
            display: 'flex',
            flexShrink: 0,
            borderBottom: '1px solid var(--border-default)',
          }}
        >
          {TABS.map((labelKey, i) => (
            <button
              key={labelKey}
              onClick={() => setActiveTab(i as TabIndex)}
              style={{
                flex: 1,
                padding: '8px 4px',
                fontSize: 11,
                fontWeight: 600,
                background: 'transparent',
                border: 'none',
                borderBottom:
                  activeTab === i
                    ? '2px solid var(--color-accent)'
                    : '2px solid transparent',
                color:
                  activeTab === i
                    ? 'var(--color-accent)'
                    : 'var(--text-secondary)',
                cursor: 'pointer',
                whiteSpace: 'nowrap',
              }}
            >
              {t(labelKey)}
            </button>
          ))}
        </div>

        {/* Tab content — scrollable */}
        <div style={{ flex: 1, overflowY: 'auto', padding: 20 }}>
          {activeTab === 0 && <DocumentMetaGrid doc={doc} />}
          {activeTab === 1 && <Timeline revisions={docRevisions} />}
          {activeTab === 2 && (
            <ApproverTable
              approvers={doc.approvers}
              assignedKeys={doc.assigned_approvers}
              counts={approverCounts!}
            />
          )}
          {activeTab === 3 && <WorkspaceAnalysisTab doc={doc} />}
          {activeTab === 4 && <RawNormGrid doc={doc} />}
          {activeTab === 5 && (
            <WorkspaceLogsTab doc={doc} anomalyLogs={docLogs} />
          )}
        </div>
      </div>

      {/* Side column */}
      <div
        style={{
          width: 290,
          flexShrink: 0,
          display: 'flex',
          flexDirection: 'column',
          gap: 12,
          padding: 12,
          borderLeft: '1px solid var(--border-default)',
          overflowY: 'auto',
          background: 'var(--bg-secondary)',
        }}
      >
        <ApproverSidebar
          approvers={doc.approvers.filter((a) => a.is_assigned)}
        />
        <ScoreBreakdownSidebar doc={doc} />
        <CrossLotSidebar doc={doc} />
      </div>
    </div>
  );
}
