import type { QueueItem, Revision, SuggestedAction } from '../types';
import type { TranslationKey } from '../i18n/en';
import { useTranslation } from '../i18n/use-translation';
import { formatRunDate } from '../i18n/format-date';
import { CATEGORY_LABEL_MAP } from '../selectors/category-constants';
import { computeScoreComponents } from '../selectors/score-breakdown';
import Badge from './Badge';
import ScoreRing from './ScoreRing';
import AIAdvisoryBox from './AIAdvisoryBox';
import Timeline from './Timeline';
import TraceabilityCard from './TraceabilityCard';

interface DetailPanelProps {
  item: QueueItem;
  revisions: Revision[];
  activeTab: 0 | 1 | 2 | 3;
  onTabChange: (tab: 0 | 1 | 2 | 3) => void;
  onOpenWorkspace: () => void;
}

const TEMPLATE_KEY_MAP: Record<SuggestedAction, TranslationKey> = {
  ISSUE_VISA: 'rec_template_issue_visa',
  ESCALATE: 'rec_template_escalate',
  ARBITRATE: 'rec_template_arbitrate',
  CHASE_APPROVERS: 'rec_template_chase_approvers',
  HOLD: 'rec_template_hold',
};

const TAB_LABELS = ['Overview', 'Approvers', 'History', 'Logs'] as const;

// ─── Score bar row ──────────────────────────────────────────────────

function ScoreBar({ label, value, max, color }: { label: string; value: number; max: number; color: string }) {
  const pct = max > 0 ? Math.min(1, value / max) : 0;
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
      <span style={{ width: 90, fontSize: 11, color: 'var(--text-secondary)', flexShrink: 0 }}>{label}</span>
      <div style={{ flex: 1, height: 6, borderRadius: 3, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
        <div style={{ width: `${pct * 100}%`, height: '100%', borderRadius: 3, background: color }} />
      </div>
      <span style={{ width: 30, fontSize: 11, fontWeight: 600, color, textAlign: 'right', flexShrink: 0 }}>+{value}</span>
    </div>
  );
}

// ─── Tab content renderers ──────────────────────────────────────────

function OverviewTab({ item, onOpenWorkspace, t }: { item: QueueItem; onOpenWorkspace: () => void; t: (k: TranslationKey) => string }) {
  const components = computeScoreComponents(item);

  // Why this rank
  const whyParts: string[] = [];
  if (item.is_overdue) whyParts.push(`${item.days_overdue}d overdue`);
  whyParts.push(item.consensus_type);
  whyParts.push(`Rev ${item.revision_count}`);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      {/* Why this rank */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
          <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>{t('why_rank_label')}</span>
          <Badge variant="system" label="M3" />
        </div>
        <div
          style={{
            borderLeft: '3px solid var(--color-accent)',
            padding: '8px 12px',
            background: 'color-mix(in srgb, var(--color-accent) 5%, transparent)',
            borderRadius: '0 4px 4px 0',
            fontSize: 12,
            color: 'var(--text-secondary)',
          }}
        >
          {whyParts.join(' \u00b7 ')}
        </div>
      </div>

      {/* Score breakdown */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
          <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>Score breakdown</span>
          <Badge variant="system" label="M3" />
        </div>
        <ScoreBar label="Overdue" value={components.overduePoints} max={40} color="var(--color-danger)" />
        <ScoreBar label="Deadline" value={components.deadlinePoints} max={25} color="var(--color-warning)" />
        <ScoreBar label="Completeness" value={components.completenessPoints} max={20} color="var(--color-info)" />
        <ScoreBar label="Rev. depth" value={components.revisionPoints} max={5} color="var(--color-neutral)" />
        {components.deadlinePenalty !== 0 && (
          <div style={{ fontSize: 11, color: 'var(--color-danger)', marginTop: 2 }}>
            Deadline penalty: {components.deadlinePenalty}
          </div>
        )}
        <div style={{ marginTop: 6, fontSize: 13, fontWeight: 700, color: 'var(--text-primary)' }}>
          Total: {item.priority_score}
        </div>
      </div>

      {/* AI recommendation */}
      <AIAdvisoryBox
        body={t(TEMPLATE_KEY_MAP[item.suggested_action])}
        isTemplate={true}
        actions={[{ label: t('open_workspace'), onClick: onOpenWorkspace, variant: 'ghost' }]}
      />
    </div>
  );
}

function ApproversTab({ item, t }: { item: QueueItem; t: (k: TranslationKey) => string }) {
  // Gather all unique approver keys
  const allApprovers = new Set<string>([
    ...item.missing_approvers,
    ...item.blocking_approvers,
  ]);
  const blockingSet = new Set(item.blocking_approvers);
  const missingSet = new Set(item.missing_approvers);

  // Responded estimate: blocking approvers have responded (with REF),
  // missing approvers have not responded yet
  const respondedCount = item.blocking_approvers.length;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {/* Summary counts */}
      <div style={{ display: 'flex', gap: 12 }}>
        <SummaryCount label={t('detail_responded')} count={respondedCount} color="var(--color-success)" />
        <SummaryCount label={t('detail_blocking')} count={item.blocking_approvers.length} color="var(--color-danger)" />
        <SummaryCount label={t('detail_missing')} count={item.missing_approvers.length} color="var(--color-warning)" />
      </div>

      {/* Per-approver rows */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {[...allApprovers].map((key) => {
          const isBlocking = blockingSet.has(key);
          const isMissing = missingSet.has(key);
          const dotColor = isBlocking
            ? 'var(--color-danger)'
            : isMissing
              ? 'var(--color-warning)'
              : 'var(--color-success)';
          const statusLabel = isBlocking ? 'REF' : isMissing ? 'Pending' : 'Responded';

          return (
            <div key={key} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0' }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: dotColor, flexShrink: 0 }} />
              <span style={{ flex: 1, fontSize: 12, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {key}
              </span>
              <span style={{ fontSize: 11, color: dotColor, fontWeight: 500 }}>{statusLabel}</span>
            </div>
          );
        })}
        {allApprovers.size === 0 && (
          <span style={{ fontSize: 12, color: 'var(--text-tertiary)' }}>No approver data</span>
        )}
      </div>
    </div>
  );
}

function SummaryCount({ label, count, color }: { label: string; count: number; color: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', flex: 1, padding: 8, background: 'var(--bg-tertiary)', borderRadius: 6 }}>
      <span style={{ fontSize: 18, fontWeight: 700, color }}>{count}</span>
      <span style={{ fontSize: 10, color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>{label}</span>
    </div>
  );
}

function LogsTab({ item }: { item: QueueItem }) {
  return (
    <TraceabilityCard
      fields={[
        { name: 'source_sheet', value: item.source_sheet },
        { name: 'source_row', value: item.source_row },
        { name: 'row_id', value: item.row_id },
        { name: 'doc_family_key', value: item.doc_family_key },
        { name: 'doc_version_key', value: item.doc_version_key },
        {
          name: 'row_quality',
          value: item.row_quality,
          highlight: item.row_quality === 'OK' ? 'ok' : item.row_quality === 'WARNING' ? 'warn' : 'error',
        },
        {
          name: 'duplicate_flag',
          value: item.duplicate_flag,
          highlight: item.duplicate_flag === 'UNIQUE' ? 'ok' : 'warn',
        },
        { name: 'is_latest', value: String(item.is_latest) },
        { name: 'is_cross_lot', value: String(item.is_cross_lot) },
      ]}
    />
  );
}

// ─── Main DetailPanel ───────────────────────────────────────────────

export default function DetailPanel({
  item,
  revisions,
  activeTab,
  onTabChange,
  onOpenWorkspace,
}: DetailPanelProps) {
  const { t, lang } = useTranslation();

  return (
    <div
      style={{
        width: 340,
        flexShrink: 0,
        display: 'flex',
        flexDirection: 'column',
        borderLeft: '1px solid var(--border-default)',
        background: 'var(--bg-secondary)',
        overflow: 'hidden',
      }}
    >
      {/* Header */}
      <div style={{ padding: '14px 16px 10px', flexShrink: 0 }}>
        {/* Document reference */}
        <div
          style={{
            fontFamily: 'var(--font-mono)',
            fontSize: 13,
            fontWeight: 600,
            color: 'var(--text-primary)',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
          title={item.document}
        >
          {item.document}
        </div>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {item.titre ?? '\u2014'}
        </div>

        {/* Badge row */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginTop: 8 }}>
          <Badge variant="category" category={item.category} label={t(CATEGORY_LABEL_MAP[item.category])} />
          {item.is_overdue && (
            <Badge variant="score" scoreBand="CRITICAL" label="OVERDUE" />
          )}
          {item.ind && <Badge variant="lot" label={`IND ${item.ind}`} />}
          <Badge variant="lot" label={item.source_sheet} />
        </div>

        {/* Score row */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginTop: 10 }}>
          <ScoreRing score={item.priority_score} band={item.score_band} size={44} />
          <div>
            <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>
              {item.score_band}
            </div>
            <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
              Rev {item.revision_count}
              {item.date_diffusion && (
                <> &middot; {formatRunDate(item.date_diffusion, lang)}</>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Tab bar */}
      <div style={{ display: 'flex', flexShrink: 0, borderBottom: '1px solid var(--border-default)' }}>
        {TAB_LABELS.map((label, i) => {
          const isActive = activeTab === i;
          return (
            <button
              key={label}
              onClick={() => onTabChange(i as 0 | 1 | 2 | 3)}
              style={{
                flex: 1,
                padding: '8px 4px',
                fontSize: 11,
                fontWeight: 600,
                background: 'transparent',
                border: 'none',
                borderBottom: isActive ? '2px solid var(--color-accent)' : '2px solid transparent',
                color: isActive ? 'var(--color-accent)' : 'var(--text-secondary)',
                cursor: 'pointer',
                whiteSpace: 'nowrap',
              }}
            >
              {label}
            </button>
          );
        })}
      </div>

      {/* Scrollable tab body */}
      <div style={{ flex: 1, overflowY: 'auto', padding: 16 }}>
        {activeTab === 0 && <OverviewTab item={item} onOpenWorkspace={onOpenWorkspace} t={t} />}
        {activeTab === 1 && <ApproversTab item={item} t={t} />}
        {activeTab === 2 && <Timeline revisions={revisions} />}
        {activeTab === 3 && <LogsTab item={item} />}
      </div>
    </div>
  );
}
