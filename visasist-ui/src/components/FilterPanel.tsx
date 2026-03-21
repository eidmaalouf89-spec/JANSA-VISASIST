import { useState } from 'react';
import type { Category, ScoreBand } from '../types';
import { useTranslation } from '../i18n/use-translation';
import type { QueueFilters } from '../selectors/queue-selectors';
import { DEFAULT_QUEUE_FILTERS } from '../selectors/queue-selectors';
import { ALL_CATEGORIES, CATEGORY_LABEL_MAP, getCategoryColor } from '../selectors/category-constants';

interface FilterPanelProps {
  filters: QueueFilters;
  onChange: (filters: QueueFilters) => void;
  availableLots: string[];
  availableApprovers: string[];
  totalCount: number;
  filteredCount: number;
}

const SCORE_BANDS: ScoreBand[] = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'];

function getBandColor(band: ScoreBand): string {
  switch (band) {
    case 'CRITICAL': return 'var(--color-danger)';
    case 'HIGH': return 'var(--color-warning)';
    case 'MEDIUM': return 'var(--color-info)';
    case 'LOW': return 'var(--color-neutral)';
  }
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const sa = [...a].sort();
  const sb = [...b].sort();
  return sa.every((v, i) => v === sb[i]);
}

function computeActiveCount(filters: QueueFilters): number {
  let count = 0;
  if (filters.overdueOnly !== DEFAULT_QUEUE_FILTERS.overdueOnly) count++;
  if (filters.pendingOnly !== DEFAULT_QUEUE_FILTERS.pendingOnly) count++;
  if (filters.latestRevision !== DEFAULT_QUEUE_FILTERS.latestRevision) count++;
  if (filters.categories.length > 0) count++;
  if (filters.lots.length > 0) count++;
  if (!arraysEqual(filters.scoreBands, DEFAULT_QUEUE_FILTERS.scoreBands)) count++;
  if (filters.approver !== null) count++;
  return count;
}

// ─── Styles ───────────────────────────────────────────────────────

const panelStyle: React.CSSProperties = {
  width: 196,
  flexShrink: 0,
  display: 'flex',
  flexDirection: 'column',
  borderRight: '1px solid var(--border-default)',
  background: 'var(--bg-secondary)',
  overflow: 'hidden',
};

const headerStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: '12px 12px 8px',
  flexShrink: 0,
};

const sectionStyle: React.CSSProperties = {
  padding: '8px 12px',
  borderTop: '1px solid var(--border-default)',
};

const sectionLabelStyle: React.CSSProperties = {
  fontSize: 10.5,
  fontWeight: 700,
  textTransform: 'uppercase' as const,
  letterSpacing: '0.5px',
  color: 'var(--text-tertiary)',
  marginBottom: 6,
};

const checkRowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 6,
  padding: '2px 0',
  minWidth: 0,
};

const labelTextStyle: React.CSSProperties = {
  fontSize: 12,
  color: 'var(--text-primary)',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  minWidth: 0,
};

const toggleRowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: '3px 0',
  minWidth: 0,
};

const toggleLabelStyle: React.CSSProperties = {
  fontSize: 12,
  color: 'var(--text-primary)',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  minWidth: 0,
  flex: 1,
};

// ─── Toggle Switch ────────────────────────────────────────────────

function ToggleSwitch({ checked, onChange }: { checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <button
      onClick={() => onChange(!checked)}
      style={{
        width: 28,
        height: 16,
        borderRadius: 8,
        border: 'none',
        background: checked ? 'var(--color-accent)' : 'var(--bg-tertiary)',
        position: 'relative',
        cursor: 'pointer',
        flexShrink: 0,
        transition: 'background 120ms ease',
      }}
    >
      <div
        style={{
          width: 12,
          height: 12,
          borderRadius: '50%',
          background: 'var(--text-primary)',
          position: 'absolute',
          top: 2,
          left: checked ? 14 : 2,
          transition: 'left 120ms ease',
        }}
      />
    </button>
  );
}

// ─── Component ────────────────────────────────────────────────────

export default function FilterPanel({
  filters,
  onChange,
  availableLots,
  availableApprovers,
  totalCount,
  filteredCount,
}: FilterPanelProps) {
  const { t } = useTranslation();
  const [lotSearch, setLotSearch] = useState('');
  const [approverExpanded, setApproverExpanded] = useState(false);

  const activeCount = computeActiveCount(filters);
  const filteredLots = lotSearch
    ? availableLots.filter((l) => l.toLowerCase().includes(lotSearch.toLowerCase()))
    : availableLots;

  function toggleCategory(cat: Category) {
    const cats = filters.categories.includes(cat)
      ? filters.categories.filter((c) => c !== cat)
      : [...filters.categories, cat];
    onChange({ ...filters, categories: cats });
  }

  function toggleLot(lot: string) {
    const lots = filters.lots.includes(lot)
      ? filters.lots.filter((l) => l !== lot)
      : [...filters.lots, lot];
    onChange({ ...filters, lots });
  }

  function toggleScoreBand(band: ScoreBand) {
    const bands = filters.scoreBands.includes(band)
      ? filters.scoreBands.filter((b) => b !== band)
      : [...filters.scoreBands, band];
    onChange({ ...filters, scoreBands: bands });
  }

  function toggleApprover(approver: string) {
    onChange({
      ...filters,
      approver: filters.approver === approver ? null : approver,
    });
  }

  return (
    <div style={panelStyle}>
      {/* Header */}
      <div style={headerStyle}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
            Filters
          </span>
          {activeCount > 0 && (
            <span
              style={{
                fontSize: 10,
                fontWeight: 600,
                color: 'var(--color-accent)',
                background: 'color-mix(in srgb, var(--color-accent) 15%, transparent)',
                padding: '1px 6px',
                borderRadius: 'var(--radius-badge)',
              }}
            >
              {t('filter_active_count').replace('{n}', String(activeCount))}
            </span>
          )}
        </div>
        {activeCount > 0 && (
          <button
            onClick={() => onChange(DEFAULT_QUEUE_FILTERS)}
            style={{
              fontSize: 11,
              color: 'var(--text-secondary)',
              background: 'none',
              border: 'none',
              cursor: 'pointer',
              padding: 0,
            }}
          >
            {t('filter_reset')}
          </button>
        )}
      </div>

      {/* Count summary */}
      <div style={{ padding: '0 12px 8px', fontSize: 11, color: 'var(--text-tertiary)' }}>
        {filteredCount} / {totalCount}
      </div>

      {/* Scrollable body */}
      <div style={{ flex: 1, overflowY: 'auto', overflowX: 'hidden' }}>
        {/* Quick toggles */}
        <div style={sectionStyle}>
          <div style={sectionLabelStyle}>{t('filter_quick')}</div>
          <div style={toggleRowStyle}>
            <span style={toggleLabelStyle}>{t('filter_overdue_only')}</span>
            <ToggleSwitch
              checked={filters.overdueOnly}
              onChange={(v) => onChange({ ...filters, overdueOnly: v })}
            />
          </div>
          <div style={toggleRowStyle}>
            <span style={toggleLabelStyle}>{t('filter_pending_only')}</span>
            <ToggleSwitch
              checked={filters.pendingOnly}
              onChange={(v) => onChange({ ...filters, pendingOnly: v })}
            />
          </div>
          <div style={toggleRowStyle}>
            <span style={toggleLabelStyle}>{t('filter_latest_revision')}</span>
            <ToggleSwitch
              checked={filters.latestRevision}
              onChange={(v) => onChange({ ...filters, latestRevision: v })}
            />
          </div>
        </div>

        {/* Category */}
        <div style={sectionStyle}>
          <div style={sectionLabelStyle}>{t('filter_category')}</div>
          {ALL_CATEGORIES.map((cat) => (
            <div key={cat} style={checkRowStyle}>
              <input
                type="checkbox"
                checked={filters.categories.includes(cat)}
                onChange={() => toggleCategory(cat)}
                style={{ accentColor: 'var(--color-accent)', flexShrink: 0 }}
              />
              <span
                style={{
                  width: 8,
                  height: 8,
                  borderRadius: '50%',
                  background: getCategoryColor(cat),
                  flexShrink: 0,
                }}
              />
              <span style={labelTextStyle}>{t(CATEGORY_LABEL_MAP[cat])}</span>
            </div>
          ))}
        </div>

        {/* Lot */}
        <div style={sectionStyle}>
          <div style={sectionLabelStyle}>{t('filter_lot')}</div>
          <input
            type="text"
            value={lotSearch}
            onChange={(e) => setLotSearch(e.target.value)}
            placeholder={t('filter_lot_search_placeholder')}
            style={{
              width: '100%',
              padding: '4px 8px',
              fontSize: 11,
              background: 'var(--bg-tertiary)',
              border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-badge)',
              color: 'var(--text-primary)',
              marginBottom: 4,
              boxSizing: 'border-box',
            }}
          />
          <div style={{ maxHeight: 120, overflowY: 'auto' }}>
            {filteredLots.map((lot) => (
              <div key={lot} style={checkRowStyle}>
                <input
                  type="checkbox"
                  checked={filters.lots.includes(lot)}
                  onChange={() => toggleLot(lot)}
                  style={{ accentColor: 'var(--color-accent)', flexShrink: 0 }}
                />
                <span style={labelTextStyle}>{lot}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Score band */}
        <div style={sectionStyle}>
          <div style={sectionLabelStyle}>{t('filter_score_band')}</div>
          {SCORE_BANDS.map((band) => (
            <div key={band} style={checkRowStyle}>
              <input
                type="checkbox"
                checked={filters.scoreBands.includes(band)}
                onChange={() => toggleScoreBand(band)}
                style={{ accentColor: 'var(--color-accent)', flexShrink: 0 }}
              />
              <span style={{ ...labelTextStyle, color: getBandColor(band) }}>{band}</span>
            </div>
          ))}
        </div>

        {/* Approver */}
        <div style={sectionStyle}>
          <div
            style={{ ...sectionLabelStyle, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}
            onClick={() => setApproverExpanded(!approverExpanded)}
          >
            {t('filter_approver')}
            <span style={{ fontSize: 10 }}>{approverExpanded ? '▾' : '▸'}</span>
          </div>
          {approverExpanded && (
            <div style={{ maxHeight: 140, overflowY: 'auto' }}>
              {availableApprovers.map((approver) => (
                <div key={approver} style={checkRowStyle}>
                  <input
                    type="radio"
                    name="approver-filter"
                    checked={filters.approver === approver}
                    onChange={() => toggleApprover(approver)}
                    style={{ accentColor: 'var(--color-accent)', flexShrink: 0 }}
                  />
                  <span style={labelTextStyle}>{approver}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
