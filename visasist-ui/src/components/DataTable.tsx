import { useRef } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { QueueItem } from '../types';
import type { QueueSort } from '../selectors/queue-selectors';
import { useTranslation } from '../i18n/use-translation';
import type { TranslationKey } from '../i18n/en';
import TableRow from './TableRow';

// ─── Props ──────────────────────────────────────────────────────────

interface DataTableProps {
  rows: QueueItem[];
  selectedRows: string[];
  activeRowId: string | null;
  onRowClick: (key: string) => void;
  onCheckboxChange: (key: string, checked: boolean) => void;
  onSelectAll: (checked: boolean) => void;
  sortBy: QueueSort;
  onSort: (col: QueueSort['column']) => void;
}

// ─── Column config ──────────────────────────────────────────────────

interface ColConfig {
  labelKey: TranslationKey;
  width: number;
  sortKey?: QueueSort['column'];
}

const COLUMNS: ColConfig[] = [
  { labelKey: 'col_document', width: 220, sortKey: 'document' },
  { labelKey: 'col_lot', width: 100 },
  { labelKey: 'col_category', width: 130 },
  { labelKey: 'col_score', width: 70, sortKey: 'priority_score' },
  { labelKey: 'col_deadline', width: 100, sortKey: 'deadline' },
  { labelKey: 'col_overdue', width: 70, sortKey: 'days_overdue' },
  { labelKey: 'col_consensus', width: 100 },
  { labelKey: 'col_missing', width: 120 },
  { labelKey: 'col_rev', width: 50 },
  { labelKey: 'col_action', width: 130 },
];

const CHECKBOX_WIDTH = 40;
const ROW_HEIGHT = 44;

// ─── Sort indicator ─────────────────────────────────────────────────

function sortIndicator(col: ColConfig, sortBy: QueueSort): string {
  if (!col.sortKey) return '';
  if (sortBy.column === col.sortKey) {
    return sortBy.direction === 'asc' ? ' \u2191' : ' \u2193';
  }
  return ' \u2195';
}

// ─── Component ──────────────────────────────────────────────────────

export default function DataTable({
  rows,
  selectedRows,
  activeRowId,
  onRowClick,
  onCheckboxChange,
  onSelectAll,
  sortBy,
  onSort,
}: DataTableProps) {
  const { t } = useTranslation();
  const scrollRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 8,
  });

  const allSelected = rows.length > 0 && rows.every((r) => selectedRows.includes(r.doc_version_key));
  const someSelected = !allSelected && rows.some((r) => selectedRows.includes(r.doc_version_key));

  const totalWidth = CHECKBOX_WIDTH + COLUMNS.reduce((sum, c) => sum + c.width, 0);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: 'var(--bg-secondary)', borderRadius: 'var(--radius-card)', border: '1px solid var(--border-default)' }}>
      {/* Header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          height: 36,
          minWidth: totalWidth,
          position: 'sticky',
          top: 0,
          zIndex: 2,
          background: 'var(--bg-tertiary)',
          borderBottom: '1px solid var(--border-default)',
          flexShrink: 0,
        }}
      >
        {/* Select-all */}
        <div style={{ width: CHECKBOX_WIDTH, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
          <input
            type="checkbox"
            checked={allSelected}
            ref={(el) => { if (el) el.indeterminate = someSelected; }}
            onChange={() => onSelectAll(!allSelected)}
            style={{ accentColor: 'var(--color-accent)', cursor: 'pointer' }}
          />
        </div>
        {COLUMNS.map((col) => {
          const isActive = sortBy.column === col.sortKey;
          return (
            <div
              key={col.labelKey}
              onClick={col.sortKey ? () => onSort(col.sortKey!) : undefined}
              style={{
                width: col.width,
                flexShrink: 0,
                padding: '0 8px',
                fontSize: 10.5,
                fontWeight: 700,
                textTransform: 'uppercase',
                letterSpacing: '0.5px',
                color: isActive ? 'var(--color-accent)' : 'var(--text-secondary)',
                cursor: col.sortKey ? 'pointer' : 'default',
                userSelect: 'none',
                whiteSpace: 'nowrap',
                overflow: 'hidden',
              }}
            >
              {t(col.labelKey)}{sortIndicator(col, sortBy)}
            </div>
          );
        })}
      </div>

      {/* Virtualised body */}
      <div ref={scrollRef} style={{ flex: 1, overflow: 'auto', minWidth: totalWidth }}>
        {rows.length === 0 ? (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200, color: 'var(--text-tertiary)', fontSize: 13 }}>
            {t('empty_no_results')}
          </div>
        ) : (
          <div style={{ position: 'relative', height: virtualizer.getTotalSize(), width: '100%' }}>
            {virtualizer.getVirtualItems().map((virtualRow) => {
              const item = rows[virtualRow.index];
              return (
                <div
                  key={item.doc_version_key}
                  style={{
                    position: 'absolute',
                    top: virtualRow.start,
                    width: '100%',
                    height: ROW_HEIGHT,
                  }}
                >
                  <TableRow
                    item={item}
                    selected={selectedRows.includes(item.doc_version_key)}
                    active={activeRowId === item.doc_version_key}
                    onRowClick={() => onRowClick(item.doc_version_key)}
                    onCheckboxChange={(checked) => onCheckboxChange(item.doc_version_key, checked)}
                  />
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

// Re-export for backward compatibility (Dashboard might use it)
export type { DataTableProps };
