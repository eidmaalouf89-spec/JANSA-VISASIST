import { useRef, useCallback, useMemo } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { QueueItem, ScoreBand, Category } from '../types';
import type { QueueSort } from '../selectors/queue-selectors';
import Badge from './Badge';

// ─── Column definition ──────────────────────────────────────────────

export interface ColumnDef {
  key: string;
  header: string;
  width: number;           // px
  minWidth?: number;
  sortKey?: QueueSort['column']; // if sortable, maps to QueueSort column
  render: (item: QueueItem) => React.ReactNode;
}

// ─── Props ──────────────────────────────────────────────────────────

interface DataTableProps {
  rows: QueueItem[];
  columns: ColumnDef[];
  sort: QueueSort;
  onSortChange: (sort: QueueSort) => void;
  selectedKeys: Set<string>;
  onSelectionChange: (keys: Set<string>) => void;
  onRowClick?: (item: QueueItem) => void;
  rowHeight?: number;
  overscan?: number;
  emptyMessage?: string;
}

// ─── Constants ──────────────────────────────────────────────────────

const ROW_HEIGHT = 40;
const HEADER_HEIGHT = 36;
const CHECKBOX_WIDTH = 40;

// ─── Styles ─────────────────────────────────────────────────────────

const tableWrapperStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  border: '1px solid var(--border-default)',
  borderRadius: 'var(--radius-card)',
  overflow: 'hidden',
  background: 'var(--bg-secondary)',
};

const headerRowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  height: HEADER_HEIGHT,
  borderBottom: '1px solid var(--border-default)',
  background: 'var(--bg-tertiary)',
  flexShrink: 0,
  paddingRight: 8, // scrollbar compensation
};

const headerCellStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  padding: '0 8px',
  fontSize: 11,
  fontWeight: 600,
  color: 'var(--text-secondary)',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  userSelect: 'none',
};

const sortableHeaderStyle: React.CSSProperties = {
  ...headerCellStyle,
  cursor: 'pointer',
};

const bodyCellStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  padding: '0 8px',
  fontSize: 13,
  color: 'var(--text-primary)',
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
};

const checkboxStyle: React.CSSProperties = {
  width: 14,
  height: 14,
  accentColor: 'var(--color-accent)',
  cursor: 'pointer',
};

// ─── Sort arrow helper ──────────────────────────────────────────────

function SortArrow({ direction }: { direction: 'asc' | 'desc' }) {
  return (
    <span
      style={{
        fontSize: 10,
        lineHeight: 1,
        color: 'var(--color-accent)',
        marginLeft: 2,
      }}
      aria-hidden
    >
      {direction === 'asc' ? '▲' : '▼'}
    </span>
  );
}

// ─── Component ──────────────────────────────────────────────────────

export default function DataTable({
  rows,
  columns,
  sort,
  onSortChange,
  selectedKeys,
  onSelectionChange,
  onRowClick,
  rowHeight = ROW_HEIGHT,
  overscan = 10,
  emptyMessage = 'No items match your current filters.',
}: DataTableProps) {
  const parentRef = useRef<HTMLDivElement>(null);

  // Virtualizer — only virtualizes rows
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => rowHeight,
    overscan,
  });

  // ── Selection handlers ──────────────────────────────────────────

  const allSelected = rows.length > 0 && rows.every((r) => selectedKeys.has(r.doc_version_key));
  const someSelected = !allSelected && rows.some((r) => selectedKeys.has(r.doc_version_key));

  const toggleAll = useCallback(() => {
    if (allSelected) {
      onSelectionChange(new Set());
    } else {
      onSelectionChange(new Set(rows.map((r) => r.doc_version_key)));
    }
  }, [allSelected, rows, onSelectionChange]);

  const toggleRow = useCallback(
    (key: string) => {
      const next = new Set(selectedKeys);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      onSelectionChange(next);
    },
    [selectedKeys, onSelectionChange],
  );

  // ── Sort handler ────────────────────────────────────────────────

  const handleSort = useCallback(
    (sortKey: QueueSort['column']) => {
      if (sort.column === sortKey) {
        onSortChange({ column: sortKey, direction: sort.direction === 'asc' ? 'desc' : 'asc' });
      } else {
        onSortChange({ column: sortKey, direction: 'desc' });
      }
    },
    [sort, onSortChange],
  );

  // ── Total width ─────────────────────────────────────────────────

  const totalWidth = useMemo(
    () => CHECKBOX_WIDTH + columns.reduce((sum, c) => sum + c.width, 0),
    [columns],
  );

  // ── Render ──────────────────────────────────────────────────────

  const virtualItems = virtualizer.getVirtualItems();

  return (
    <div style={tableWrapperStyle}>
      {/* Header */}
      <div style={{ ...headerRowStyle, minWidth: totalWidth }} role="row" aria-rowindex={1}>
        {/* Select-all checkbox */}
        <div style={{ ...headerCellStyle, width: CHECKBOX_WIDTH, flexShrink: 0, justifyContent: 'center' }}>
          <input
            type="checkbox"
            checked={allSelected}
            ref={(el) => {
              if (el) el.indeterminate = someSelected;
            }}
            onChange={toggleAll}
            style={checkboxStyle}
            aria-label="Select all rows"
          />
        </div>
        {columns.map((col) => {
          const isSorted = sort.column === col.sortKey;
          const style = col.sortKey ? sortableHeaderStyle : headerCellStyle;
          return (
            <div
              key={col.key}
              role="columnheader"
              aria-sort={isSorted ? (sort.direction === 'asc' ? 'ascending' : 'descending') : 'none'}
              style={{ ...style, width: col.width, flexShrink: 0 }}
              onClick={col.sortKey ? () => handleSort(col.sortKey!) : undefined}
            >
              {col.header}
              {isSorted && <SortArrow direction={sort.direction} />}
            </div>
          );
        })}
      </div>

      {/* Virtualized body */}
      <div
        ref={parentRef}
        style={{
          flex: 1,
          overflow: 'auto',
          minWidth: totalWidth,
        }}
        role="rowgroup"
      >
        {rows.length === 0 ? (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              height: 200,
              color: 'var(--text-tertiary)',
              fontSize: 13,
            }}
          >
            {emptyMessage}
          </div>
        ) : (
          <div
            style={{
              height: virtualizer.getTotalSize(),
              width: '100%',
              position: 'relative',
            }}
          >
            {virtualItems.map((virtualRow) => {
              const item = rows[virtualRow.index];
              const isSelected = selectedKeys.has(item.doc_version_key);
              const rowIndex = virtualRow.index;

              return (
                <div
                  key={item.doc_version_key}
                  role="row"
                  aria-rowindex={rowIndex + 2}
                  aria-selected={isSelected}
                  data-index={rowIndex}
                  style={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    width: '100%',
                    height: rowHeight,
                    transform: `translateY(${virtualRow.start}px)`,
                    display: 'flex',
                    alignItems: 'center',
                    borderBottom: '1px solid var(--border-default)',
                    background: isSelected
                      ? 'color-mix(in srgb, var(--color-accent) 8%, transparent)'
                      : rowIndex % 2 === 0
                        ? 'transparent'
                        : 'color-mix(in srgb, var(--bg-tertiary) 40%, transparent)',
                    cursor: onRowClick ? 'pointer' : 'default',
                    transition: 'background 80ms ease',
                  }}
                  onClick={() => onRowClick?.(item)}
                  onMouseEnter={(e) => {
                    if (!isSelected) {
                      e.currentTarget.style.background =
                        'color-mix(in srgb, var(--color-accent) 5%, transparent)';
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (!isSelected) {
                      e.currentTarget.style.background =
                        rowIndex % 2 === 0
                          ? 'transparent'
                          : 'color-mix(in srgb, var(--bg-tertiary) 40%, transparent)';
                    }
                  }}
                >
                  {/* Row checkbox */}
                  <div
                    style={{
                      ...bodyCellStyle,
                      width: CHECKBOX_WIDTH,
                      flexShrink: 0,
                      justifyContent: 'center',
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={(e) => {
                        e.stopPropagation();
                        toggleRow(item.doc_version_key);
                      }}
                      onClick={(e) => e.stopPropagation()}
                      style={checkboxStyle}
                      aria-label={`Select ${item.document}`}
                    />
                  </div>

                  {/* Data cells */}
                  {columns.map((col) => (
                    <div
                      key={col.key}
                      style={{ ...bodyCellStyle, width: col.width, flexShrink: 0 }}
                    >
                      {col.render(item)}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Default column definitions for Queue ───────────────────────────

export function getQueueColumns(t: (key: string) => string): ColumnDef[] {
  return [
    {
      key: 'document',
      header: 'Document',
      width: 220,
      sortKey: 'document',
      render: (item) => (
        <span style={{ fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis' }} title={item.document}>
          {item.document}
        </span>
      ),
    },
    {
      key: 'lot',
      header: 'Lot',
      width: 80,
      render: (item) =>
        item.lot ? <Badge label={item.lot} variant="lot" /> : <span style={{ color: 'var(--text-tertiary)' }}>—</span>,
    },
    {
      key: 'category',
      header: 'Category',
      width: 130,
      render: (item) => {
        const labelMap: Record<Category, string> = {
          EASY_WIN_APPROVE: t('cat_easy_win'),
          BLOCKED: t('cat_blocked'),
          FAST_REJECT: t('cat_fast_reject'),
          CONFLICT: t('cat_conflict'),
          WAITING: t('cat_waiting'),
          NOT_STARTED: t('cat_not_started'),
        };
        return <Badge label={labelMap[item.category]} variant="category" category={item.category} />;
      },
    },
    {
      key: 'score',
      header: 'Score',
      width: 80,
      sortKey: 'priority_score',
      render: (item) => (
        <Badge label={String(item.priority_score)} variant="score" scoreBand={item.score_band} />
      ),
    },
    {
      key: 'score_band',
      header: 'Band',
      width: 90,
      render: (item) => {
        const bandMap: Record<ScoreBand, string> = {
          CRITICAL: t('band_critical'),
          HIGH: t('band_high'),
          MEDIUM: t('band_medium'),
          LOW: t('band_low'),
        };
        return <Badge label={bandMap[item.score_band]} variant="score" scoreBand={item.score_band} />;
      },
    },
    {
      key: 'overdue',
      header: 'Overdue',
      width: 80,
      sortKey: 'days_overdue',
      render: (item) =>
        item.is_overdue ? (
          <span style={{ color: 'var(--color-danger)', fontWeight: 600 }}>
            {item.days_overdue}d
          </span>
        ) : (
          <span style={{ color: 'var(--text-tertiary)' }}>—</span>
        ),
    },
    {
      key: 'deadline',
      header: 'Deadline',
      width: 90,
      sortKey: 'deadline',
      render: (item) =>
        item.days_until_deadline !== null ? (
          <span
            style={{
              color:
                item.days_until_deadline <= 0
                  ? 'var(--color-danger)'
                  : item.days_until_deadline <= 5
                    ? 'var(--color-warning)'
                    : 'var(--text-primary)',
            }}
          >
            {item.days_until_deadline <= 0 ? 'Past' : `${item.days_until_deadline}d`}
          </span>
        ) : (
          <span style={{ color: 'var(--text-tertiary)' }}>—</span>
        ),
    },
    {
      key: 'diffusion',
      header: 'Diffusion',
      width: 80,
      sortKey: 'days_since_diffusion',
      render: (item) =>
        item.days_since_diffusion !== null ? (
          <span>{item.days_since_diffusion}d</span>
        ) : (
          <span style={{ color: 'var(--text-tertiary)' }}>—</span>
        ),
    },
    {
      key: 'action',
      header: 'Suggestion',
      width: 130,
      render: (item) => {
        const actionMap: Record<string, { label: string; color: string }> = {
          ISSUE_VISA: { label: t('suggested_issue_visa'), color: 'var(--color-success)' },
          ESCALATE: { label: t('suggested_escalate'), color: 'var(--color-warning)' },
          ARBITRATE: { label: t('suggested_arbitrate'), color: 'var(--color-arbitration)' },
          CHASE_APPROVERS: { label: t('suggested_chase_approvers'), color: 'var(--color-info)' },
          HOLD: { label: t('suggested_hold'), color: 'var(--color-neutral)' },
        };
        const info = actionMap[item.suggested_action] ?? { label: item.suggested_action, color: 'var(--text-secondary)' };
        return (
          <span
            style={{
              fontSize: 11,
              fontWeight: 600,
              color: info.color,
              padding: '2px 6px',
              borderRadius: 'var(--radius-badge)',
              background: `color-mix(in srgb, ${info.color} 12%, transparent)`,
            }}
          >
            {info.label}
          </span>
        );
      },
    },
    {
      key: 'quality',
      header: 'Quality',
      width: 70,
      render: (item) => <Badge label={item.row_quality} variant="quality" quality={item.row_quality} />,
    },
  ];
}
