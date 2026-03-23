import { useState, useEffect, useMemo, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { workspaceRoute } from '../../routes';
import {
  getFilteredSortedRows,
  getRowByKey,
  getSelectedCount,
  getAvailableLots,
  getAvailableApprovers,
  DEFAULT_QUEUE_FILTERS,
  DEFAULT_QUEUE_SORT,
  type QueueFilters,
  type QueueSort,
} from '../../selectors/queue-selectors';
import { getQueueItems } from '../../data/queue';
import { DATA_MODE } from '../../data/config';
import { revisions } from '../../mock/revisions';
import type { QueueItem, Category, ScoreBand } from '../../types';
import FilterPanel from '../../components/FilterPanel';
import DataTable from '../../components/DataTable';
import BulkActionBar from '../../components/BulkActionBar';
import DetailPanel from '../../components/DetailPanel';

// ─── URL parsing helpers ────────────────────────────────────────────

function parseFiltersFromUrl(params: URLSearchParams): QueueFilters {
  const catParam = params.get('cat');
  const lotParam = params.get('lot');
  const bandParam = params.get('band');
  const overdueParam = params.get('overdue');

  return {
    overdueOnly: overdueParam === '1',
    pendingOnly: DEFAULT_QUEUE_FILTERS.pendingOnly,
    latestRevision: DEFAULT_QUEUE_FILTERS.latestRevision,
    categories: catParam ? (catParam.split(',') as Category[]) : [],
    lots: lotParam ? lotParam.split(',') : [],
    scoreBands: bandParam
      ? (bandParam.split(',') as ScoreBand[])
      : DEFAULT_QUEUE_FILTERS.scoreBands,
    approver: null,
  };
}

function parseSortFromUrl(params: URLSearchParams): QueueSort {
  const col = params.get('sort') as QueueSort['column'] | null;
  const dir = params.get('dir') as 'asc' | 'desc' | null;
  if (col) {
    return { column: col, direction: dir === 'asc' ? 'asc' : 'desc' };
  }
  return DEFAULT_QUEUE_SORT;
}

// ─── Skeleton rows for loading state ────────────────────────────────

function SkeletonRows() {
  return (
    <div style={{ padding: '0 16px' }}>
      {Array.from({ length: 10 }).map((_, i) => (
        <div
          key={i}
          style={{
            height: 44,
            marginBottom: 2,
            background: 'var(--bg-tertiary)',
            borderRadius: 4,
          }}
        />
      ))}
    </div>
  );
}

// ─── Component ──────────────────────────────────────────────────────

export default function QueueScreen() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  // ── Data loading ──────────────────────────────────────────────────
  const [items, setItems] = useState<QueueItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [integrationError, setIntegrationError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    getQueueItems()
      .then((result) => {
        if (!cancelled) {
          setItems(result);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setLoading(false);
          if (DATA_MODE === 'strict') setIntegrationError(String(err.message));
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // ── Domain state — initialised from URL ─────────────────────────
  const [filters, setFilters] = useState<QueueFilters>(() => parseFiltersFromUrl(searchParams));
  const [sort, setSort] = useState<QueueSort>(() => parseSortFromUrl(searchParams));
  const [selectedRows, setSelectedRows] = useState<string[]>([]);
  const [activeRowId, setActiveRowId] = useState<string | null>(() => searchParams.get('row') ?? null);
  const [detailTab, setDetailTab] = useState<0 | 1 | 2 | 3>(() => {
    const t = searchParams.get('tab');
    return (t && [0, 1, 2, 3].includes(Number(t)) ? Number(t) : 0) as 0 | 1 | 2 | 3;
  });

  // ── Derived (never stored) ──────────────────────────────────────
  const allItems = items;
  const availableLots = useMemo(() => getAvailableLots(allItems), [allItems]);
  const availableApprovers = useMemo(() => getAvailableApprovers(allItems), [allItems]);
  const filteredSorted = useMemo(
    () => getFilteredSortedRows(allItems, filters, sort),
    [allItems, filters, sort],
  );
  const selectedCount = getSelectedCount(selectedRows);
  const activeItem = activeRowId ? (getRowByKey(allItems, activeRowId) ?? null) : null;

  // Revision lookup — explicit relational field, never string prefix
  const activeRevisions = activeItem
    ? revisions.filter((r) => r.doc_family_key === activeItem.doc_family_key)
    : [];

  // ── URL sync ────────────────────────────────────────────────────
  useEffect(() => {
    const params: Record<string, string> = {};

    if (filters.categories.length > 0) params.cat = filters.categories.join(',');
    if (filters.lots.length > 0) params.lot = filters.lots.join(',');
    if (filters.overdueOnly) params.overdue = '1';

    // Only write scoreBands if different from default
    const defaultBands = DEFAULT_QUEUE_FILTERS.scoreBands.slice().sort().join(',');
    const currentBands = filters.scoreBands.slice().sort().join(',');
    if (currentBands !== defaultBands) params.band = filters.scoreBands.join(',');

    if (sort.column !== DEFAULT_QUEUE_SORT.column) params.sort = sort.column;
    if (sort.direction !== DEFAULT_QUEUE_SORT.direction) params.dir = sort.direction;
    if (activeRowId) params.row = activeRowId;
    if (detailTab !== 0) params.tab = String(detailTab);

    setSearchParams(params, { replace: true });
  }, [filters, sort, activeRowId, detailTab, setSearchParams]);

  // ── Keyboard ────────────────────────────────────────────────────
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setSelectedRows([]);
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  // ── Handlers ────────────────────────────────────────────────────
  function handleSort(col: QueueSort['column']) {
    setSort((prev) => {
      if (prev.column !== col) return { column: col, direction: 'desc' };
      if (prev.direction === 'desc') return { column: col, direction: 'asc' };
      return DEFAULT_QUEUE_SORT;
    });
  }

  function handleRowClick(key: string) {
    setActiveRowId(key);
    setDetailTab(0);
    setSelectedRows([]);
  }

  function handleCheckboxChange(key: string, checked: boolean) {
    setSelectedRows((prev) =>
      checked ? [...prev, key] : prev.filter((k) => k !== key),
    );
  }

  function handleSelectAll(checked: boolean) {
    setSelectedRows(
      checked ? filteredSorted.map((r) => r.doc_version_key) : [],
    );
  }

  const handleBulkExport = useCallback(() => {
    // Get selected items
    const selectedItems = filteredSorted.filter((r) =>
      selectedRows.includes(r.doc_version_key),
    );
    if (selectedItems.length === 0) return;

    // Technical/debug CSV with raw field names
    const keys = Object.keys(selectedItems[0]) as (keyof typeof selectedItems[0])[];
    const header = keys.join(',');
    const rows = selectedItems.map((item) =>
      keys
        .map((k) => {
          const val = item[k];
          if (val === null || val === undefined) return '';
          if (Array.isArray(val)) return `"${val.join(';')}"`;
          if (typeof val === 'string' && (val.includes(',') || val.includes('"')))
            return `"${val.replace(/"/g, '""')}"`;
          return String(val);
        })
        .join(','),
    );

    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `queue_export_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [filteredSorted, selectedRows]);

  // ── Layout ──────────────────────────────────────────────────────
  return (
    <div style={{ display: 'flex', height: '100%', overflow: 'hidden' }}>
      <FilterPanel
        filters={filters}
        onChange={setFilters}
        availableLots={availableLots}
        availableApprovers={availableApprovers}
        totalCount={allItems.length}
        filteredCount={filteredSorted.length}
      />
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        {integrationError && (
          <div style={{
            background: 'var(--color-danger)', color: '#fff',
            padding: '12px 20px', fontSize: 12, fontWeight: 600,
          }}>
            ⚠ Integration error: {integrationError} — set VITE_DATA_MODE=hybrid to use mock fallback
          </div>
        )}
        {selectedCount > 0 && (
          <BulkActionBar
            selectedCount={selectedCount}
            onIssue={() => {/* stub — no modal in P2 */}}
            onChase={() => {/* stub */}}
            onExport={handleBulkExport}
            onFlag={() => {/* stub */}}
            onClear={() => setSelectedRows([])}
          />
        )}
        {loading ? (
          <SkeletonRows />
        ) : (
          <DataTable
            rows={filteredSorted}
            selectedRows={selectedRows}
            activeRowId={activeRowId}
            onRowClick={handleRowClick}
            onCheckboxChange={handleCheckboxChange}
            onSelectAll={handleSelectAll}
            sortBy={sort}
            onSort={handleSort}
          />
        )}
      </div>
      {activeItem && (
        <DetailPanel
          item={activeItem}
          revisions={activeRevisions}
          activeTab={detailTab}
          onTabChange={setDetailTab}
          onOpenWorkspace={() => navigate(workspaceRoute(activeItem.doc_version_key))}
        />
      )}
    </div>
  );
}
