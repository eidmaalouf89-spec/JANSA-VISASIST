import { useState, useEffect, useMemo, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from '../../i18n/use-translation';
import { getQueueItems } from '../../data/queue';
import {
  getFilteredSortedRows,
  DEFAULT_QUEUE_FILTERS,
  DEFAULT_QUEUE_SORT,
  type QueueFilters,
  type QueueSort,
} from '../../selectors/queue-selectors';
import DataTable, { getQueueColumns } from '../../components/DataTable';
import type { QueueItem } from '../../types';

export default function QueueScreen() {
  const { t } = useTranslation();
  const navigate = useNavigate();

  // ── Data state ────────────────────────────────────────────────────
  const [allItems, setAllItems] = useState<QueueItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    getQueueItems().then((items) => {
      if (!cancelled) {
        setAllItems(items);
        setLoading(false);
      }
    });
    return () => { cancelled = true; };
  }, []);

  // ── Filter & sort state ───────────────────────────────────────────
  const [filters] = useState<QueueFilters>(DEFAULT_QUEUE_FILTERS);
  const [sort, setSort] = useState<QueueSort>(DEFAULT_QUEUE_SORT);
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());

  // ── Derived rows via queue-selectors ──────────────────────────────
  const rows = useMemo(
    () => getFilteredSortedRows(allItems, filters, sort),
    [allItems, filters, sort],
  );

  // ── Column definitions ────────────────────────────────────────────
  const columns = useMemo(() => getQueueColumns(t as (key: string) => string), [t]);

  // ── Handlers ──────────────────────────────────────────────────────
  const handleRowClick = useCallback(
    (item: QueueItem) => {
      navigate(`/workspace/${item.doc_version_key}`);
    },
    [navigate],
  );

  // ── Render ────────────────────────────────────────────────────────

  if (loading) {
    return (
      <div style={{ padding: 24, color: 'var(--text-tertiary)' }}>
        {t('dashboard_loading')}
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', padding: 24, gap: 16 }}>
      {/* Header bar */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexShrink: 0 }}>
        <h1 style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 600 }}>
          {t('nav_queue')}
        </h1>
        <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>
          {rows.length} / {allItems.length} items
          {selectedKeys.size > 0 && ` · ${selectedKeys.size} selected`}
        </span>
      </div>

      {/* Table fills remaining space */}
      <div style={{ flex: 1, minHeight: 0 }}>
        <DataTable
          rows={rows}
          columns={columns}
          sort={sort}
          onSortChange={setSort}
          selectedKeys={selectedKeys}
          onSelectionChange={setSelectedKeys}
          onRowClick={handleRowClick}
          emptyMessage={t('empty_no_results')}
        />
      </div>
    </div>
  );
}
