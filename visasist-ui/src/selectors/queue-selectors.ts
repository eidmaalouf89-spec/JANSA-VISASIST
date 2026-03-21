import type { QueueItem, Category, ScoreBand } from '../types';

// ─── Filter types ───────────────────────────────────────────────────

export interface QueueFilters {
  overdueOnly: boolean;
  pendingOnly: boolean;
  latestRevision: boolean;
  categories: Category[];
  lots: string[];
  scoreBands: ScoreBand[];
  approver: string | null;
}

export const DEFAULT_QUEUE_FILTERS: QueueFilters = {
  overdueOnly: false,
  pendingOnly: true,
  latestRevision: true,
  categories: [],
  lots: [],
  scoreBands: ['CRITICAL', 'HIGH'],
  approver: null,
};

// ─── Sort types ─────────────────────────────────────────────────────

export interface QueueSort {
  column:
    | 'priority_score'
    | 'days_overdue'
    | 'days_since_diffusion'
    | 'deadline'
    | 'document';
  direction: 'asc' | 'desc';
}

export const DEFAULT_QUEUE_SORT: QueueSort = {
  column: 'priority_score',
  direction: 'desc',
};

// ─── Filter logic ───────────────────────────────────────────────────

export function applyQueueFilters(
  items: QueueItem[],
  filters: QueueFilters,
): QueueItem[] {
  return items.filter((item) => {
    // Structural: pendingOnly → exclude duplicates
    if (filters.pendingOnly && item.duplicate_flag === 'DUPLICATE') return false;

    // Structural: latestRevision
    if (filters.latestRevision && !item.is_latest) return false;

    // Overdue filter
    if (filters.overdueOnly && !item.is_overdue) return false;

    // Category filter (empty = all)
    if (filters.categories.length > 0 && !filters.categories.includes(item.category)) {
      return false;
    }

    // Lot filter (empty = all)
    if (filters.lots.length > 0 && !filters.lots.includes(item.lot ?? '')) {
      return false;
    }

    // Score band filter (empty = all)
    if (filters.scoreBands.length > 0 && !filters.scoreBands.includes(item.score_band)) {
      return false;
    }

    // Approver filter
    if (filters.approver !== null) {
      const inMissing = item.missing_approvers.includes(filters.approver);
      const inBlocking = item.blocking_approvers.includes(filters.approver);
      if (!inMissing && !inBlocking) return false;
    }

    return true;
  });
}

// ─── Sort logic ─────────────────────────────────────────────────────

function comparePrimary(a: QueueItem, b: QueueItem, sort: QueueSort): number {
  const dir = sort.direction === 'asc' ? 1 : -1;

  switch (sort.column) {
    case 'priority_score':
      return (a.priority_score - b.priority_score) * dir;

    case 'days_overdue':
      return (a.days_overdue - b.days_overdue) * dir;

    case 'days_since_diffusion': {
      const aVal = a.days_since_diffusion ?? 0;
      const bVal = b.days_since_diffusion ?? 0;
      return (aVal - bVal) * dir;
    }

    case 'deadline': {
      const aVal = a.days_until_deadline ?? Infinity;
      const bVal = b.days_until_deadline ?? Infinity;
      // "asc" = soonest first → smallest days_until_deadline first
      return (aVal - bVal) * dir;
    }

    case 'document':
      return a.document.localeCompare(b.document) * dir;
  }
}

function tieBreak(a: QueueItem, b: QueueItem): number {
  // 1. days_overdue desc
  const overdueDiff = b.days_overdue - a.days_overdue;
  if (overdueDiff !== 0) return overdueDiff;

  // 2. days_since_diffusion desc (null → 0)
  const aDiff = a.days_since_diffusion ?? 0;
  const bDiff = b.days_since_diffusion ?? 0;
  const diffDiff = bDiff - aDiff;
  if (diffDiff !== 0) return diffDiff;

  // 3. doc_version_key asc
  return a.doc_version_key.localeCompare(b.doc_version_key);
}

export function applyQueueSort(
  items: QueueItem[],
  sort: QueueSort,
): QueueItem[] {
  return [...items].sort((a, b) => {
    const primary = comparePrimary(a, b, sort);
    return primary !== 0 ? primary : tieBreak(a, b);
  });
}

// ─── Composed convenience ───────────────────────────────────────────

export function getFilteredSortedRows(
  items: QueueItem[],
  filters: QueueFilters,
  sort: QueueSort,
): QueueItem[] {
  return applyQueueSort(applyQueueFilters(items, filters), sort);
}

// ─── Single-item lookup ─────────────────────────────────────────────

export function getRowByKey(
  items: QueueItem[],
  docVersionKey: string,
): QueueItem | undefined {
  return items.find((item) => item.doc_version_key === docVersionKey);
}

// ─── Selection count ────────────────────────────────────────────────

export function getSelectedCount(selectedRows: string[]): number {
  return selectedRows.length;
}

// ─── Available filter values ────────────────────────────────────────

export function getAvailableLots(items: QueueItem[]): string[] {
  const lots = new Set<string>();
  for (const item of items) {
    if (item.lot !== null) {
      lots.add(item.lot);
    }
  }
  return [...lots].sort((a, b) => a.localeCompare(b));
}

export function getAvailableApprovers(items: QueueItem[]): string[] {
  const approvers = new Set<string>();
  for (const item of items) {
    for (const key of item.missing_approvers) approvers.add(key);
    for (const key of item.blocking_approvers) approvers.add(key);
  }
  return [...approvers].sort((a, b) => a.localeCompare(b));
}
