import type { Category } from '../types';
import { ALL_CATEGORIES } from './category-constants';

// Category breakdown with computed percentages
export interface CategoryRow {
  category: Category;
  count: number;
  percentage: number;
}

/**
 * Compute category breakdown rows from a category_counts map.
 * Pure function — no mock imports.
 */
export function getCategoryBreakdown(categoryCounts: Record<string, number>): CategoryRow[] {
  const total = Object.values(categoryCounts).reduce((a, b) => a + b, 0);
  return ALL_CATEGORIES.map((cat) => ({
    category: cat,
    count: categoryCounts[cat] ?? 0,
    percentage: total > 0 ? ((categoryCounts[cat] ?? 0) / total) * 100 : 0,
  }));
}

/**
 * The largest category count (for bar width normalization).
 */
export function getCategoryMaxCount(categoryCounts: Record<string, number>): number {
  return Math.max(...Object.values(categoryCounts), 1);
}

// Blocker display name lookup
const BLOCKER_DISPLAY_NAMES: Record<string, string> = {
  'BET_STR_TERRELL': 'BET STR-TERRELL',
  'BET_FLU_AQUIA': 'BET FLU-AQUIA',
  'BET_ELE_SNEF': 'BET ELE-SNEF',
  'BET_CVC_COFELY': 'BET CVC-COFELY',
  'BET_PLB_AXIMA': 'BET PLB-AXIMA',
  'approver_hvac_01': 'HVAC-01',
};

export function getMainBlockerDisplayName(blockerKey: string | null): string | null {
  if (!blockerKey) return null;
  return BLOCKER_DISPLAY_NAMES[blockerKey] ?? blockerKey.replace(/_/g, ' ');
}

// Lot health bar color
export function getLotBarColor(overduePct: number): string {
  if (overduePct > 0.25) return 'var(--color-danger)';
  if (overduePct > 0.18) return 'var(--color-warning)';
  return 'var(--color-info)';
}

// Truncate helper
export function truncate(text: string, maxLen: number): string {
  if (text.length <= maxLen) return text;
  return text.slice(0, maxLen) + '\u2026';
}
