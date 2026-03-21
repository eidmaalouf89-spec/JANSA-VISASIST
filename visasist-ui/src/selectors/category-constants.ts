import type { Category, SuggestedAction } from '../types';
import type { TranslationKey } from '../i18n/en';

/**
 * All 6 canonical categories in display order.
 * Use this instead of ad-hoc arrays to ensure consistency.
 */
export const ALL_CATEGORIES: readonly Category[] = [
  'EASY_WIN_APPROVE',
  'BLOCKED',
  'FAST_REJECT',
  'CONFLICT',
  'WAITING',
  'NOT_STARTED',
] as const;

/**
 * Canonical map from Category enum → i18n label key.
 * Single source of truth for category display labels.
 */
export const CATEGORY_LABEL_MAP: Record<Category, TranslationKey> = {
  EASY_WIN_APPROVE: 'cat_easy_win',
  BLOCKED: 'cat_blocked',
  FAST_REJECT: 'cat_fast_reject',
  CONFLICT: 'cat_conflict',
  WAITING: 'cat_waiting',
  NOT_STARTED: 'cat_not_started',
};

/**
 * Maps a SuggestedAction to its most relevant Category for queue navigation.
 * Used when a recommendation card needs to link to the Queue with a filter.
 */
export const ACTION_TO_CATEGORY: Record<SuggestedAction, Category> = {
  ISSUE_VISA: 'EASY_WIN_APPROVE',
  ESCALATE: 'BLOCKED',
  ARBITRATE: 'CONFLICT',
  CHASE_APPROVERS: 'WAITING',
  HOLD: 'WAITING',
};

/**
 * CSS token variable for a category's primary color.
 */
export function getCategoryColor(cat: Category): string {
  switch (cat) {
    case 'EASY_WIN_APPROVE': return 'var(--color-success)';
    case 'BLOCKED':
    case 'FAST_REJECT': return 'var(--color-danger)';
    case 'CONFLICT': return 'var(--color-arbitration)';
    case 'WAITING': return 'var(--color-info)';
    case 'NOT_STARTED': return 'var(--color-neutral)';
  }
}
