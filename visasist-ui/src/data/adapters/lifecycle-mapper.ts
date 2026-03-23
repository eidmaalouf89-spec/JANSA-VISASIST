import type { LifecycleState } from '../../types';

/**
 * Single canonical source for lifecycle_state derivation.
 * Used by both queue adapter and document adapter.
 * No component or selector may re-derive lifecycle_state.
 */
export function getLifecycleState(
  consensusType: string,
  revisionCount: number,
): LifecycleState {
  switch (consensusType) {
    case 'NOT_STARTED':  return 'NOT_STARTED';
    case 'INCOMPLETE':   return 'WAITING_RESPONSES';
    case 'ALL_APPROVE':  return 'READY_TO_ISSUE';
    case 'ALL_REJECT':   return revisionCount > 1 ? 'CHRONIC_BLOCKED' : 'READY_TO_REJECT';
    case 'MIXED':        return 'NEEDS_ARBITRATION';
    case 'ALL_HM':       return 'EXCLUDED';
    default:             return 'NOT_STARTED';
  }
}
