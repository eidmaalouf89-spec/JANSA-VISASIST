export type Category =
  | 'EASY_WIN_APPROVE' | 'BLOCKED' | 'FAST_REJECT'
  | 'CONFLICT' | 'WAITING' | 'NOT_STARTED';

export type ConsensusType =
  | 'ALL_APPROVE' | 'ALL_REJECT' | 'MIXED'
  | 'INCOMPLETE' | 'NOT_STARTED' | 'ALL_HM';

export type SuggestedAction =
  | 'ISSUE_VISA' | 'ESCALATE' | 'ARBITRATE' | 'CHASE_APPROVERS' | 'HOLD';

export type LifecycleState =
  | 'NOT_STARTED' | 'WAITING_RESPONSES' | 'READY_TO_ISSUE'
  | 'READY_TO_REJECT' | 'NEEDS_ARBITRATION' | 'CHRONIC_BLOCKED'
  | 'ON_HOLD' | 'EXCLUDED';

export type ScoreBand = 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';
export type RowQuality = 'OK' | 'WARNING' | 'ERROR';
export type DuplicateFlag = 'UNIQUE' | 'DUPLICATE' | 'SUSPECT';
export type VisaStatus = 'VSO' | 'VAO' | 'REF' | 'HM' | 'SUS' | 'DEF' | 'FAV';
export type Lang = 'en' | 'fr';

export type { QueueItem } from './queue-item';
export type { Document } from './document';
export type { Approver } from './approver';
export type { Revision } from './revision';
export type { AnomalyLog } from './anomaly-log';
export type { AISuggestion, DraftAction } from './ai-suggestion';
