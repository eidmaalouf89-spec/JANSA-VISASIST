import type { QueueItem } from '../types';

export interface ScoreComponents {
  overduePoints: number;       // 0–40
  deadlinePoints: number;      // 0–25
  completenessPoints: number;  // 0–20
  revisionPoints: number;      // 0–5
  deadlinePenalty: number;     // −10 or 0
}

export function computeScoreComponents(item: QueueItem): ScoreComponents {
  // Overdue: linear 0→40 over 0→30 days, capped at 40
  const overduePoints = Math.min(40, Math.round((item.days_overdue / 30) * 40));

  // Deadline proximity
  let deadlinePoints = 0;
  if (item.days_until_deadline !== null) {
    if (item.days_until_deadline <= 3) deadlinePoints = 25;
    else if (item.days_until_deadline <= 7) deadlinePoints = 20;
    else if (item.days_until_deadline <= 14) deadlinePoints = 10;
  }

  // Completeness based on consensus
  let completenessPoints = 0;
  switch (item.consensus_type) {
    case 'ALL_APPROVE': completenessPoints = 20; break;
    case 'ALL_REJECT': completenessPoints = 15; break;
    case 'MIXED': completenessPoints = 10; break;
    case 'INCOMPLETE': completenessPoints = 5; break;
    case 'NOT_STARTED':
    case 'ALL_HM': completenessPoints = 0; break;
  }

  // Revision depth
  let revisionPoints = 0;
  if (item.revision_count > 2) revisionPoints = 5;
  else if (item.revision_count === 2) revisionPoints = 3;

  // Deadline penalty: missing contractual deadline
  const deadlinePenalty = item.date_contractuelle_visa === null ? -10 : 0;

  return {
    overduePoints,
    deadlinePoints,
    completenessPoints,
    revisionPoints,
    deadlinePenalty,
  };
}
