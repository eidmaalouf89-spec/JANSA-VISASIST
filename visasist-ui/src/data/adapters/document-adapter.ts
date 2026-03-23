import type { Document, Approver, Revision, AnomalyLog } from '../../types';
import type { ScoreComponents } from '../../types/document';
import { getLifecycleState } from './lifecycle-mapper';

// ─── Approver constants ─────────────────────────────────────────────

const APPROVER_KEYS = [
  'MOEX_GEMO', 'ARCHI_MOX', 'BET_STR_TERRELL', 'BET_GEOLIA_G4',
  'ACOUSTICIEN_AVLS', 'AMO_HQE_LE_SOMMER', 'BET_POLLUTION_DIE', 'SOCOTEC',
  'BET_ELIOTH', 'BET_EGIS', 'BET_ASCAUDIT', 'BET_ASCENSEUR', 'BET_SPK', 'PAYSAGISTE_MUGO',
] as const;

const DISPLAY_NAMES: Record<string, string> = {
  MOEX_GEMO:           'MOEX GEMO',
  ARCHI_MOX:           'ARCHI MOX',
  BET_STR_TERRELL:     'BET STR-TERRELL',
  BET_GEOLIA_G4:       'BET GEOLIA - G4',
  ACOUSTICIEN_AVLS:    'ACOUSTICIEN AVLS',
  AMO_HQE_LE_SOMMER:  'AMO HQE LE SOMMER',
  BET_POLLUTION_DIE:   'BET POLLUTION DIE',
  SOCOTEC:             'SOCOTEC',
  BET_ELIOTH:          'BET ELIOTH',
  BET_EGIS:            'BET EGIS',
  BET_ASCAUDIT:        'BET ASCAUDIT',
  BET_ASCENSEUR:       'BET ASCENSEUR',
  BET_SPK:             'BET SPK',
  PAYSAGISTE_MUGO:     'PAYSAGISTE MUGO',
};

// ─── Score components ───────────────────────────────────────────────

function computeScoreComponents(r: Record<string, unknown>): ScoreComponents {
  const daysOverdue = (r.days_overdue as number) ?? 0;
  const daysUntilDeadline = r.days_until_deadline as number | null;
  const consensusType = r.consensus_type as string;
  const revisionCount = (r.revision_count as number) ?? 0;
  const dateContractuelleVisa = r.date_contractuelle_visa as string | null;

  const overdue_points = Math.min(40, Math.round((daysOverdue / 30) * 40));

  let deadline_points = 0;
  if (daysUntilDeadline !== null) {
    if (daysUntilDeadline <= 3) deadline_points = 25;
    else if (daysUntilDeadline <= 7) deadline_points = 20;
    else if (daysUntilDeadline <= 14) deadline_points = 10;
  }

  let completeness_points = 0;
  switch (consensusType) {
    case 'ALL_APPROVE':  completeness_points = 20; break;
    case 'ALL_REJECT':   completeness_points = 15; break;
    case 'MIXED':        completeness_points = 10; break;
    case 'INCOMPLETE':   completeness_points = 5;  break;
  }

  const revision_points = revisionCount > 2 ? 5 : revisionCount === 2 ? 3 : 0;
  const deadline_penalty = dateContractuelleVisa === null ? -10 : 0;

  return {
    overdue_points,
    deadline_points,
    completeness_points,
    revision_points,
    deadline_penalty,
  };
}

// ─── Approver assembly ──────────────────────────────────────────────

function assembleApprovers(
  r: Record<string, unknown>,
  assignedKeys: string[],
): Approver[] {
  const assignedSet = new Set(assignedKeys);
  return APPROVER_KEYS.map((key) => {
    const statut = (r[`${key}_statut`] as Approver['statut']) ?? null;
    const isAssigned = assignedSet.has(key);
    return {
      canonical_key:  key,
      display_name:   DISPLAY_NAMES[key] ?? key,
      is_assigned:    isAssigned,
      statut,
      statut_raw:     (r[`${key}_statut_raw`] as string | null) ?? null,
      date:           (r[`${key}_date`] as string | null) ?? null,
      numero_visa:    (r[`${key}_n`] as string | null) ?? null,
      is_blocking:    statut === 'REF',
      is_pending:     statut === null && isAssigned,
      is_hm:          statut === 'HM',
    };
  });
}

// ─── Temporary derivations ──────────────────────────────────────────

// TEMPORARY: remove score_band derivation when M3 emits this field directly
function deriveScoreBand(score: number): Document['score_band'] {
  if (score >= 80) return 'CRITICAL';
  if (score >= 60) return 'HIGH';
  if (score >= 40) return 'MEDIUM';
  return 'LOW';
}

// TEMPORARY: remove suggested_action derivation when M3 emits this field directly
function deriveSuggestedAction(category: string): Document['suggested_action'] {
  switch (category) {
    case 'EASY_WIN_APPROVE': return 'ISSUE_VISA';
    case 'BLOCKED':          return 'ESCALATE';
    case 'FAST_REJECT':      return 'ISSUE_VISA';
    case 'CONFLICT':         return 'ARBITRATE';
    case 'WAITING':          return 'CHASE_APPROVERS';
    default:                 return 'HOLD';
  }
}

// ─── Main adapter ───────────────────────────────────────────────────

export function adaptM2RowToDocument(
  raw: Record<string, unknown>,
  revisions: Revision[],    // HYBRID P3.5: from mock — replace when M2 emits revision chain file
  anomalyLogs: AnomalyLog[], // HYBRID P3.5: from mock — replace when import_log.json is populated
): Document {
  // Suppress unused parameter warning — anomalyLogs passed for future use
  void anomalyLogs;
  void revisions;

  const r = raw;
  const score = r.priority_score as number;
  const category = r.category as Document['category'];
  const consensusType = r.consensus_type as string;
  const revisionCount = (r.revision_count as number) ?? 0;
  const assignedApprovers = (r.assigned_approvers as string[]) ?? [];

  return {
    // Traceability
    doc_version_key:      r.doc_version_key as string,
    doc_family_key:       r.doc_family_key as string,
    row_id:               r.row_id as string,
    source_sheet:         r.source_sheet as string,
    source_row:           r.source_row as number,
    // Document identity
    document:             r.document as string,
    document_raw:         r.document_raw as string,
    titre:                (r.titre as string | null) ?? null,
    lot:                  (r.lot as string | null) ?? null,
    type_doc:             (r.type_doc as string | null) ?? null,
    niv:                  (r.niv as string | null) ?? null,
    ind:                  (r.ind as string | null) ?? null,
    ind_sort_order:       (r.ind_sort_order as number) ?? 0,
    // Dates
    date_diffusion:       (r.date_diffusion as string | null) ?? null,
    date_contractuelle_visa: (r.date_contractuelle_visa as string | null) ?? null,
    days_since_diffusion: (r.days_since_diffusion as number | null) ?? null,
    days_until_deadline:  (r.days_until_deadline as number | null) ?? null,
    days_overdue:         (r.days_overdue as number) ?? 0,
    is_overdue:           (r.is_overdue as boolean) ?? false,
    // M3 scoring
    priority_score:       score,
    // TEMPORARY: remove when M3 emits score_band directly
    score_band:           deriveScoreBand(score),
    category,
    consensus_type:       consensusType as Document['consensus_type'],
    missing_approvers:    (r.missing_approvers as string[]) ?? [],
    blocking_approvers:   (r.blocking_approvers as string[]) ?? [],
    // TEMPORARY: remove when M3 emits suggested_action directly
    suggested_action:     deriveSuggestedAction(category as string),
    // M2 revision info
    revision_count:       revisionCount,
    is_latest:            true,
    is_cross_lot:         (r.is_cross_lot as boolean) ?? false,
    // Quality
    row_quality:          (r.row_quality as Document['row_quality']) ?? 'OK',
    duplicate_flag:       (r.duplicate_flag as Document['duplicate_flag']) ?? 'UNIQUE',
    // Extended identity
    zone:                 (r.zone as string | null) ?? null,
    n_doc:                (r.n_doc as string | null) ?? null,
    type_format:          (r.type_format as string | null) ?? null,
    ancien:               (r.ancien as string | null) ?? null,
    n_bdx:                (r.n_bdx as string | null) ?? null,
    date_reception:       (r.date_reception as string | null) ?? null,
    non_recu_papier:      (r.non_recu_papier as string | null) ?? null,
    observations:         (r.observations as string | null) ?? null,
    visa_global:          (r.visa_global as Document['visa_global']) ?? null,
    visa_global_raw:      (r.visa_global_raw as string | null) ?? null,
    // M2 extended
    lifecycle_state:      getLifecycleState(consensusType, revisionCount),
    cross_lot_sheets:     (r.cross_lot_sheets as string[] | null) ?? null,
    previous_version_key: (r.previous_version_key as string | null) ?? null,
    // Approvers
    approvers:            assembleApprovers(r, assignedApprovers),
    assigned_approvers:   assignedApprovers,
    // Score breakdown
    score_components:     computeScoreComponents(r),
    // M5 AI suggestion — out of scope for P3.5
    ai_suggestion:        null,
  };
}
