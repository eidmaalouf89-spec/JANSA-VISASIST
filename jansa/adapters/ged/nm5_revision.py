"""NM5-GED: Revision Linking & Active Dataset.

Determines the active document set by applying a two-level revision filter:
  1. Latest technical Version within each (famille_key, INDICE)
  2. Latest contractual INDICE per (famille_key, lot, batiment)
  3. Legacy exclusion: documents flagged ancien=1 in GrandFichier

Produces the active_dataset that NM7 operates on.

[V1.1 — P4] Uniqueness key: (famille_key, lot, batiment)
[V1.2 — Legacy] ancien=1 cross-reference from GrandFichier
"""

from typing import Optional

import pandas as pd

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.legacy_loader import flag_legacy_docs
from jansa.adapters.ged.logging import log_event


# ---------------------------------------------------------------------------
# Contract validation (P10)
# ---------------------------------------------------------------------------

def _validate_contract(df: pd.DataFrame) -> None:
    """Validate NM5 input contract: required columns from NM1-NM4."""
    required = [
        'doc_id', 'famille_key', 'indice', 'indice_sort_order',
        'version_number', 'lot', 'batiment',
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ContractError(
            f"NM5 input contract violated — missing columns: {missing}"
        )


# ---------------------------------------------------------------------------
# Revision chain analysis
# ---------------------------------------------------------------------------

def _analyze_revision_chains(doc_level: pd.DataFrame) -> pd.DataFrame:
    """Compute revision chain metadata per famille_key.

    For each document: revision_count, previous_indice, has_revision_gap.
    R-NM5-03: revision_count counts distinct INDICE where is_latest_version=True.
    """
    # Work only on latest-version docs for revision_count (R-NM5-03)
    latest_versions = doc_level[doc_level['is_latest_version']].copy()

    # Revision count per famille_key (distinct INDICE among latest versions)
    rev_counts = latest_versions.groupby('famille_key')['indice'].nunique()
    doc_level['revision_count'] = doc_level['famille_key'].map(rev_counts).fillna(1).astype(int)

    # Previous indice and gap detection — per famille_key, sorted by indice_sort_order
    prev_indice = pd.Series(index=doc_level.index, dtype='object')
    has_gap = pd.Series(False, index=doc_level.index, dtype=bool)
    anomaly_flags = [[] for _ in range(len(doc_level))]

    for fk, group in doc_level.groupby('famille_key', sort=False):
        sorted_group = group.sort_values('indice_sort_order')
        indices = sorted_group['indice'].values
        sort_orders = sorted_group['indice_sort_order'].values
        group_indices = sorted_group.index.values

        # Previous indice
        for i, idx in enumerate(group_indices):
            if i > 0:
                prev_indice.at[idx] = indices[i - 1]

        # Revision gap detection (non-contiguous sort order)
        if len(sort_orders) > 1:
            gap_detected = any(
                sort_orders[i + 1] - sort_orders[i] > 1
                for i in range(len(sort_orders) - 1)
            )
            if gap_detected:
                for idx in group_indices:
                    has_gap.at[idx] = True
                    anomaly_flags[doc_level.index.get_loc(idx)].append('REVISION_GAP')
                log_event(
                    None, 'NM5', 'WARNING', 'REVISION_GAP',
                    f'Non-contiguous INDICE sequence for famille_key: {fk}',
                )

    doc_level['previous_indice'] = prev_indice
    doc_level['has_revision_gap'] = has_gap
    doc_level['anomaly_flags'] = anomaly_flags

    return doc_level


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compute_active_dataset(
    ged_long: pd.DataFrame,
    ancien_set: Optional[set] = None,
) -> tuple:
    """Compute revision metadata and filter to active dataset.

    Args:
        ged_long: NM1-NM4 enriched DataFrame (ged_long format)
        ancien_set: Optional set of (numero, indice) tuples where ancien=1.
                    From legacy_loader.load_ancien_flags(). If None, no legacy
                    exclusion is applied.

    Returns:
        (ged_long_enriched, doc_level, active_dataset)
        - ged_long_enriched: ged_long with is_active, is_legacy columns joined
        - doc_level: DataFrame with one row per doc_id containing revision metadata
        - active_dataset: ged_long filtered to is_active=True documents
    """
    _validate_contract(ged_long)

    # ------------------------------------------------------------------
    # Step 1: Aggregate to document level
    # ------------------------------------------------------------------
    doc_cols = ['doc_id', 'famille_key', 'indice', 'indice_sort_order',
                'version_number', 'lot', 'batiment']
    # Include numero for legacy cross-reference (if available)
    if 'numero' in ged_long.columns:
        doc_cols.append('numero')
    doc_level = ged_long.drop_duplicates('doc_id')[doc_cols].copy()

    # Fill missing lot/batiment for groupby safety
    doc_level['lot'] = doc_level['lot'].fillna('')
    doc_level['batiment'] = doc_level['batiment'].fillna('')

    # Include date_depot if available (for anomaly detection)
    if 'date_depot' in ged_long.columns:
        date_depot_map = ged_long.drop_duplicates('doc_id').set_index('doc_id')['date_depot']
        doc_level['date_depot'] = doc_level['doc_id'].map(date_depot_map)

    # ------------------------------------------------------------------
    # Step 2: Compute is_latest_version
    # Latest version within same (famille_key, indice)
    # ------------------------------------------------------------------
    doc_level['max_version'] = doc_level.groupby(
        ['famille_key', 'indice']
    )['version_number'].transform('max')
    doc_level['is_latest_version'] = (
        doc_level['version_number'] == doc_level['max_version']
    )

    # ------------------------------------------------------------------
    # Step 3: Compute is_latest_indice
    # [V1.1 — P4] Latest INDICE per (famille_key, lot, batiment)
    # ------------------------------------------------------------------
    doc_level['max_indice_sort'] = doc_level.groupby(
        ['famille_key', 'lot', 'batiment']
    )['indice_sort_order'].transform('max')
    doc_level['is_latest_indice'] = (
        doc_level['indice_sort_order'] == doc_level['max_indice_sort']
    )

    # ------------------------------------------------------------------
    # Step 4: Compute is_active (R-NM5-01)
    # ------------------------------------------------------------------
    doc_level['is_active'] = (
        doc_level['is_latest_version'] & doc_level['is_latest_indice']
    )

    # ------------------------------------------------------------------
    # Step 4b: Deterministic tie-break for active uniqueness
    # When multiple docs are is_active=True for the same
    # (famille_key, lot, batiment), keep only the single winner.
    # Priority: highest indice_sort_order, highest version_number,
    #           latest date_depot, highest doc_id (final fallback).
    # ------------------------------------------------------------------
    active_mask = doc_level['is_active']
    if active_mask.any():
        active_subset = doc_level[active_mask]
        group_counts = active_subset.groupby(
            ['famille_key', 'lot', 'batiment']
        ).size()
        tied_groups = group_counts[group_counts > 1]

        if len(tied_groups) > 0:
            # Sort candidates by tie-break priority (descending)
            sort_cols = ['indice_sort_order', 'version_number']
            ascending = [False, False]
            if 'date_depot' in doc_level.columns:
                sort_cols.append('date_depot')
                ascending.append(False)
            sort_cols.append('doc_id')
            ascending.append(False)

            for (fk, lot, bat), _ in tied_groups.items():
                candidates_mask = (
                    (doc_level['famille_key'] == fk) &
                    (doc_level['lot'] == lot) &
                    (doc_level['batiment'] == bat) &
                    doc_level['is_active']
                )
                candidates = doc_level.loc[candidates_mask].sort_values(
                    sort_cols, ascending=ascending, na_position='last',
                )
                # Winner is first row after sort; losers are the rest
                winner_idx = candidates.index[0]
                loser_indices = candidates.index[1:]

                doc_level.loc[loser_indices, 'is_active'] = False

                winner_doc_id = doc_level.at[winner_idx, 'doc_id']
                loser_doc_ids = doc_level.loc[loser_indices, 'doc_id'].tolist()
                log_event(
                    int(winner_doc_id) if pd.notna(winner_doc_id) else None,
                    'NM5', 'WARNING', 'ACTIVE_TIE_RESOLVED',
                    f'Tie-break for ({fk}, {lot}, {bat}): '
                    f'winner doc_id={winner_doc_id}, '
                    f'demoted doc_ids={loser_doc_ids}',
                )

    # ------------------------------------------------------------------
    # Step 4c: Legacy exclusion (ancien=1 from GrandFichier)
    # Documents flagged as ancien in the GrandFichier are legacy and
    # must be excluded from the active dataset.
    # ------------------------------------------------------------------
    if ancien_set and 'numero' in doc_level.columns:
        doc_level = flag_legacy_docs(
            doc_level, ancien_set,
            numero_col='numero', indice_col='indice',
        )
        legacy_mask = doc_level['is_legacy']
        legacy_active_count = (legacy_mask & doc_level['is_active']).sum()
        if legacy_active_count > 0:
            # Demote legacy docs from active
            doc_level.loc[legacy_mask, 'is_active'] = False
            log_event(
                None, 'NM5', 'INFO', 'LEGACY_EXCLUDED',
                f'{legacy_active_count} legacy (ancien=1) documents '
                f'excluded from active dataset',
            )
    else:
        doc_level['is_legacy'] = False
        doc_level['exclusion_reason'] = None
        if ancien_set:
            log_event(
                None, 'NM5', 'WARNING', 'LEGACY_NO_NUMERO',
                'ancien_set provided but numero column not in doc_level — '
                'legacy exclusion skipped',
            )

    # ------------------------------------------------------------------
    # Step 5: Revision chain analysis
    # ------------------------------------------------------------------
    doc_level = _analyze_revision_chains(doc_level)

    # ------------------------------------------------------------------
    # Step 6: Cross-lot detection
    # ------------------------------------------------------------------
    family_lots = doc_level.groupby('famille_key')['lot'].nunique()
    cross_lot_families = set(family_lots[family_lots > 1].index)
    doc_level['is_cross_lot'] = doc_level['famille_key'].isin(cross_lot_families)

    # Cross-lot list per famille_key
    if cross_lot_families:
        family_lot_lists = doc_level.groupby('famille_key')['lot'].apply(
            lambda x: list(x.unique())
        ).to_dict()
        doc_level['cross_lot_list'] = doc_level['famille_key'].map(
            lambda fk: family_lot_lists.get(fk) if fk in cross_lot_families else None
        )
    else:
        doc_level['cross_lot_list'] = None

    # ------------------------------------------------------------------
    # Anomaly detection: exact duplicates (R-NM5-04)
    # ------------------------------------------------------------------
    if 'doc_version_key' in ged_long.columns:
        dvk_map = ged_long.drop_duplicates('doc_id').set_index('doc_id')['doc_version_key']
        doc_level['doc_version_key'] = doc_level['doc_id'].map(dvk_map)
        dvk_counts = doc_level['doc_version_key'].value_counts()
        dup_dvks = set(dvk_counts[dvk_counts > 1].index)
        if dup_dvks:
            for dvk in dup_dvks:
                affected_docs = doc_level.loc[
                    doc_level['doc_version_key'] == dvk, 'doc_id'
                ].tolist()
                for did in affected_docs:
                    log_event(
                        int(did) if pd.notna(did) else None,
                        'NM5', 'WARNING', 'EXACT_DUPLICATE',
                        f'Exact duplicate doc_version_key: {dvk}',
                    )
                    loc = doc_level.index[doc_level['doc_id'] == did]
                    for idx in loc:
                        pos = doc_level.index.get_loc(idx)
                        doc_level.at[idx, 'anomaly_flags'] = (
                            doc_level.at[idx, 'anomaly_flags'] + ['EXACT_DUPLICATE']
                        )

    # Anomaly: INDICE sort order tied (SUSPECT_DUPLICATE)
    tied = doc_level.groupby(
        ['famille_key', 'lot', 'batiment', 'indice_sort_order']
    ).size()
    tied_groups = tied[tied > 1]
    if len(tied_groups) > 0:
        for (fk, lot, bat, iso), cnt in tied_groups.items():
            if iso > 0:  # Skip 0 (null INDICE) — handled separately
                affected = doc_level[
                    (doc_level['famille_key'] == fk) &
                    (doc_level['lot'] == lot) &
                    (doc_level['batiment'] == bat) &
                    (doc_level['indice_sort_order'] == iso)
                ]
                for idx in affected.index:
                    pos = doc_level.index.get_loc(idx)
                    doc_level.at[idx, 'anomaly_flags'] = (
                        doc_level.at[idx, 'anomaly_flags'] + ['SUSPECT_DUPLICATE']
                    )
                    log_event(
                        int(affected.at[idx, 'doc_id']) if pd.notna(affected.at[idx, 'doc_id']) else None,
                        'NM5', 'WARNING', 'SUSPECT_DUPLICATE',
                        f'Tied indice_sort_order={iso} for ({fk}, {lot}, {bat})',
                    )

    # Anomaly: indice_sort_order=0 (R-NM5-05)
    zero_indice = doc_level['indice_sort_order'] == 0
    if zero_indice.any():
        for idx in doc_level.index[zero_indice]:
            did = doc_level.at[idx, 'doc_id']
            log_event(
                int(did) if pd.notna(did) else None,
                'NM5', 'INFO', 'NULL_INDICE_ACTIVE',
                f'Document with indice_sort_order=0 (null/unparseable INDICE)',
            )

    # ------------------------------------------------------------------
    # Step 7: Build active_dataset
    # ------------------------------------------------------------------
    active_doc_ids = set(doc_level.loc[doc_level['is_active'], 'doc_id'])
    active_dataset = ged_long[ged_long['doc_id'].isin(active_doc_ids)].copy()

    # Join is_active and revision metadata back to ged_long
    doc_meta_cols = [
        'doc_id', 'is_latest_version', 'is_latest_indice', 'is_active',
        'is_legacy', 'exclusion_reason',
        'revision_count', 'previous_indice', 'has_revision_gap',
        'is_cross_lot',
    ]
    doc_meta = doc_level[doc_meta_cols].copy()
    ged_long_enriched = ged_long.merge(doc_meta, on='doc_id', how='left')

    # ------------------------------------------------------------------
    # Validation checks
    # ------------------------------------------------------------------
    assert doc_level['is_active'].notna().all(), "is_active has nulls"
    assert len(active_dataset) > 0, "active_dataset is empty"
    assert (doc_level['revision_count'] >= 1).all(), "revision_count < 1 found"

    # Check uniqueness: one active doc per (famille_key, lot, batiment)
    active_docs = doc_level[doc_level['is_active']]
    uniqueness_check = active_docs.groupby(
        ['famille_key', 'lot', 'batiment']
    ).size()
    violations = uniqueness_check[uniqueness_check > 1]
    if len(violations) > 0:
        for (fk, lot, bat), cnt in violations.items():
            log_event(
                None, 'NM5', 'WARNING', 'ACTIVE_UNIQUENESS_VIOLATION',
                f'Multiple active docs for ({fk}, {lot}, {bat}): {cnt}',
            )

    legacy_count = int(doc_level['is_legacy'].sum())
    log_event(
        None, 'NM5', 'INFO', 'NM5_COMPLETE',
        f'NM5 complete: {len(doc_level)} docs, '
        f'{len(active_doc_ids)} active, '
        f'{legacy_count} legacy excluded, '
        f'{len(ged_long_enriched)} enriched rows, '
        f'{len(active_dataset)} active_dataset rows',
    )

    return ged_long_enriched, doc_level, active_dataset
