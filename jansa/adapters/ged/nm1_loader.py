"""NM1-GED: GED Adapter & Document Extraction.

Ingests AxeoBIM GED export Excel file and produces a canonical normalized
dataset in JANSA long format. Strictly structural normalization — no
workflow semantics (mission_type, SAS, is_late, etc.).
"""

import pandas as pd
import numpy as np

from jansa.adapters.ged.exceptions import NM1InputError, NM1OutputError
from jansa.adapters.ged.logging import log_event, clear_log, get_log_as_dataframe
from jansa.adapters.ged.constants import (
    GED_PRIMARY_SHEET,
    GED_FALLBACK_SHEET,
    IDENTITY_COLS,
    FAMILY_KEY_COLS,
    REQUIRED_COLUMNS,
    COLUMN_RENAME_MAP,
)


def indice_to_sort(ind: object) -> int:
    """Convert INDICE string to numeric sort order.

    A=1, B=2, ..., Z=26, AA=27, AB=28, ..., numeric=int, null/empty=0.
    """
    if pd.isna(ind) or str(ind).strip() == '':
        return 0
    ind = str(ind).strip().upper()
    if len(ind) == 1 and ind.isalpha():
        return ord(ind) - ord('A') + 1
    if len(ind) == 2 and ind.isalpha():
        return 26 + (ord(ind[0]) - ord('A')) * 26 + (ord(ind[1]) - ord('A') + 1)
    try:
        return int(ind)
    except ValueError:
        return 0


def _load_sheet(filepath: str) -> tuple[pd.DataFrame, bool]:
    """Load primary or fallback sheet. Returns (df, has_type_document_ged)."""
    try:
        df = pd.read_excel(
            filepath,
            sheet_name=GED_PRIMARY_SHEET,
            header=1,
            dtype=str,
        )
        has_type_doc_ged = 'Type de document' in df.columns
        return df, has_type_doc_ged
    except ValueError:
        pass

    try:
        df = pd.read_excel(
            filepath,
            sheet_name=GED_FALLBACK_SHEET,
            header=1,
            dtype=str,
        )
        log_event(
            None, 'NM1', 'WARNING', 'FALLBACK_SHEET',
            f'Primary sheet not found, using fallback: {GED_FALLBACK_SHEET}',
        )
        return df, False
    except ValueError:
        raise NM1InputError(
            f"Neither '{GED_PRIMARY_SHEET}' nor '{GED_FALLBACK_SHEET}' found in workbook."
        )


def _validate_required_columns(df: pd.DataFrame) -> None:
    """Validate all required columns are present. Raise NM1InputError if not."""
    present = set(df.columns)
    missing = [c for c in REQUIRED_COLUMNS if c not in present]
    if missing:
        raise NM1InputError(f"Missing required columns: {missing}")


def _forward_fill_doc_id(df: pd.DataFrame) -> pd.DataFrame:
    """Step 3: detect document group boundaries via Identifiant forward-fill."""
    df['doc_id'] = pd.to_numeric(df['Identifiant'], errors='coerce')
    df['doc_id'] = df['doc_id'].ffill()
    # Convert to Int64 (nullable integer)
    df['doc_id'] = df['doc_id'].astype('Int64')
    return df


def _validate_forward_fill(df: pd.DataFrame) -> pd.DataFrame:
    """Post-ffill validation: check for orphan rows and identity consistency."""
    # Orphan rows: doc_id still null after ffill
    orphan_mask = df['doc_id'].isna()
    if orphan_mask.any():
        orphan_indices = df.loc[orphan_mask, 'row_index'].tolist()
        for idx in orphan_indices:
            log_event(
                None, 'NM1', 'ERROR', 'ORPHAN_ROWS',
                f'Row {idx} has no doc_id after forward-fill',
            )
        df.loc[orphan_mask, 'row_quality'] = 'ERROR'
        df.loc[orphan_mask, 'row_quality_details'] = df.loc[orphan_mask, 'row_quality_details'].apply(
            lambda x: x + ['ORPHAN_ROWS']
        )

    # Identity consistency within each doc_id group
    check_cols = ['AFFAIRE', 'LOT', 'NUMERO', 'INDICE']
    valid_docs = df.dropna(subset=['doc_id'])
    if len(valid_docs) > 0:
        for col in check_cols:
            if col not in df.columns:
                continue
            nunique = valid_docs.groupby('doc_id')[col].nunique()
            inconsistent_docs = nunique[nunique > 1].index
            if len(inconsistent_docs) > 0:
                mask = df['doc_id'].isin(inconsistent_docs)
                for doc_id in inconsistent_docs:
                    log_event(
                        int(doc_id), 'NM1', 'WARNING', 'FFILL_IDENTITY_MISMATCH',
                        f'Inconsistent {col} within doc_id {doc_id}',
                        field=col,
                    )
                df.loc[mask & (df['row_quality'] == 'OK'), 'row_quality'] = 'WARNING'
                df.loc[mask, 'row_quality_details'] = df.loc[mask, 'row_quality_details'].apply(
                    lambda x: x + ['FFILL_IDENTITY_MISMATCH'] if 'FFILL_IDENTITY_MISMATCH' not in x else x
                )
    return df


def _forward_fill_identity(df: pd.DataFrame) -> pd.DataFrame:
    """Step 4: forward-fill document identity fields."""
    cols_to_fill = [c for c in IDENTITY_COLS if c in df.columns]
    df[cols_to_fill] = df[cols_to_fill].ffill()
    return df


def _parse_types(df: pd.DataFrame) -> pd.DataFrame:
    """Step 5: parse and type identity fields."""
    # Version number: handle sub-1.0 floats (e.g. 0.2 → 2) per spec edge case
    version_float = pd.to_numeric(df['Version'], errors='coerce').fillna(0)
    # If version < 1.0, multiply by 10 to get integer form (0.2 → 2)
    df['version_number'] = np.where(
        (version_float > 0) & (version_float < 1.0),
        (version_float * 10).round().astype(int),
        version_float.round().astype(int),
    )

    # NUMERO as string (preserve leading zeros)
    df['NUMERO'] = df['NUMERO'].astype(str).str.strip()

    # Parse dates
    date_pairs = [
        ('Date de dépôt effectif', 'date_depot'),
        ('Date prévisionnelle', 'date_prevue'),
    ]
    for raw_col, parsed_col in date_pairs:
        if raw_col in df.columns:
            df[parsed_col] = pd.to_datetime(df[raw_col], errors='coerce', dayfirst=True)
            unparsed = df[raw_col].notna() & df[parsed_col].isna()
            if unparsed.any():
                bad_values = df.loc[unparsed, raw_col].unique()[:10]
                for val in bad_values:
                    log_event(
                        None, 'NM1', 'WARNING', 'UNPARSEABLE_DATE',
                        f'Cannot parse date in {raw_col}',
                        raw_value=str(val),
                        field=raw_col,
                    )
                df.loc[unparsed, 'row_quality'] = np.where(
                    df.loc[unparsed, 'row_quality'] == 'ERROR', 'ERROR', 'WARNING'
                )
                df.loc[unparsed, 'row_quality_details'] = df.loc[unparsed, 'row_quality_details'].apply(
                    lambda x: x + ['UNPARSEABLE_DATE']
                )

    # ecart_depot as numeric
    if 'Écart avec la date de dépôt prévue' in df.columns:
        df['ecart_depot_parsed'] = pd.to_numeric(
            df['Écart avec la date de dépôt prévue'], errors='coerce'
        )

    return df


def _compute_famille_key(df: pd.DataFrame) -> pd.DataFrame:
    """Step 6: compute famille_key from FAMILY_KEY_COLS."""
    df['famille_key'] = df[FAMILY_KEY_COLS].fillna('').agg('_'.join, axis=1)
    return df


def _compute_indice_sort_order(df: pd.DataFrame) -> pd.DataFrame:
    """Step 7: compute indice_sort_order via vectorized apply."""
    df['indice_sort_order'] = df['INDICE'].apply(indice_to_sort)
    return df


def _compute_doc_version_key(df: pd.DataFrame) -> pd.DataFrame:
    """Step 8: compute doc_version_key."""
    df['doc_version_key'] = (
        df['famille_key'] + '::' +
        df['INDICE'].fillna('') + '::' +
        df['version_number'].astype(str)
    )
    return df


def _parse_reviewer_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Step 10: parse reviewer response date fields."""
    df['deadline'] = pd.to_datetime(
        df['Date limite pour répondre'], errors='coerce', dayfirst=True
    )
    df['date_reponse'] = pd.to_datetime(
        df['Réponse donnée le'], errors='coerce', dayfirst=True
    )
    df['ecart_reponse'] = pd.to_numeric(
        df['Écart avec la date de réponse prévue'], errors='coerce'
    )

    # Log unparseable deadline dates
    for raw_col, parsed_col in [
        ('Date limite pour répondre', 'deadline'),
        ('Réponse donnée le', 'date_reponse'),
    ]:
        if raw_col in df.columns:
            unparsed = df[raw_col].notna() & df[parsed_col].isna()
            if unparsed.any():
                sample = df.loc[unparsed, raw_col].head(5)
                for val in sample.unique():
                    log_event(
                        None, 'NM1', 'WARNING', 'UNPARSEABLE_DATE',
                        f'Cannot parse date in {raw_col}',
                        raw_value=str(val),
                        field=raw_col,
                    )

    return df


def _detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Step 13: anomaly detection — emit events via log_event()."""
    # INDICE null or empty
    indice_missing = df['INDICE'].isna() | (df['INDICE'].astype(str).str.strip() == '')
    if indice_missing.any():
        doc_ids = df.loc[indice_missing, 'doc_id'].dropna().unique()
        for did in doc_ids:
            log_event(
                int(did), 'NM1', 'WARNING', 'MISSING_INDICE',
                f'INDICE is null or empty for doc_id {did}',
                field='INDICE',
            )
        df.loc[indice_missing & (df['row_quality'] == 'OK'), 'row_quality'] = 'WARNING'
        df.loc[indice_missing, 'row_quality_details'] = df.loc[indice_missing, 'row_quality_details'].apply(
            lambda x: x + ['MISSING_INDICE'] if 'MISSING_INDICE' not in x else x
        )

    # Mission null (but not separator rows — those are handled separately)
    mission_null = df['Mission'].isna()
    reponse_null = df['Réponse'].isna()
    repondant_null = df['Répondant'].isna() if 'Répondant' in df.columns else pd.Series(True, index=df.index)

    # Separator rows: Mission, Répondant, and Réponse all null
    separator_mask = mission_null & reponse_null & repondant_null
    df.loc[separator_mask & (df['row_quality'] != 'ERROR'), 'row_quality'] = 'WARNING'
    df.loc[separator_mask, 'row_quality_details'] = df.loc[separator_mask, 'row_quality_details'].apply(
        lambda x: x + ['EMPTY_REVIEWER_ROW'] if 'EMPTY_REVIEWER_ROW' not in x else x
    )

    # Mission null but not separator (has reponse or repondant)
    missing_mission = mission_null & ~separator_mask
    if missing_mission.any():
        doc_ids = df.loc[missing_mission, 'doc_id'].dropna().unique()
        for did in doc_ids:
            log_event(
                int(did), 'NM1', 'WARNING', 'MISSING_MISSION',
                f'Mission is null for non-separator row in doc_id {did}',
                field='Mission',
            )

    # version_number = 0
    version_zero = df['version_number'] == 0
    if version_zero.any():
        doc_ids = df.loc[version_zero, 'doc_id'].dropna().unique()
        for did in doc_ids:
            log_event(
                int(did), 'NM1', 'WARNING', 'VERSION_ZERO',
                f'version_number is 0 (unparseable) for doc_id {did}',
                field='Version',
            )

    # doc_version_key structural consistency check
    # In GED long format, multiple rows per doc_version_key is expected (one per reviewer).
    # Only flag if the same doc_version_key maps to multiple distinct doc_id values,
    # which indicates a structural inconsistency.
    key_doc_counts = df.loc[~separator_mask].groupby('doc_version_key')['doc_id'].nunique()
    inconsistent_keys = key_doc_counts[key_doc_counts > 1].index
    if len(inconsistent_keys) > 0:
        for key in inconsistent_keys:
            doc_ids = df.loc[df['doc_version_key'] == key, 'doc_id'].dropna().unique()
            for did in doc_ids:
                log_event(
                    int(did), 'NM1', 'WARNING', 'DOC_VERSION_KEY_INCONSISTENCY',
                    f'doc_version_key {key} maps to multiple doc_ids: {list(doc_ids)}',
                    field='doc_version_key',
                )

    # Commentaire non-null (INFO)
    if 'Commentaire' in df.columns:
        has_comment = df['Commentaire'].notna() & (df['Commentaire'].str.strip() != '')
        if has_comment.any():
            count = has_comment.sum()
            log_event(
                None, 'NM1', 'INFO', 'COMMENTS_PRESENT',
                f'{count} rows have reviewer comments',
                field='Commentaire',
            )

    return df


def _rename_and_select_output(df: pd.DataFrame, has_type_doc_ged: bool) -> pd.DataFrame:
    """Rename columns to internal aliases and select output schema."""
    # Step 11: Preserve reponse_raw (GP-TRACE)
    # The COLUMN_RENAME_MAP maps 'Réponse' -> 'reponse_raw', so renaming handles this.
    # But we need to drop columns that would conflict with already-computed ones.
    # 'ecart_reponse' is already computed in _parse_reviewer_dates, so skip the rename for it.
    rename = {}
    skip_rename = {'Écart avec la date de réponse prévue'}  # already computed as ecart_reponse
    for k, v in COLUMN_RENAME_MAP.items():
        if k in df.columns and k not in skip_rename:
            rename[k] = v
    df = df.rename(columns=rename)
    # Drop the original ecart column if still present
    if 'Écart avec la date de réponse prévue' in df.columns:
        df = df.drop(columns=['Écart avec la date de réponse prévue'])

    # type_document_ged handling
    if not has_type_doc_ged:
        df['type_document_ged'] = pd.NA

    # Parse ecart_depot
    if 'ecart_depot_parsed' in df.columns:
        df['ecart_depot'] = df['ecart_depot_parsed']

    # Select output columns (order per spec)
    output_cols = [
        'doc_id', 'row_index', 'chemin', 'affaire', 'tranche', 'batiment',
        'phase', 'emetteur', 'specialite', 'lot', 'type_doc', 'zone',
        'niveau', 'numero', 'indice', 'libelle', 'format_fichier',
        'deposant', 'date_depot_raw', 'date_depot', 'date_prevue_raw',
        'date_prevue', 'ecart_depot', 'version_number',
        'famille_key', 'doc_version_key', 'indice_sort_order',
        'mission', 'repondant', 'deadline_raw', 'deadline',
        'date_reponse_raw', 'date_reponse', 'ecart_reponse',
        'reponse_raw', 'commentaire', 'pieces_jointes',
        'type_document_ged',
        'row_quality', 'row_quality_details',
    ]

    # Only select columns that exist
    final_cols = [c for c in output_cols if c in df.columns]
    return df[final_cols]


def load_ged_export(filepath: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load GED export and produce canonical normalized dataset.

    Returns: (ged_long, import_log)
    """
    # Clear log for fresh run
    clear_log()

    # Step 1: Load sheet
    df, has_type_doc_ged = _load_sheet(filepath)

    # Contract validation
    _validate_required_columns(df)

    # Early exit if no data rows
    if len(df) == 0:
        raise NM1OutputError("NM1 produced zero output rows.")

    # Step 2: Add traceability fields
    df['row_index'] = df.index + 2  # Excel row number (1-based, +header, +0-index)

    # Step 12: Row quality initialization (early so ffill validation can use it)
    df['row_quality'] = 'OK'
    df['row_quality_details'] = [[] for _ in range(len(df))]

    # Step 3: Forward-fill doc_id
    df = _forward_fill_doc_id(df)

    # Validation after forward-fill
    df = _validate_forward_fill(df)

    # Step 4: Forward-fill identity fields
    df = _forward_fill_identity(df)

    # Step 5: Parse types
    df = _parse_types(df)

    # Step 6: Compute famille_key
    df = _compute_famille_key(df)

    # Step 7: Compute indice_sort_order
    df = _compute_indice_sort_order(df)

    # Step 8: Compute doc_version_key
    df = _compute_doc_version_key(df)

    # Step 9: mission passthrough (no mission_type classification)
    # df['Mission'] is already present — will be renamed to 'mission' in output

    # Step 10: Parse reviewer dates
    df = _parse_reviewer_dates(df)

    # Step 11: reponse_raw preservation (handled in _rename_and_select_output)

    # Step 13: Anomaly detection
    df = _detect_anomalies(df)

    # Step 14: Rename and select output
    ged_long = _rename_and_select_output(df, has_type_doc_ged)

    # Post-load validation
    if len(ged_long) == 0:
        raise NM1OutputError("NM1 produced zero output rows.")

    log_event(
        None, 'NM1', 'INFO', 'NM1_COMPLETE',
        f'NM1 complete: {len(ged_long)} rows, {ged_long["doc_id"].nunique()} documents',
    )

    # Step 15: Return
    import_log = get_log_as_dataframe()
    return ged_long, import_log
