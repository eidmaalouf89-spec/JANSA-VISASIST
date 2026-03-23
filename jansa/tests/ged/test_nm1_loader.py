"""NM1-GED unit tests.

All tests use inline fixtures — no file I/O.
Tests validate ingestion and structural normalization ONLY.
No mission_type, no is_late, no response normalization.
"""

import inspect
import io
import tempfile
import os

import pandas as pd
import pytest

from jansa.adapters.ged.nm1_loader import load_ged_export, indice_to_sort
from jansa.adapters.ged.logging import clear_log, get_log, get_log_as_dataframe
from jansa.adapters.ged.exceptions import NM1InputError, NM1OutputError
from jansa.tests.ged.fixtures.sample_ged_rows import (
    SAMPLE_ROWS, SINGLE_DOC_ROW, INDICE_TEST_ROWS,
    REVISION_ROWS, BAD_DATE_ROW,
)


def _make_excel(rows, sheet_name='Vue détaillée des documents 1'):
    """Create a temporary Excel file from row dicts matching GED export format.

    Row 0: section label ('Informations documents')
    Row 1: column headers
    Row 2+: data
    This matches pd.read_excel(..., header=1) expectations.
    """
    from openpyxl import Workbook
    from jansa.tests.ged.fixtures.sample_ged_rows import _BASE

    tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name

    # Use all columns from _BASE template to ensure all required columns present
    columns = list(_BASE.keys())

    # Row 1 (Excel): section label
    ws.append(['Informations documents'] + [None] * (len(columns) - 1))

    # Row 2 (Excel): column headers
    ws.append(columns)

    # Row 3+ (Excel): data rows
    for row in rows:
        ws.append([row.get(col) for col in columns])

    wb.save(tmp.name)
    return tmp.name


@pytest.fixture(autouse=True)
def _clear_log():
    """Clear centralized log before each test."""
    clear_log()


class TestForwardFillDocId:
    def test_forward_fill_doc_id(self):
        """Verify doc_id is filled for all rows including separator rows."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            assert ged_long['doc_id'].isna().sum() == 0, "Null doc_ids after ffill"
            # Separator row (index 3) should inherit doc_id from doc 60003453
            sep_rows = ged_long[ged_long['row_quality_details'].apply(lambda x: 'EMPTY_REVIEWER_ROW' in x)]
            assert len(sep_rows) > 0, "No separator rows detected"
            assert sep_rows['doc_id'].isna().sum() == 0, "Separator row has null doc_id"
        finally:
            os.unlink(path)


class TestForwardFillIntegrity:
    def test_forward_fill_integrity(self):
        """Verify AFFAIRE, LOT, NUMERO consistent within each doc_id group."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            for doc_id in ged_long['doc_id'].unique():
                group = ged_long[ged_long['doc_id'] == doc_id]
                assert group['affaire'].nunique() <= 1, f"AFFAIRE inconsistent for doc_id {doc_id}"
                assert group['lot'].nunique() <= 1 or group['lot'].isna().all(), \
                    f"LOT inconsistent for doc_id {doc_id}"
        finally:
            os.unlink(path)


class TestFamilleKey:
    def test_famille_key_construction(self):
        """Verify famille_key excludes INDICE. Same family, different INDICE -> same key."""
        path = _make_excel(REVISION_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            keys = ged_long['famille_key'].unique()
            # Both rows have same family but different INDICE (A vs B)
            assert len(keys) == 1, f"Expected 1 unique famille_key for revisions, got {len(keys)}"
            # Verify INDICE is NOT part of the key
            key = keys[0]
            assert '::A' not in key and '::B' not in key, "famille_key should not contain INDICE"
        finally:
            os.unlink(path)


class TestDocVersionKey:
    def test_doc_version_key_uniqueness(self):
        """Verify unique key per (famille_key, INDICE, version_number)."""
        path = _make_excel(REVISION_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            # Two rows with different INDICE should have different doc_version_key
            assert ged_long['doc_version_key'].nunique() == 2
            # Keys should contain INDICE
            keys = ged_long['doc_version_key'].tolist()
            assert '::A::' in keys[0] or '::B::' in keys[0]
        finally:
            os.unlink(path)


class TestIndiceSortOrder:
    def test_indice_sort_order(self):
        """A=1, B=2, Z=26, AA=27, AB=28, null=0."""
        assert indice_to_sort('A') == 1
        assert indice_to_sort('B') == 2
        assert indice_to_sort('Z') == 26
        assert indice_to_sort('AA') == 27
        assert indice_to_sort('AB') == 28
        assert indice_to_sort(None) == 0
        assert indice_to_sort('') == 0
        assert indice_to_sort('a') == 1  # case insensitive

    def test_indice_sort_in_dataframe(self):
        """Verify indice_sort_order computed correctly in full pipeline."""
        path = _make_excel(INDICE_TEST_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            sort_map = dict(zip(ged_long['indice'], ged_long['indice_sort_order']))
            assert sort_map.get('A') == 1
            assert sort_map.get('B') == 2
            assert sort_map.get('Z') == 26
            assert sort_map.get('AA') == 27
            assert sort_map.get('AB') == 28
        finally:
            os.unlink(path)


class TestReponseRawPreserved:
    def test_reponse_raw_preserved(self):
        """reponse_raw always equals source Réponse. Never modified."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            # reponse_raw must be a Series (not DataFrame — no duplicate columns)
            assert isinstance(ged_long['reponse_raw'], pd.Series), \
                "reponse_raw is duplicated — should be a single column"
            expected_responses = [r.get('Réponse') for r in SAMPLE_ROWS]
            actual = ged_long['reponse_raw'].tolist()
            for exp, act in zip(expected_responses, actual):
                if exp is None:
                    assert pd.isna(act), f"Expected null, got {act}"
                else:
                    assert act == exp, f"Expected '{exp}', got '{act}'"
        finally:
            os.unlink(path)


class TestMissionPassthrough:
    def test_mission_column_passed_through_raw(self):
        """mission column preserved as-is, no mission_type column in output."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            assert 'mission_type' not in ged_long.columns, \
                "mission_type must NOT be in NM1 output"
            assert 'mission' in ged_long.columns
            # First row should have original mission value
            assert ged_long.iloc[0]['mission'] == '0-BET Structure'
        finally:
            os.unlink(path)


class TestEcartReponseNoIsLate:
    def test_ecart_reponse_preserved_no_is_late(self):
        """ecart_reponse present, is_late NOT present in output."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            assert 'is_late' not in ged_long.columns, \
                "is_late must NOT be in NM1 output"
            assert 'ecart_reponse' in ged_long.columns
            # ecart_reponse must be a Series (no duplicate columns)
            assert isinstance(ged_long['ecart_reponse'], pd.Series), \
                "ecart_reponse is duplicated — should be a single column"
            # First row has ecart -13
            notna_mask = ged_long['ecart_reponse'].notna()
            first_with_ecart = ged_long.loc[notna_mask].iloc[0]
            assert first_with_ecart['ecart_reponse'] == -13.0
        finally:
            os.unlink(path)


class TestSeparatorRowHandling:
    def test_separator_row_handling(self):
        """Rows with null Mission and null Réponse are kept with row_quality=WARNING."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            sep = ged_long[ged_long['row_quality_details'].apply(lambda x: 'EMPTY_REVIEWER_ROW' in x)]
            assert len(sep) >= 1, "Expected at least 1 separator row"
            assert all(sep['row_quality'] == 'WARNING')
        finally:
            os.unlink(path)


class TestDateParsing:
    def test_date_parsing_valid(self):
        """Valid dates parsed to datetime."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            ged_long, _ = load_ged_export(path)
            # First row has date_reponse = 13/12/2023
            first = ged_long.iloc[0]
            assert pd.notna(first['date_reponse'])
            assert first['date_reponse'].day == 13
            assert first['date_reponse'].month == 12
            assert first['date_reponse'].year == 2023
        finally:
            os.unlink(path)

    def test_date_parsing_invalid(self):
        """Invalid dates -> null + WARNING in log."""
        path = _make_excel(BAD_DATE_ROW)
        try:
            ged_long, log_df = load_ged_export(path)
            assert pd.isna(ged_long.iloc[0]['date_depot'])
            warnings = log_df[
                (log_df['severity'] == 'WARNING') &
                (log_df['code'] == 'UNPARSEABLE_DATE')
            ]
            assert len(warnings) > 0, "Expected UNPARSEABLE_DATE warning"
        finally:
            os.unlink(path)


class TestLogEventUsed:
    def test_log_event_used_for_anomalies(self):
        """All anomaly events appear in log returned by get_log_as_dataframe()."""
        path = _make_excel(SAMPLE_ROWS)
        try:
            _, log_df = load_ged_export(path)
            assert len(log_df) > 0, "Expected log events"
            assert 'module' in log_df.columns
            assert all(log_df['module'] == 'NM1')
            assert 'severity' in log_df.columns
            assert 'code' in log_df.columns
        finally:
            os.unlink(path)


class TestMissingSheet:
    def test_missing_sheet_raises_error(self):
        """NM1InputError raised when primary sheet not found."""
        path = _make_excel(SAMPLE_ROWS, sheet_name='WrongSheet')
        try:
            with pytest.raises(NM1InputError):
                load_ged_export(path)
        finally:
            os.unlink(path)


class TestZeroRows:
    def test_zero_rows_raises_error(self):
        """NM1OutputError raised when output is empty."""
        path = _make_excel([])  # no data rows, just headers
        try:
            with pytest.raises(NM1OutputError):
                load_ged_export(path)
        finally:
            os.unlink(path)


class TestNoIterrows:
    def test_no_iterrows_in_nm1(self):
        """Lint check: nm1_loader.py source must not contain 'iterrows'."""
        import jansa.adapters.ged.nm1_loader as m
        source = inspect.getsource(m)
        assert 'iterrows' not in source, "iterrows() is forbidden in NM1"

    def test_no_print_in_nm1(self):
        """nm1_loader.py must not contain print() calls."""
        import jansa.adapters.ged.nm1_loader as m
        source = inspect.getsource(m)
        # Allow 'print' in docstrings/comments but not as function call
        lines = [l for l in source.split('\n')
                 if l.strip().startswith('print(') or l.strip().startswith('print (')]
        assert len(lines) == 0, f"print() found in NM1: {lines}"
