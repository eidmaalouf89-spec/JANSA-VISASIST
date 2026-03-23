"""NM2-GED unit tests: SAS state interpretation."""

import inspect

import pandas as pd
import pytest

from jansa.adapters.ged.nm2_sas import interpret_sas
from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe


def _make_nm3_output(rows):
    """Build a minimal NM3-output-like DataFrame for NM2 testing."""
    defaults = {
        'doc_id': 1, 'mission_type': 'REVIEWER', 'mission': 'X',
        'reponse_raw': None, 'reponse_normalized': None,
        'response_status': 'NOT_RESPONDED', 'repondant': None,
        'date_reponse': pd.NaT,
    }
    data = []
    for r in rows:
        row = dict(defaults)
        row.update(r)
        data.append(row)
    return pd.DataFrame(data)


@pytest.fixture(autouse=True)
def _clear():
    clear_log()


class TestSASClassification:
    def test_sas_blocked_from_responded_reject(self):
        df = _make_nm3_output([
            {'doc_id': 1, 'mission_type': 'SAS', 'reponse_normalized': 'REF',
             'response_status': 'RESPONDED_REJECT', 'repondant': 'Alice'},
            {'doc_id': 1, 'mission_type': 'REVIEWER', 'response_status': 'NOT_RESPONDED'},
        ])
        result = interpret_sas(df)
        sas = result[result['doc_id'] == 1].iloc[0]
        assert sas['sas_state'] == 'SAS_BLOCKED'
        assert sas['sas_verdict'] == 'REF'
        assert sas['sas_confidence'] == 'HIGH'

    def test_sas_passed_from_responded_approve(self):
        df = _make_nm3_output([
            {'doc_id': 1, 'mission_type': 'SAS', 'reponse_normalized': 'VSO',
             'response_status': 'RESPONDED_APPROVE', 'repondant': 'Bob'},
            {'doc_id': 1, 'mission_type': 'REVIEWER', 'response_status': 'NOT_RESPONDED'},
        ])
        result = interpret_sas(df)
        sas = result[result['doc_id'] == 1].iloc[0]
        assert sas['sas_state'] == 'SAS_PASSED'
        assert sas['sas_verdict'] == 'VSO'
        assert sas['sas_confidence'] == 'HIGH'

    def test_sas_pending_from_not_responded(self):
        df = _make_nm3_output([
            {'doc_id': 1, 'mission_type': 'SAS', 'reponse_normalized': None,
             'response_status': 'NOT_RESPONDED'},
        ])
        result = interpret_sas(df)
        sas = result[result['doc_id'] == 1].iloc[0]
        assert sas['sas_state'] == 'SAS_PENDING'
        assert pd.isna(sas['sas_verdict']) or sas['sas_verdict'] is None
        assert sas['sas_confidence'] == 'HIGH'


class TestNoSASRow:
    def test_sas_unknown_no_row(self):
        """No SAS row → SAS_UNKNOWN + SAS_ASSUMED_PASSED flag."""
        df = _make_nm3_output([
            {'doc_id': 1, 'mission_type': 'REVIEWER', 'response_status': 'NOT_RESPONDED'},
            {'doc_id': 2, 'mission_type': 'MOEX', 'response_status': 'RESPONDED_APPROVE'},
        ])
        result = interpret_sas(df)
        assert len(result) == 2  # one row per doc_id
        for _, row in result.iterrows():
            assert row['sas_state'] == 'SAS_UNKNOWN'
            assert row['sas_confidence'] == 'LOW'
            assert 'SAS_ASSUMED_PASSED' in row['inference_flags']


class TestUnexpectedStatus:
    def test_sas_unknown_unexpected_status(self):
        """HM on SAS row → SAS_UNKNOWN + WARNING."""
        df = _make_nm3_output([
            {'doc_id': 1, 'mission_type': 'SAS', 'reponse_normalized': 'HM',
             'response_status': 'RESPONDED_HM'},
        ])
        result = interpret_sas(df)
        sas = result[result['doc_id'] == 1].iloc[0]
        assert sas['sas_state'] == 'SAS_UNKNOWN'
        log_df = get_log_as_dataframe()
        warnings = log_df[log_df['code'] == 'UNEXPECTED_SAS_STATUS']
        assert len(warnings) > 0


class TestMultipleSASRows:
    def test_multiple_sas_rows(self):
        """Multiple SAS rows for same doc — keep most recent, log warning."""
        df = _make_nm3_output([
            {'doc_id': 1, 'mission_type': 'SAS', 'reponse_normalized': None,
             'response_status': 'NOT_RESPONDED', 'date_reponse': pd.Timestamp('2023-01-01')},
            {'doc_id': 1, 'mission_type': 'SAS', 'reponse_normalized': 'VSO',
             'response_status': 'RESPONDED_APPROVE', 'date_reponse': pd.Timestamp('2023-06-01')},
        ])
        result = interpret_sas(df)
        doc1 = result[result['doc_id'] == 1]
        assert len(doc1) == 1
        assert doc1.iloc[0]['sas_state'] == 'SAS_PASSED'  # most recent wins
        log_df = get_log_as_dataframe()
        assert any(log_df['code'] == 'MULTIPLE_SAS_ROWS')


class TestOneRowPerDoc:
    def test_one_row_per_doc(self):
        """Output has exactly one row per doc_id."""
        df = _make_nm3_output([
            {'doc_id': 1, 'mission_type': 'SAS', 'response_status': 'RESPONDED_APPROVE',
             'reponse_normalized': 'VSO'},
            {'doc_id': 1, 'mission_type': 'REVIEWER', 'response_status': 'NOT_RESPONDED'},
            {'doc_id': 2, 'mission_type': 'REVIEWER', 'response_status': 'NOT_RESPONDED'},
            {'doc_id': 2, 'mission_type': 'MOEX', 'response_status': 'RESPONDED_APPROVE',
             'reponse_normalized': 'VAO'},
            {'doc_id': 3, 'mission_type': 'REVIEWER', 'response_status': 'RESPONDED_APPROVE',
             'reponse_normalized': 'VAO'},
        ])
        result = interpret_sas(df)
        assert len(result) == 3
        assert result['doc_id'].nunique() == 3
        assert result['sas_state'].isna().sum() == 0
        assert result['sas_confidence'].isna().sum() == 0


class TestContract:
    def test_nm2_contract_missing_column(self):
        """ContractError raised if mission_type absent."""
        df = pd.DataFrame({'doc_id': [1], 'reponse_normalized': ['VSO'],
                           'response_status': ['RESPONDED_APPROVE']})
        with pytest.raises(ContractError):
            interpret_sas(df)


class TestNoForbiddenPatterns:
    def test_no_iterrows_in_nm2(self):
        import jansa.adapters.ged.nm2_sas as m
        source = inspect.getsource(m)
        assert 'iterrows' not in source
        lines = [l for l in source.split('\n')
                 if l.strip().startswith('print(') or l.strip().startswith('print (')]
        assert len(lines) == 0, f"print() found: {lines}"
