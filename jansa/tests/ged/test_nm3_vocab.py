"""NM3-GED unit tests: response normalization & mission_type classification."""

import inspect

import pandas as pd
import pytest

from jansa.adapters.ged.nm3_vocab import (
    normalize_responses,
    classify_mission,
    normalize_response,
)
from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.logging import clear_log, get_log_as_dataframe


def _make_ged_long(rows):
    """Build a minimal NM1-output-like DataFrame for NM3 testing."""
    defaults = {
        'doc_id': 1, 'mission': None, 'reponse_raw': None,
        'repondant': None, 'date_reponse': pd.NaT,
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


# ===== mission_type classification =====

class TestMissionType:
    def test_mission_type_sas(self):
        assert classify_mission('0-SAS') == 'SAS'

    def test_mission_type_moex(self):
        assert classify_mission("B-Maître d'Oeuvre EXE") == 'MOEX'
        assert classify_mission("0-Maître d'Oeuvre EXE") == 'MOEX'
        assert classify_mission("Maitre d'Oeuvre EXE") == 'MOEX'

    def test_mission_type_reviewer(self):
        assert classify_mission('0-BET Structure') == 'REVIEWER'
        assert classify_mission('0-Bureau de Contrôle') == 'REVIEWER'
        assert classify_mission('0-AMO HQE') == 'REVIEWER'

    def test_mission_type_unknown(self):
        assert classify_mission(None) == 'UNKNOWN'
        assert classify_mission('') == 'UNKNOWN'
        assert classify_mission('  ') == 'UNKNOWN'

    def test_mission_type_never_null(self):
        df = _make_ged_long([
            {'doc_id': 1, 'mission': '0-SAS'},
            {'doc_id': 2, 'mission': None},
            {'doc_id': 3, 'mission': ''},
            {'doc_id': 4, 'mission': '0-BET Structure'},
        ])
        result = normalize_responses(df)
        assert result['mission_type'].isna().sum() == 0


# ===== Response vocabulary mapping =====

class TestVocabMapping:
    def test_all_known_vocab(self):
        """All 11 known values map correctly."""
        cases = [
            ('Validé sans observation - SAS', 'VSO', 'RESPONDED_APPROVE'),
            ('Validé sans observation', 'VSO', 'RESPONDED_APPROVE'),
            ('Validé avec observation', 'VAO', 'RESPONDED_APPROVE'),
            ('Favorable', 'FAV', 'RESPONDED_APPROVE'),
            ('Refusé', 'REF', 'RESPONDED_REJECT'),
            ('Défavorable', 'DEF', 'RESPONDED_REJECT'),
            ('Hors Mission', 'HM', 'RESPONDED_HM'),
            ('Suspendu', 'SUS', 'RESPONDED_OTHER'),
            ('Sollicitation supplémentaire', 'SUP', 'RESPONDED_OTHER'),
            ('En attente', None, 'NOT_RESPONDED'),
            ('Soumis', None, 'PENDING_CIRCUIT'),
        ]
        for raw, expected_norm, expected_status in cases:
            norm, status = normalize_response(raw)
            assert norm == expected_norm, f"'{raw}' → normalized={norm}, expected {expected_norm}"
            assert status == expected_status, f"'{raw}' → status={status}, expected {expected_status}"

    def test_sas_variant_priority(self):
        """'Validé sans observation - SAS' must match before 'Validé sans observation'."""
        norm, status = normalize_response('Validé sans observation - SAS')
        assert norm == 'VSO'
        assert status == 'RESPONDED_APPROVE'
        # Regular one still works
        norm2, _ = normalize_response('Validé sans observation')
        assert norm2 == 'VSO'

    def test_favorable_maps_approve(self):
        norm, status = normalize_response('Favorable')
        assert norm == 'FAV'
        assert status == 'RESPONDED_APPROVE'

    def test_defavorable_maps_reject(self):
        norm, status = normalize_response('Défavorable')
        assert norm == 'DEF'
        assert status == 'RESPONDED_REJECT'

    def test_en_attente_dual_repr(self):
        """En attente: normalized=null, status=NOT_RESPONDED, raw preserved."""
        df = _make_ged_long([{'doc_id': 1, 'reponse_raw': 'En attente', 'mission': 'X'}])
        result = normalize_responses(df)
        row = result.iloc[0]
        assert pd.isna(row['reponse_normalized'])
        assert row['response_status'] == 'NOT_RESPONDED'
        assert row['reponse_raw'] == 'En attente'

    def test_soumis_dual_repr(self):
        """Soumis: normalized=null, status=PENDING_CIRCUIT."""
        norm, status = normalize_response('Soumis pour avis')
        assert norm is None
        assert status == 'PENDING_CIRCUIT'

    def test_prefix_matching_with_suffix(self):
        """Favorable - En retard → FAV, RESPONDED_APPROVE (prefix match)."""
        norm, status = normalize_response('Favorable - En retard (3 jours)')
        assert norm == 'FAV'
        assert status == 'RESPONDED_APPROVE'

    def test_refus_with_suffix(self):
        """Refusé - En retard → REF, RESPONDED_REJECT."""
        norm, status = normalize_response('Refusé - En retard (5 jours)')
        assert norm == 'REF'
        assert status == 'RESPONDED_REJECT'


class TestUnknownVocab:
    def test_unknown_vocab_ambiguous(self):
        """Unknown → RESPONDED_AMBIGUOUS, log_event called."""
        df = _make_ged_long([{'doc_id': 99, 'reponse_raw': 'SomeNewValue', 'mission': 'X'}])
        result = normalize_responses(df)
        assert result.iloc[0]['response_status'] == 'RESPONDED_AMBIGUOUS'
        assert pd.isna(result.iloc[0]['reponse_normalized'])
        log_df = get_log_as_dataframe()
        warnings = log_df[log_df['code'] == 'UNKNOWN_RESPONSE_VOCABULARY']
        assert len(warnings) > 0

    def test_null_response(self):
        """Null raw → NOT_RESPONDED + null normalized."""
        norm, status = normalize_response(None)
        assert norm is None
        assert status == 'NOT_RESPONDED'

    def test_empty_response(self):
        norm, status = normalize_response('')
        assert norm is None
        assert status == 'NOT_RESPONDED'


class TestRawPreservation:
    def test_reponse_raw_unchanged(self):
        """reponse_raw never modified by NM3."""
        df = _make_ged_long([
            {'doc_id': 1, 'reponse_raw': 'Validé avec observation', 'mission': 'X'},
            {'doc_id': 2, 'reponse_raw': None, 'mission': 'Y'},
            {'doc_id': 3, 'reponse_raw': 'En attente', 'mission': 'Z'},
        ])
        original_raw = df['reponse_raw'].copy()
        result = normalize_responses(df)
        pd.testing.assert_series_equal(result['reponse_raw'], original_raw)


class TestResponseStatusNeverNull:
    def test_response_status_never_null(self):
        df = _make_ged_long([
            {'doc_id': 1, 'reponse_raw': 'Validé avec observation', 'mission': 'X'},
            {'doc_id': 2, 'reponse_raw': None, 'mission': 'Y'},
            {'doc_id': 3, 'reponse_raw': '', 'mission': 'Z'},
            {'doc_id': 4, 'reponse_raw': 'UnknownValue', 'mission': 'W'},
        ])
        result = normalize_responses(df)
        assert result['response_status'].isna().sum() == 0


class TestContract:
    def test_nm3_contract_missing_column(self):
        """ContractError raised if required columns absent."""
        df = pd.DataFrame({'doc_id': [1], 'mission': ['X']})  # missing reponse_raw
        with pytest.raises(ContractError):
            normalize_responses(df)

    def test_nm3_contract_missing_mission(self):
        df = pd.DataFrame({'doc_id': [1], 'reponse_raw': ['X']})  # missing mission
        with pytest.raises(ContractError):
            normalize_responses(df)


class TestNoForbiddenPatterns:
    def test_no_iterrows_in_nm3(self):
        import jansa.adapters.ged.nm3_vocab as m
        source = inspect.getsource(m)
        assert 'iterrows' not in source
        lines = [l for l in source.split('\n')
                 if l.strip().startswith('print(') or l.strip().startswith('print (')]
        assert len(lines) == 0, f"print() found: {lines}"
