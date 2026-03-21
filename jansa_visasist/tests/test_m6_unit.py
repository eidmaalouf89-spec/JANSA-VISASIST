"""
Module 6 — Unit Tests.

Tests for parser, normalizer, executor, formatter.
"""

import os
import pytest
import json
import tempfile
from unittest.mock import patch

from jansa_visasist.pipeline.m6.normalizer import normalize_query, normalize_text
from jansa_visasist.pipeline.m6.parser import parse_query, ParseResult
from jansa_visasist.pipeline.m6.executor import (
    execute_c1, execute_c2, execute_c3, execute_c4, execute_c5,
    execute_c6, execute_c7, execute_c8, execute_c9, execute_c10,
    execute_c11, execute_c12, execute_command,
)
from jansa_visasist.pipeline.m6.formatter import (
    ChatbotResponse, format_response, _apply_truncation,
    _build_data_references,
)
from jansa_visasist.pipeline.m6.dictionaries import (
    build_lot_aliases, build_approver_aliases, build_category_aliases,
    build_status_synonyms, build_action_keywords, _normalize_key,
)
from jansa_visasist.pipeline.m6.indexes import (
    build_queue_index, build_doc_index, build_lot_index, build_approver_indexes,
)
from jansa_visasist.pipeline.m6.exporter import export_to_csv
from jansa_visasist.context_m6 import Module6Context
from jansa_visasist.config_m6 import (
    CHATBOT_INLINE_LIMIT, CHATBOT_PREVIEW_COUNT,
    FIELDS_FILTER_ROW, FIELDS_C6_DISAMBIGUATION,
)


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────

def _make_item(row_id, document, source_sheet, category="BLOCKED", priority_score=50.0,
               is_overdue=False, days_overdue=0, ind="A", titre="Test",
               missing_approvers=None, blocking_approvers=None,
               assigned_approvers=None, consensus_type="INCOMPLETE",
               observations=None, revision_count=1, **kwargs):
    """Create a minimal M3 queue item for testing."""
    item = {
        "row_id": row_id,
        "document": document,
        "titre": titre,
        "source_sheet": source_sheet,
        "lot": source_sheet,
        "ind": ind,
        "category": category,
        "consensus_type": consensus_type,
        "priority_score": priority_score,
        "is_overdue": is_overdue,
        "days_overdue": days_overdue,
        "missing_approvers": missing_approvers or [],
        "blocking_approvers": blocking_approvers or [],
        "assigned_approvers": assigned_approvers or [],
        "observations": observations,
        "revision_count": revision_count,
        "days_since_diffusion": 30,
        "has_deadline": True,
        "total_assigned": len(assigned_approvers or []),
        "replied": 0,
        "pending": len(assigned_approvers or []),
    }
    # Add approver statut columns
    from jansa_visasist.config import CANONICAL_APPROVERS
    for appr in CANONICAL_APPROVERS:
        item[f"{appr}_statut"] = kwargs.get(f"{appr}_statut")
    item.update(kwargs)
    return item


@pytest.fixture
def sample_queue():
    """Sample queue data for testing."""
    return [
        _make_item("1_10", "P17T2INEXEVDPTERI001MTDTZFD026005", "LOT 42-PLB-UTB",
                    category="BLOCKED", priority_score=80.0, is_overdue=True, days_overdue=30,
                    missing_approvers=["SOCOTEC"], blocking_approvers=["BET_EGIS"],
                    assigned_approvers=["SOCOTEC", "BET_EGIS", "MOEX_GEMO"],
                    SOCOTEC_statut=None, BET_EGIS_statut="REF", MOEX_GEMO_statut="VSO"),
        _make_item("2_20", "P17T2INEXEVDPTERI001CPETZFD026001", "LOT 42-PLB-UTB",
                    category="EASY_WIN_APPROVE", priority_score=60.0, is_overdue=False,
                    assigned_approvers=["MOEX_GEMO"], MOEX_GEMO_statut="VSO"),
        _make_item("3_30", "P17T2INEXEVDPTERI003MTDTZFD099001", "LOT 03-GOE-LGD",
                    category="WAITING", priority_score=40.0, is_overdue=True, days_overdue=10,
                    missing_approvers=["SOCOTEC", "ARCHI_MOX"],
                    assigned_approvers=["SOCOTEC", "ARCHI_MOX"]),
        _make_item("4_40", "P17T2INEXEVDPTERI001MTDTZFD026005", "LOT 03-GOE-LGD",
                    category="BLOCKED", priority_score=70.0, is_overdue=True, days_overdue=20,
                    missing_approvers=["SOCOTEC"],
                    assigned_approvers=["SOCOTEC"],
                    ind="B"),
        _make_item("5_50", "P17T2INEXEVDPTERIDOCUNIQUE00001", "LOT 03-GOE-LGD",
                    category="FAST_REJECT", priority_score=30.0, is_overdue=False,
                    assigned_approvers=["BET_SPK"]),
    ]


@pytest.fixture
def ctx(sample_queue):
    """Build a Module6Context from sample data."""
    queue_index = build_queue_index(sample_queue)
    doc_index = build_doc_index(sample_queue)
    lot_index = build_lot_index(sample_queue)
    missing_idx, blocking_idx, assigned_idx = build_approver_indexes(sample_queue)

    return Module6Context(
        queue_data=sample_queue,
        queue_index=queue_index,
        doc_index=doc_index,
        lot_index=lot_index,
        approver_missing_index=missing_idx,
        approver_blocking_index=blocking_idx,
        approver_assigned_index=assigned_idx,
        lot_aliases=build_lot_aliases(sample_queue),
        approver_aliases=build_approver_aliases(),
        category_aliases=build_category_aliases(),
        status_synonyms=build_status_synonyms(),
        action_keywords=build_action_keywords(),
        m4_data={},
        m5_data={},
        export_dir=tempfile.mkdtemp(),
    )


# ══════════════════════════════════════════════
# Normalizer Tests
# ══════════════════════════════════════════════

class TestNormalizer:
    """Tests for query normalization."""

    def test_strips_whitespace_lowercases_removes_accents(self):
        nq = normalize_query("  Résumé LOT 42  ")
        assert "resume" in nq.normalized
        assert nq.normalized == nq.normalized.strip()
        assert nq.normalized == nq.normalized.lower()

    def test_extracts_document_reference(self):
        nq = normalize_query("show P17T2INEXEVDPTERI001MTDTZFD026005")
        assert nq.doc_ref is not None
        assert "p17t2inexevdpteri001mtdtzfd026005" == nq.doc_ref

    def test_extracts_n_from_top_10(self):
        nq = normalize_query("top 10 blocked")
        assert nq.n_value == 10

    def test_extracts_n_from_5_premiers(self):
        nq = normalize_query("5 premiers en retard")
        assert nq.n_value == 5

    def test_preserves_underscores_in_doc_refs(self):
        # Document refs can contain underscores
        nq = normalize_query("P17T2_DOC_TEST")
        assert nq.doc_ref is not None
        assert "p17t2_doc_test" == nq.doc_ref


# ══════════════════════════════════════════════
# Parser Tests
# ══════════════════════════════════════════════

class TestParser:
    """Tests for L2 deterministic parser."""

    def _parse(self, query, ctx_fixture):
        nq = normalize_query(query)
        return parse_query(
            nq,
            ctx_fixture.lot_aliases,
            ctx_fixture.approver_aliases,
            ctx_fixture.category_aliases,
            ctx_fixture.status_synonyms,
            ctx_fixture.action_keywords,
        )

    def test_explain_detected(self, ctx):
        result = self._parse("pourquoi P17T2INEXEVDPTERI001MTDTZFD026005", ctx)
        assert result.command_id == "C7"

    def test_export_detected(self, ctx):
        result = self._parse("export lot 42", ctx)
        assert result.command_id == "C12"

    def test_count_detected(self, ctx):
        result = self._parse("combien de bloques", ctx)
        assert result.command_id == "C10"

    def test_summary_lot(self, ctx):
        result = self._parse("resume lot 42", ctx)
        assert result.command_id == "C8"

    def test_summary_approver(self, ctx):
        result = self._parse("resume socotec", ctx)
        assert result.command_id == "C9"

    def test_top_n_precedence(self, ctx):
        """Top-N always resolves to C11, not C1 or C2."""
        result = self._parse("top 10 blocked lot 42", ctx)
        assert result.command_id == "C11"
        assert result.parameters.get("n") == 10

    def test_combined_filter(self, ctx):
        result = self._parse("bloques lot 42", ctx)
        assert result.command_id == "C5"

    def test_single_filter_lot(self, ctx):
        result = self._parse("lot 42", ctx)
        assert result.command_id == "C1"

    def test_single_filter_category(self, ctx):
        result = self._parse("blocked", ctx)
        assert result.command_id == "C2"

    def test_single_filter_approver(self, ctx):
        result = self._parse("socotec", ctx)
        assert result.command_id == "C3"

    def test_overdue(self, ctx):
        result = self._parse("en retard", ctx)
        assert result.command_id == "C4"

    def test_document_lookup(self, ctx):
        result = self._parse("P17T2INEXEVDPTERIDOCUNIQUE00001", ctx)
        assert result.command_id == "C6"

    def test_rejected_gibberish(self, ctx):
        result = self._parse("xyzzy foobar baz", ctx)
        assert result.command_id == "REJECTED"


# ══════════════════════════════════════════════
# Executor Tests
# ══════════════════════════════════════════════

class TestExecutor:
    """Tests for C1-C12 execution functions."""

    def test_c6_not_found(self, ctx):
        results, count, extra = execute_c6(ctx, {"document": "nonexistent_doc"})
        assert count == 0
        assert results == []

    def test_c6_unique(self, ctx):
        results, count, extra = execute_c6(ctx, {"document": "p17t2inexevdpteridocunique00001"})
        assert count == 1
        assert len(results) == 1
        assert results[0]["row_id"] == "5_50"

    def test_c6_ambiguous(self, ctx):
        """Same document in two lots -> disambiguation list."""
        results, count, extra = execute_c6(ctx, {"document": "p17t2inexevdpteri001mtdtzfd026005"})
        assert count == 2
        assert extra.get("disambiguation") is True
        row_ids = {r["row_id"] for r in results}
        assert "1_10" in row_ids
        assert "4_40" in row_ids

    def test_c1_returns_lot_items_sorted(self, ctx):
        results, count, extra = execute_c1(ctx, {"lot": "LOT 42-PLB-UTB"})
        assert count == 2
        # Should be sorted by priority descending
        assert results[0]["priority_score"] >= results[1]["priority_score"]

    def test_c10_returns_count_only(self, ctx):
        results, count, extra = execute_c10(ctx, {"category": "BLOCKED"})
        assert results == []
        assert count == 2  # Two BLOCKED items in sample

    def test_c11_returns_top_n(self, ctx):
        results, count, extra = execute_c11(ctx, {"n": 2})
        assert len(results) == 2
        assert count == 5  # Total items

    def test_c12_returns_empty_results(self, ctx):
        results, count, extra = execute_c12(ctx, {})
        assert extra.get("raw_items") is not None
        assert count == 5

    def test_c4_returns_overdue(self, ctx):
        results, count, extra = execute_c4(ctx, {})
        assert count == 3  # Three overdue items in sample


# ══════════════════════════════════════════════
# Formatter Tests
# ══════════════════════════════════════════════

class TestFormatter:
    """Tests for response formatting."""

    def test_truncation_rl1_all_inline(self):
        """<=20 items returned as-is."""
        items = [{"row_id": str(i)} for i in range(15)]
        truncated, is_trunc = _apply_truncation(items, 15, "C1")
        assert len(truncated) == 15
        assert is_trunc is False

    def test_truncation_rl2_preview(self):
        """>20 items returns 5 preview."""
        items = [{"row_id": str(i)} for i in range(30)]
        truncated, is_trunc = _apply_truncation(items, 30, "C1")
        assert len(truncated) == CHATBOT_PREVIEW_COUNT
        assert is_trunc is True

    def test_data_references_cite_only_returned(self):
        """data_references built from final results only."""
        items = [
            {"row_id": "1", "document": "DOC1", "source_sheet": "LOT1"},
            {"row_id": "2", "document": "DOC2", "source_sheet": "LOT2"},
        ]
        refs = _build_data_references(items, 2, "C1", False)
        assert len(refs) == 2
        assert refs[0]["row_id"] == "1"
        assert refs[1]["row_id"] == "2"

    def test_data_references_truncated_adds_aggregate(self):
        """Truncated results add ONE aggregate entry."""
        items = [{"row_id": "1", "document": "DOC1", "source_sheet": "LOT1"}]
        refs = _build_data_references(items, 100, "C1", True)
        # 1 per-row + 1 aggregate
        assert len(refs) == 2
        assert refs[-1]["row_id"] is None

    def test_fields_used_matches_constants(self):
        """fields_used in data_references match command constants."""
        items = [{"row_id": "1", "document": "DOC1", "source_sheet": "LOT1"}]
        refs = _build_data_references(items, 1, "C1", False)
        assert refs[0]["fields_used"] == FIELDS_FILTER_ROW

    def test_format_response_schema(self, ctx):
        """Every response has valid schema."""
        response = format_response(
            command_id="C1",
            layer="L2",
            confidence=1.0,
            params={"lot": "LOT 42"},
            results=[{"row_id": "1", "document": "DOC1", "source_sheet": "LOT1",
                       "category": "BLOCKED", "priority_score": 50, "is_overdue": True, "titre": "T"}],
            result_count=1,
            extra={"sources_used": ["M3"], "warnings": []},
        )
        assert isinstance(response, ChatbotResponse)
        assert response.command_id == "C1"
        assert response.classification_layer == "L2"
        assert isinstance(response.results, list)
        assert isinstance(response.warnings, list)
        assert isinstance(response.sources_used, list)
        assert isinstance(response.data_references, list)
        assert response.export_metadata is None

    def test_c12_format(self, ctx):
        """C12 returns results=[], export_metadata populated."""
        response = format_response(
            command_id="C12",
            layer="L2",
            confidence=1.0,
            params={},
            results=[],
            result_count=100,
            extra={"sources_used": ["M3"], "warnings": []},
            export_metadata={"export_path": "/tmp/test.csv", "format": "csv", "row_count": 100},
        )
        assert response.results == []
        assert response.results_truncated is False
        assert response.export_metadata is not None
        assert response.export_metadata["row_count"] == 100


# ══════════════════════════════════════════════
# Exporter Tests
# ══════════════════════════════════════════════

class TestExporter:
    """Tests for CSV exporter."""

    def test_export_creates_file(self, tmp_path):
        items = [{"row_id": "1", "document": "DOC1"}]
        meta = export_to_csv(items, str(tmp_path), "test.csv")
        assert meta["row_count"] == 1
        assert meta["format"] == "csv"
        assert os.path.exists(meta["export_path"])

    def test_export_empty(self, tmp_path):
        meta = export_to_csv([], str(tmp_path), "empty.csv")
        assert meta["row_count"] == 0
        assert os.path.exists(meta["export_path"])


# ══════════════════════════════════════════════
# Dictionary Tests
# ══════════════════════════════════════════════

class TestDictionaries:
    """Tests for dictionary building."""

    def test_normalize_key(self):
        assert _normalize_key("  Résumé  ") == "resume"
        assert _normalize_key("LOT 42") == "lot 42"

    def test_lot_aliases_built(self, sample_queue):
        aliases = build_lot_aliases(sample_queue)
        assert len(aliases) > 0
        # "42" or "plb" should map to LOT 42-PLB-UTB
        assert aliases.get("42") == "LOT 42-PLB-UTB" or aliases.get("plb") == "LOT 42-PLB-UTB"

    def test_approver_aliases_built(self):
        aliases = build_approver_aliases()
        assert aliases.get("socotec") == "SOCOTEC"
        assert aliases.get("terrell") == "BET_STR_TERRELL"
        assert aliases.get("egis") == "BET_EGIS"

    def test_category_aliases_built(self):
        aliases = build_category_aliases()
        assert aliases.get("blocked") == "BLOCKED"
        assert aliases.get("bloque") == "BLOCKED"


# ══════════════════════════════════════════════
# Index Tests
# ══════════════════════════════════════════════

class TestIndexes:
    """Tests for index building."""

    def test_doc_index_multi_match(self, sample_queue):
        """Same document in 2 lots -> list of 2 row_ids."""
        doc_idx = build_doc_index(sample_queue)
        from jansa_visasist.pipeline.m6.indexes import _normalize_doc
        norm = _normalize_doc("P17T2INEXEVDPTERI001MTDTZFD026005")
        assert norm in doc_idx
        assert len(doc_idx[norm]) == 2

    def test_lot_index_sorted(self, sample_queue):
        lot_idx = build_lot_index(sample_queue)
        assert "LOT 42-PLB-UTB" in lot_idx
        row_ids = lot_idx["LOT 42-PLB-UTB"]
        assert len(row_ids) == 2
        # First should be higher priority
        from jansa_visasist.pipeline.m6.indexes import build_queue_index
        qi = build_queue_index(sample_queue)
        assert qi[row_ids[0]]["priority_score"] >= qi[row_ids[1]]["priority_score"]
