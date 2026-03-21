"""
Module 6 — Integration Tests with real M3 pipeline data.

Skip if data/GrandFichier_1.xlsx not present.
Runs M1->M2->M3 pipeline, then tests M6 chatbot queries.
"""

import json
import os
import pytest

from jansa_visasist.pipeline.m6.formatter import ChatbotResponse


# Skip entire module if no test data or no M3 output
M3_OUTPUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "output", "m3", "m3_priority_queue.json",
)
GRAND_FICHIER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data", "GrandFichier_1.xlsx",
)

# We use the pre-computed M3 output if it exists
pytestmark = pytest.mark.skipif(
    not os.path.exists(M3_OUTPUT),
    reason="M3 output not available (output/m3/m3_priority_queue.json)",
)


@pytest.fixture(scope="module")
def m6_ctx():
    """Initialize M6 context from real M3 data."""
    from jansa_visasist.main_m6 import init_chatbot
    return init_chatbot(M3_OUTPUT)


@pytest.fixture(scope="module")
def query_fn(m6_ctx):
    """Return a helper to process queries."""
    from jansa_visasist.main_m6 import process_query

    def _query(q):
        return process_query(m6_ctx, q)
    return _query


def _validate_schema(response: ChatbotResponse):
    """Validate that response has all required fields with correct types."""
    assert isinstance(response.command_id, str)
    assert response.command_id in (
        "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9",
        "C10", "C11", "C12", "REJECTED",
    )
    assert isinstance(response.classification_layer, str)
    assert response.classification_layer in ("L1", "L2", "L3")
    assert response.classification_confidence is None or isinstance(response.classification_confidence, (int, float))
    assert isinstance(response.parameters, dict)
    assert isinstance(response.result_count, int)
    assert isinstance(response.results, list)
    assert isinstance(response.results_truncated, bool)
    assert isinstance(response.response_text, str)
    assert isinstance(response.sources_used, list)
    assert isinstance(response.data_references, list)
    assert response.export_metadata is None or isinstance(response.export_metadata, dict)
    assert isinstance(response.warnings, list)


class TestM6Integration:
    """Integration tests using real M3 pipeline output."""

    def test_lot_query(self, query_fn, m6_ctx):
        """'lot 42' or similar returns items from the correct lot."""
        # Find a lot that exists in the data
        lots = list(m6_ctx.lot_index.keys())
        if not lots:
            pytest.skip("No lots in data")

        # Try to find one with a number
        target_lot = lots[0]
        for lot in lots:
            if "42" in lot:
                target_lot = lot
                break

        # Query using the lot name
        resp = query_fn(target_lot)
        _validate_schema(resp)
        assert resp.command_id in ("C1", "C5")

    def test_blocked_category(self, query_fn):
        """'bloques' returns items with category BLOCKED (may be 0)."""
        resp = query_fn("bloques")
        _validate_schema(resp)
        assert resp.command_id == "C2"
        # All results should be BLOCKED category
        for item in resp.results:
            assert item.get("category") == "BLOCKED"

    def test_overdue(self, query_fn):
        """'en retard' returns overdue items."""
        resp = query_fn("en retard")
        _validate_schema(resp)
        assert resp.command_id == "C4"
        assert resp.result_count > 0

    def test_top_5(self, query_fn):
        """'top 5' returns at most 5 items (fewer if queue is smaller)."""
        resp = query_fn("top 5")
        _validate_schema(resp)
        assert resp.command_id == "C11"
        assert len(resp.results) <= 5
        assert len(resp.results) == min(5, resp.result_count)

    def test_count_overdue(self, query_fn):
        """'combien en retard' returns count."""
        resp = query_fn("combien en retard")
        _validate_schema(resp)
        assert resp.command_id == "C10"
        assert resp.results == []
        assert resp.result_count > 0

    def test_summary_lot(self, query_fn, m6_ctx):
        """'resume lot X' returns summary for lot."""
        lots = list(m6_ctx.lot_index.keys())
        if not lots:
            pytest.skip("No lots")

        # Find a lot with "03" in it
        target = lots[0]
        for lot in lots:
            if "03" in lot:
                target = lot
                break

        resp = query_fn(f"resume {target}")
        _validate_schema(resp)
        assert resp.command_id == "C8"
        assert resp.result_count >= 1

    def test_rejected_gibberish(self, query_fn):
        """Gibberish returns REJECTED."""
        resp = query_fn("xyzzy qwerty foobar")
        _validate_schema(resp)
        assert resp.command_id == "REJECTED"

    def test_every_response_has_valid_schema(self, query_fn):
        """Multiple queries all produce valid schema."""
        queries = [
            "lot 42", "bloques", "en retard", "top 5",
            "combien en retard", "xyzzy", "socotec",
        ]
        for q in queries:
            resp = query_fn(q)
            _validate_schema(resp)


class TestM6SchemaValidation:
    """Verify schema compliance for all response types."""

    def test_sources_used_present(self, query_fn):
        resp = query_fn("en retard")
        assert "M3" in resp.sources_used

    def test_data_references_present(self, query_fn):
        resp = query_fn("top 5")
        assert len(resp.data_references) > 0

    def test_truncation_for_large_results(self, query_fn):
        """If result_count > 20, results should be truncated."""
        resp = query_fn("en retard")
        if resp.result_count > 20:
            assert resp.results_truncated is True
            assert len(resp.results) == 5
        else:
            assert resp.results_truncated is False
