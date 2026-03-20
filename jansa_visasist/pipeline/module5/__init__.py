"""
Module 5: Suggestion Engine — JANSA VISASIST Pipeline.

Tier 3 in the pipeline orchestration [V2.2.2 E1 §2].
Consumes M3 (priority queue) and M4 (per-item analysis) outputs.
Produces deterministic, actionable recommendations for MOEX operators.

GP10 guarantee: Fully functional without any AI/LLM component.
"""

from .runner import run_module5

__all__ = ["run_module5"]
