from __future__ import annotations

import os
from typing import Dict, Any

# Placeholder planner; enforces privacy by only exposing schema/aggregates

class Planner:
    def __init__(self, model: str | None = None):
        self.model = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        # Real implementation would call OpenAI with strict prompts and tools

    def plan(self, question: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        # Return a stub plan structure for now
        return {
            "intent": "deterministic" if any(w in question.lower() for w in ["count", "sum", "avg"]) else "analytic",
            "columns": schema.get("columns", []),
            "filters": [],
            "metrics": [],
            "steps": ["preview", "aggregate_or_analyze"],
        }
