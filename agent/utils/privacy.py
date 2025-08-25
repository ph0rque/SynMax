from __future__ import annotations

from typing import Dict

from agent.exec.sql_builder import QueryPlan


def plan_is_aggregate_only(plan: QueryPlan) -> bool:
    has_aggs = bool(plan.aggregations)
    # Consider group_by with aggregations as aggregate-only
    return has_aggs


def redact_preview_rows(rows: int) -> int:
    # Force very small previews when preparing LLM context
    return min(rows, 20)


def allowed_llm_context_summary(schema_columns: int, plan: QueryPlan) -> Dict[str, int | bool]:
    return {
        "schema_columns": schema_columns,
        "has_aggregations": bool(plan.aggregations),
        "num_group_by_cols": len(plan.group_by or []),
        "num_select_exprs": len(plan.select_exprs or {}),
    }
