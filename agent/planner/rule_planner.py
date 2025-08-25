from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

from agent.exec.sql_builder import Filter, QueryPlan
from agent.utils.schema_cache import SchemaSnapshot


@dataclass
class ParseResult:
    plan: Optional[QueryPlan]
    intent: str  # 'deterministic' | 'analytic' | 'unknown'
    notes: str = ""


def _find_column(schema: SchemaSnapshot, name: str) -> Optional[str]:
    # best-effort match by case-insensitive exact or prefix
    names = [c.name for c in schema.columns]
    lc = {n.lower(): n for n in names}
    if name.lower() in lc:
        return lc[name.lower()]
    # simple prefix guess
    for n in names:
        if n.lower().startswith(name.lower()):
            return n
    return None


def parse_simple(question: str, schema: SchemaSnapshot) -> ParseResult:
    q = question.strip()
    ql = q.lower()

    # COUNT rows
    if re.search(r"\bcount\b", ql) and not re.search(r"distinct", ql):
        return ParseResult(
            intent="deterministic",
            plan=QueryPlan(columns=[], filters=[], group_by=[], aggregations={"row_count": "COUNT(*)"}, order_by=[], limit=None),
            notes="count rows"
        )

    # DISTINCT count of column
    m = re.search(r"distinct\s+([a-zA-Z0-9_]+)", ql)
    if m:
        col = _find_column(schema, m.group(1))
        if col:
            return ParseResult(
                intent="deterministic",
                plan=QueryPlan(columns=[], filters=[], group_by=[], aggregations={"distinct_count": f"COUNT(DISTINCT {col})"}, order_by=[], limit=None),
                notes=f"distinct count of {col}"
            )

    # TOTAL scheduled_quantity (optionally by <col>)
    if "scheduled_quantity" in ql and ("sum" in ql or "total" in ql):
        by = None
        m = re.search(r"by\s+([a-zA-Z0-9_]+)", ql)
        if m:
            by = _find_column(schema, m.group(1))
        aggs = {"total_scheduled_quantity": "SUM(scheduled_quantity)"}
        group_by: List[str] = [by] if by else []
        order_by: List[Tuple[str,str]] = [("total_scheduled_quantity", "DESC")] if by else []
        return ParseResult(
            intent="deterministic",
            plan=QueryPlan(columns=[], filters=[], group_by=group_by, aggregations=aggs, order_by=order_by, limit=10 if by else None),
            notes=f"sum scheduled_quantity by {by}" if by else "sum scheduled_quantity"
        )

    # TOP N <col> by scheduled_quantity
    m = re.search(r"top\s+(\d+)\s+([a-zA-Z0-9_]+).*by.*scheduled_quantity", ql)
    if m:
        n = int(m.group(1))
        col = _find_column(schema, m.group(2))
        if col:
            return ParseResult(
                intent="deterministic",
                plan=QueryPlan(
                    columns=[col],
                    filters=[],
                    group_by=[col],
                    aggregations={"total_scheduled_quantity": "SUM(scheduled_quantity)"},
                    order_by=[("total_scheduled_quantity", "DESC")],
                    limit=n,
                ),
                notes=f"top {n} {col} by total scheduled_quantity"
            )

    # GROUP BY <col> totals
    m = re.search(r"total.*by\s+([a-zA-Z0-9_]+)", ql)
    if m:
        col = _find_column(schema, m.group(1))
        if col:
            return ParseResult(
                intent="deterministic",
                plan=QueryPlan(
                    columns=[col],
                    filters=[],
                    group_by=[col],
                    aggregations={"total_scheduled_quantity": "SUM(scheduled_quantity)"},
                    order_by=[("total_scheduled_quantity", "DESC")],
                    limit=None,
                ),
                notes=f"totals by {col}"
            )

    return ParseResult(plan=None, intent="unknown", notes="no simple parse")
