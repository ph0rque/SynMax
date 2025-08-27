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
    special: Optional[Dict[str, Any]] = None  # extra directives for analytics
    suggestions: Optional[List[str]] = None   # guidance for ambiguous/unknown cases


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


def _suggest_columns(schema: SchemaSnapshot, token: str, limit: int = 5) -> List[str]:
    names = [c.name for c in schema.columns]
    token_l = token.lower()
    # simple contains match, then prefix
    contains = [n for n in names if token_l in n.lower()]
    if contains:
        return contains[:limit]
    prefixes = [n for n in names if n.lower().startswith(token_l)]
    if prefixes:
        return prefixes[:limit]
    # fuzzy fallback
    try:
        import difflib
        matches = difflib.get_close_matches(token, names, n=limit, cutoff=0.6)
        return matches
    except Exception:
        return []


def _parse_filters(ql: str, schema: SchemaSnapshot) -> List[Filter]:
    filters: List[Filter] = []
    # year filter: "in 2024" or "year 2024"
    m = re.search(r"\b(20\d{2})\b", ql)
    if m and _find_column(schema, 'eff_gas_day'):
        filters.append(Filter(column=_find_column(schema, 'eff_gas_day'), op='BETWEEN', value=[f"{m.group(1)}-01-01", f"{m.group(1)}-12-31"]))
    # state filter: "in TX" or "state TX"
    m = re.search(r"\bstate\s+([A-Z]{2})\b|\bin\s+([A-Z]{2})\b", ql)
    state = m.group(1) if m and m.group(1) else (m.group(2) if m else None)
    if state and _find_column(schema, 'state_abb'):
        filters.append(Filter(column=_find_column(schema, 'state_abb'), op='=', value=state))
    # receipts/deliveries filter
    if 'receipts' in ql and _find_column(schema, 'rec_del_sign'):
        filters.append(Filter(column=_find_column(schema, 'rec_del_sign'), op='=', value=-1))
    if 'deliveries' in ql and _find_column(schema, 'rec_del_sign'):
        filters.append(Filter(column=_find_column(schema, 'rec_del_sign'), op='=', value=1))
    # Generic IN filter: "<col> in (A, B, C)" or "<col> in A,B"
    for mm in re.finditer(r"\b([a-zA-Z0-9_]+)\s+in\s*\(([^\)]+)\)", ql):
        token = mm.group(1)
        raw = mm.group(2)
        col = _find_column(schema, token)
        if not col:
            continue
        vals = [v.strip().strip("'\"") for v in raw.split(',') if v.strip()]
        if vals:
            filters.append(Filter(column=col, op='IN', value=vals))
    for mm in re.finditer(r"\b([a-zA-Z0-9_]+)\s+in\s+([A-Za-z0-9_,\- ]+)", ql):
        token = mm.group(1)
        raw = mm.group(2)
        col = _find_column(schema, token)
        if not col:
            continue
        vals = [v.strip().strip("'\"") for v in raw.split(',') if v.strip()]
        if vals:
            filters.append(Filter(column=col, op='IN', value=vals))
    # BETWEEN dates: "col between YYYY-MM-DD and YYYY-MM-DD"
    m = re.search(r"\b([a-zA-Z0-9_]+)\s+between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})", ql)
    if m:
        col = _find_column(schema, m.group(1))
        if col:
            filters.append(Filter(column=col, op='BETWEEN', value=[m.group(2), m.group(3)]))
    # Equality filters: "col=value" simple tokens
    for mm in re.finditer(r"\b([a-zA-Z0-9_]+)\s*=\s*([A-Za-z0-9_\-]+)\b", ql):
        col = _find_column(schema, mm.group(1))
        if col:
            filters.append(Filter(column=col, op='=', value=mm.group(2)))
    return filters


def parse_simple(question: str, schema: SchemaSnapshot) -> "ParseResult":
    q = question.strip()
    ql = q.lower()

    # Anomalies vs category intent
    if "anomal" in ql and ("category" in ql or "categories" in ql):
        # extract z-threshold and min days if present
        m = re.search(r"z\s*=\s*([0-9]+(?:\.[0-9]+)?)", ql)
        z = float(m.group(1)) if m else 3.0
        m = re.search(r"min[_\s-]*days\s*=\s*(\d+)", ql)
        min_days = int(m.group(1)) if m else 3
        # optional state/year/receipts-deliveries handled later in CLI
        return ParseResult(plan=None, intent="analytic", notes="anomalies vs category", special={"type": "anomalies_vs_category", "z": z, "min_days": min_days})

    # Trends intent
    if "trend" in ql or "trends" in ql:
        by = "month" if ("month" in ql or "by month" in ql) else ("day" if ("day" in ql or "by day" in ql) else "month")
        return ParseResult(plan=None, intent="analytic", notes="trends summary", special={"type": "trends", "by": by})

    # COUNT rows
    if re.search(r"\bcount\b", ql) and not re.search(r"distinct", ql):
        return ParseResult(
            intent="deterministic",
            plan=QueryPlan(columns=[], filters=_parse_filters(ql, schema), group_by=[], aggregations={"row_count": "COUNT(*)"}, order_by=[], limit=None),
            notes="count rows"
        )

    # DISTINCT count of column
    m = re.search(r"distinct\s+([a-zA-Z0-9_]+)", ql)
    if m:
        token = m.group(1)
        col = _find_column(schema, token)
        if col:
            return ParseResult(
                intent="deterministic",
                plan=QueryPlan(columns=[], filters=_parse_filters(ql, schema), group_by=[], aggregations={"distinct_count": f"COUNT(DISTINCT {col})"}, order_by=[], limit=None),
                notes=f"distinct count of {col}"
            )
        else:
            return ParseResult(plan=None, intent="unknown", notes="unknown column for distinct", suggestions=_suggest_columns(schema, token))

    # TOTAL scheduled_quantity (optionally by <col> or time buckets, supports multi-dim group-by)
    if "scheduled_quantity" in ql and ("sum" in ql or "total" in ql):
        dims: List[str] = []
        m = re.search(r"by\s+([a-zA-Z0-9_,\s]+)", ql)
        if m:
            tokens = [t.strip() for t in m.group(1).split(',') if t.strip()]
            for tok in tokens:
                col = _find_column(schema, tok)
                if col:
                    dims.append(col)
        aggs = {"total_scheduled_quantity": "SUM(scheduled_quantity)"}
        group_by: List[str] = dims.copy()
        select_exprs: Dict[str, str] = {}
        group_by_exprs: List[str] = []
        if _find_column(schema, 'eff_gas_day'):
            if 'by month' in ql:
                select_exprs['month'] = "date_trunc('month', eff_gas_day)"
                group_by_exprs.append("date_trunc('month', eff_gas_day)")
            if 'by quarter' in ql:
                select_exprs['quarter'] = "date_trunc('quarter', eff_gas_day)"
                group_by_exprs.append("date_trunc('quarter', eff_gas_day)")
            if 'by week' in ql:
                select_exprs['week'] = "date_trunc('week', eff_gas_day)"
                group_by_exprs.append("date_trunc('week', eff_gas_day)")
            if 'by year' in ql:
                select_exprs['year'] = "date_trunc('year', eff_gas_day)"
                group_by_exprs.append("date_trunc('year', eff_gas_day)")
        order_by: List[Tuple[str,str]] = [("total_scheduled_quantity", "DESC")] if (group_by or group_by_exprs) else []
        return ParseResult(
            intent="deterministic",
            plan=QueryPlan(columns=[], filters=_parse_filters(ql, schema), group_by=group_by, aggregations=aggs, order_by=order_by, limit=10 if group_by else None, select_exprs=select_exprs or None, group_by_exprs=group_by_exprs or None),
            notes=("sum scheduled_quantity by " + ", ".join(group_by) if group_by else ("sum scheduled_quantity by time bucket" if group_by_exprs else "sum scheduled_quantity"))
        )

    # TOP N <col> by scheduled_quantity
    m = re.search(r"top\s+(\d+)\s+([a-zA-Z0-9_]+).*by.*scheduled_quantity", ql)
    if m:
        n = int(m.group(1))
        token = m.group(2)
        col = _find_column(schema, token)
        if col:
            return ParseResult(
                intent="deterministic",
                plan=QueryPlan(
                    columns=[col],
                    filters=_parse_filters(ql, schema),
                    group_by=[col],
                    aggregations={"total_scheduled_quantity": "SUM(scheduled_quantity)"},
                    order_by=[("total_scheduled_quantity", "DESC")],
                    limit=n,
                ),
                notes=f"top {n} {col} by total scheduled_quantity",
            )
        else:
            return ParseResult(plan=None, intent="unknown", notes="unknown column for top-n", suggestions=_suggest_columns(schema, token))

    # GROUP BY <col>[, <col>...] totals
    m = re.search(r"total.*by\s+([a-zA-Z0-9_,\s]+)", ql)
    if m:
        tokens = [t.strip() for t in m.group(1).split(',') if t.strip()]
        cols = [ _find_column(schema, t) for t in tokens ]
        cols = [c for c in cols if c]
        if cols:
            return ParseResult(
                intent="deterministic",
                plan=QueryPlan(
                    columns=cols,
                    filters=_parse_filters(ql, schema),
                    group_by=cols,
                    aggregations={"total_scheduled_quantity": "SUM(scheduled_quantity)"},
                    order_by=[("total_scheduled_quantity", "DESC")],
                    limit=None,
                ),
                notes=f"totals by {', '.join(cols)}"
            )
        else:
            # Suggest for the first unknown token
            unk = tokens[0] if tokens else ""
            return ParseResult(plan=None, intent="unknown", notes="unknown column for totals", suggestions=_suggest_columns(schema, unk))

    return ParseResult(plan=None, intent="unknown", notes="no simple parse")
