from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from agent.utils.schema_cache import SchemaSnapshot


@dataclass
class Filter:
    column: str
    op: str  # '=', '!=', '>', '>=', '<', '<=', 'IN', 'BETWEEN', 'LIKE'
    value: Any | List[Any]


@dataclass
class QueryPlan:
    columns: List[str]
    filters: List[Filter]
    group_by: List[str]
    aggregations: Dict[str, str]  # output_name -> SQL expression (e.g., 'total_qty': 'SUM(scheduled_quantity)')
    order_by: List[Tuple[str, str]]  # (expr, 'ASC'|'DESC')
    limit: Optional[int] = None


def escape_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_sql(parquet_path: str, plan: QueryPlan, schema: SchemaSnapshot) -> Tuple[str, List[Any]]:
    # Validate columns
    valid_cols = {c.name for c in schema.columns}
    for col in plan.columns + plan.group_by + [f.column for f in plan.filters]:
        if col not in valid_cols:
            raise ValueError(f"Unknown column: {col}")

    select_parts: List[str] = []
    params: List[Any] = [parquet_path]

    # Projections
    if plan.aggregations:
        select_parts.extend([f"{expr} AS {escape_ident(alias)}" for alias, expr in plan.aggregations.items()])
    if plan.columns:
        select_parts.extend([escape_ident(c) for c in plan.columns])

    if not select_parts:
        select_parts = ['*']

    sql = f"SELECT {', '.join(select_parts)} FROM read_parquet(?)"

    # Filters (parameterized where possible)
    where_clauses: List[str] = []
    for f in plan.filters:
        col_sql = escape_ident(f.column)
        if f.op.upper() == 'IN' and isinstance(f.value, list):
            placeholders = ', '.join(['?'] * len(f.value))
            where_clauses.append(f"{col_sql} IN ({placeholders})")
            params.extend(f.value)
        elif f.op.upper() == 'BETWEEN' and isinstance(f.value, list) and len(f.value) == 2:
            where_clauses.append(f"{col_sql} BETWEEN ? AND ?")
            params.extend(f.value)
        else:
            where_clauses.append(f"{col_sql} {f.op} ?")
            params.append(f.value)
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    # Group by
    if plan.group_by:
        sql += " GROUP BY " + ", ".join([escape_ident(g) for g in plan.group_by])

    # Order by
    if plan.order_by:
        parts = [f"{expr} {direction}" for expr, direction in plan.order_by]
        sql += " ORDER BY " + ", ".join(parts)

    # Limit
    if plan.limit is not None:
        sql += " LIMIT ?"
        params.append(plan.limit)

    return sql, params
