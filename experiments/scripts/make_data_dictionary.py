#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import duckdb
from typing import Any, Dict, List, Tuple

NUMERIC_TYPES = {"TINYINT","SMALLINT","INTEGER","BIGINT","HUGEINT","REAL","FLOAT","DOUBLE","DECIMAL"}
DATETIME_TYPES = {"DATE","TIMESTAMP","TIMESTAMPTZ","TIME"}
BOOL_TYPES = {"BOOLEAN"}


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def infer_schema(con: duckdb.DuckDBPyConnection, path: str) -> List[Tuple[str,str]]:
    df = con.execute("DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [path]).fetchdf()
    return [(r['column_name'], r['column_type']) for _, r in df.iterrows()]


def compute_counts(con, path: str) -> int:
    return con.execute("SELECT COUNT(*) FROM read_parquet(?)", [path]).fetchone()[0]


def build_sample(con, path: str, sample_rows: int) -> int:
    path_sql = path.replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP VIEW sample AS SELECT * FROM read_parquet('{path_sql}') USING SAMPLE {sample_rows} ROWS")
    return con.execute("SELECT COUNT(*) FROM sample").fetchone()[0]


def approx_cardinalities(con, path: str, cols: List[str]) -> Dict[str,int]:
    # one pass query for all columns
    exprs = ", ".join([f"approx_count_distinct({qident(c)}) AS c{i}" for i,c in enumerate(cols)])
    row = con.execute(f"SELECT {exprs} FROM read_parquet(?)", [path]).fetchone()
    return {cols[i]: int(row[i] or 0) for i in range(len(cols))}


def numeric_summary(con, col: str) -> Dict[str,Any]:
    qn = qident(col)
    mn, q1, med, q3, p95, mx, avg, std = con.execute(
        f"SELECT min({qn}), quantile_cont({qn},0.25), median({qn}), quantile_cont({qn},0.75), quantile_cont({qn},0.95), max({qn}), avg({qn}), stddev_pop({qn}) FROM sample"
    ).fetchone()
    return {
        'min': None if mn is None else float(mn),
        'q1': None if q1 is None else float(q1),
        'median': None if med is None else float(med),
        'q3': None if q3 is None else float(q3),
        'p95': None if p95 is None else float(p95),
        'max': None if mx is None else float(mx),
        'mean': None if avg is None else float(avg),
        'stddev': None if std is None else float(std),
    }


def value_counts(con, col: str, limit: int = 10) -> List[Tuple[str,int]]:
    qn = qident(col)
    rows = con.execute(
        f"SELECT {qn} AS v, COUNT(*) AS c FROM sample GROUP BY 1 ORDER BY c DESC NULLS LAST LIMIT {limit}"
    ).fetchall()
    def s(v):
        return None if v is None else (str(v)[:200])
    return [(s(v), int(c)) for v,c in rows]


def null_rate(con, col: str, sample_rows: int) -> float:
    qn = qident(col)
    n = con.execute(f"SELECT SUM(CASE WHEN {qn} IS NULL THEN 1 ELSE 0 END) FROM sample").fetchone()[0]
    return (n or 0) / sample_rows if sample_rows else 0.0


def time_full_range(con, path: str, cols: List[str]) -> Dict[str, Dict[str,str]]:
    res: Dict[str, Dict[str,str]] = {}
    for c in cols:
        qn = qident(c)
        mn, mx = con.execute(f"SELECT min({qn}), max({qn}) FROM read_parquet(?)", [path]).fetchone()
        res[c] = {'min': None if mn is None else str(mn), 'max': None if mx is None else str(mx)}
    return res


def make_markdown(path: str, rows: int, schema: List[Tuple[str,str]], completeness: Dict[str,float], card: Dict[str,int], num_stats: Dict[str,Dict[str,Any]], topcats: Dict[str,List[Tuple[str,int]]], timerange: Dict[str,Dict[str,str]]) -> str:
    lines: List[str] = []
    fname = os.path.basename(path)
    lines.append(f"# Data Dictionary — {fname}")
    lines.append("")
    # Prose summary
    lines.append("## Summary")
    lines.append(f"- Rows: {rows}")
    lines.append(f"- Columns: {len(schema)}")
    if timerange:
        for c, rng in timerange.items():
            lines.append(f"- Time coverage ({c}): {rng.get('min')} → {rng.get('max')}")
    # Notable completeness
    hi_null = [c for c,rate in completeness.items() if rate < 0.8]
    if hi_null:
        lines.append(f"- Columns with <80% completeness (sample): {', '.join(hi_null)}")
    # Top categories quick view
    if topcats:
        lines.append("- Example top categories (sample):")
        for c, vals in list(topcats.items())[:3]:
            ex = ", ".join([f"{v} ({cnt})" for v,cnt in vals[:5]]) if vals else "-"
            lines.append(f"  - {c}: {ex}")
    lines.append("")

    # Table header
    lines.append("## Columns")
    lines.append("| Name | Type | Completeness (sample) | Distinct (approx) | Notes |")
    lines.append("|---|---|---:|---:|---|")
    for name, ctype in schema:
        comp = completeness.get(name, 1.0)
        comp_pct = f"{comp*100:.1f}%"
        dc = card.get(name, 0)
        note = ""
        if name in num_stats:
            s = num_stats[name]
            note = f"min={s['min']}, p50={s['median']}, p95={s['p95']}, max={s['max']}"
        elif name in topcats:
            vals = topcats[name][:3]
            note = "top: " + ", ".join([str(v) for v,_ in vals])
        lines.append(f"| {name} | {ctype} | {comp_pct} | {dc} | {note} |")

    # Details per numeric
    if num_stats:
        lines.append("")
        lines.append("## Numeric details (sample)")
        for name, s in num_stats.items():
            lines.append(f"- **{name}**: min={s['min']}, q1={s['q1']}, median={s['median']}, q3={s['q3']}, p95={s['p95']}, max={s['max']}, mean={s['mean']}, std={s['stddev']}")

    # Details per categorical
    if topcats:
        lines.append("")
        lines.append("## Top categories (sample)")
        for name, vals in topcats.items():
            nicer = ", ".join([f"{v} ({cnt})" for v,cnt in vals]) if vals else "-"
            lines.append(f"- **{name}**: {nicer}")

    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--path', default='data/pipeline_data.parquet')
    ap.add_argument('--sample-rows', type=int, default=100_000)
    ap.add_argument('--out', default='docs/data_dictionary.md')
    args = ap.parse_args()

    path = os.path.abspath(args.path)
    con = duckdb.connect(database=':memory:')

    schema = infer_schema(con, path)
    rows = compute_counts(con, path)
    sample_rows = build_sample(con, path, args.sample_rows)

    cols = [n for n,_ in schema]

    # Completeness from sample
    completeness = {c: 1.0 - null_rate(con, c, sample_rows) for c in cols}

    # Approx distinct (full data, single pass)
    try:
        card = approx_cardinalities(con, path, cols)
    except Exception:
        card = {c: 0 for c in cols}

    # Numeric stats and top categories
    num_stats: Dict[str,Dict[str,Any]] = {}
    topcats: Dict[str,List[Tuple[str,int]]] = {}
    dt_cols: List[str] = []
    for name, ctype in schema:
        t_up = ctype.upper()
        if any(nt in t_up for nt in NUMERIC_TYPES):
            num_stats[name] = numeric_summary(con, name)
        elif any(tt in t_up for tt in DATETIME_TYPES):
            dt_cols.append(name)
        elif any(bt in t_up for bt in BOOL_TYPES):
            topcats[name] = value_counts(con, name, 10)
        else:
            topcats[name] = value_counts(con, name, 10)

    # Full range for datetime
    timerange = time_full_range(con, path, dt_cols)

    md = make_markdown(path, rows, schema, completeness, card, num_stats, topcats, timerange)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w') as f:
        f.write(md)

    print(f"Wrote {args.out}")


# Helpers used above

def null_rate(con, col: str, sample_rows: int) -> float:
    qn = qident(col)
    n = con.execute(f"SELECT SUM(CASE WHEN {qn} IS NULL THEN 1 ELSE 0 END) FROM sample").fetchone()[0]
    return (n or 0) / sample_rows if sample_rows else 0.0


if __name__ == '__main__':
    main()
