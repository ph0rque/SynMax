#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List, Tuple

import duckdb


NUMERIC_TYPES = {"TINYINT","SMALLINT","INTEGER","BIGINT","HUGEINT","REAL","FLOAT","DOUBLE","DECIMAL"}
DATETIME_TYPES = {"DATE","TIMESTAMP","TIMESTAMPTZ","TIME"}
BOOL_TYPES = {"BOOLEAN"}


def escape_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def profile(path: str, sample_rows: int = 100_000, max_time_cols: int = 3) -> Dict[str, Any]:
    con = duckdb.connect(database=':memory:')

    # Schema
    schema_df = con.execute("DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [path]).fetchdf()
    schema: List[Tuple[str, str]] = [(r['column_name'], r['column_type']) for _, r in schema_df.iterrows()]

    # Counts
    row_count = con.execute("SELECT COUNT(*) AS c FROM read_parquet(?)", [path]).fetchone()[0]
    col_count = len(schema)

    # Sample view
    # DuckDB cannot prepare parameter in CREATE VIEW; inline the sanitized path
    path_sql = path.replace("'", "''")
    con.execute(
        f"CREATE TEMP VIEW sample AS SELECT * FROM read_parquet('{path_sql}') USING SAMPLE {sample_rows} ROWS"
    )
    sample_rows_actual = con.execute("SELECT COUNT(*) FROM sample").fetchone()[0]

    summary: Dict[str, Any] = {
        'path': os.path.abspath(path),
        'rows': int(row_count),
        'columns': int(col_count),
        'sample_rows': int(sample_rows_actual),
        'schema': [{'name': n, 'type': t} for n,t in schema],
        'columns_summary': {}
    }

    # Identify datetime columns (limit)
    time_cols = [n for n,t in schema if any(tt in t.upper() for tt in DATETIME_TYPES)][:max_time_cols]

    # Per-column sample stats
    for name, ctype in schema:
        t_up = ctype.upper()
        qn = escape_ident(name)
        info: Dict[str, Any] = {'type': ctype}

        # Nulls (sample)
        nulls = con.execute(f"SELECT SUM(CASE WHEN {qn} IS NULL THEN 1 ELSE 0 END) FROM sample").fetchone()[0]
        info['sample_nulls'] = int(nulls or 0)
        info['sample_null_rate'] = (nulls or 0) / sample_rows_actual if sample_rows_actual else None

        if any(nt in t_up for nt in NUMERIC_TYPES):
            min_, p50, p95, max_, avg_ = con.execute(
                f"SELECT min({qn}), quantile_cont({qn}, 0.5), quantile_cont({qn}, 0.95), max({qn}), avg({qn}) FROM sample"
            ).fetchone()
            info['sample_min'] = float(min_) if min_ is not None else None
            info['sample_p50'] = float(p50) if p50 is not None else None
            info['sample_p95'] = float(p95) if p95 is not None else None
            info['sample_max'] = float(max_) if max_ is not None else None
            info['sample_mean'] = float(avg_) if avg_ is not None else None
        elif any(tt in t_up for tt in DATETIME_TYPES):
            min_, max_ = con.execute(
                f"SELECT min({qn}), max({qn}) FROM sample"
            ).fetchone()
            info['sample_min'] = None if min_ is None else str(min_)
            info['sample_max'] = None if max_ is None else str(max_)
        elif any(bt in t_up for bt in BOOL_TYPES):
            counts = con.execute(
                f"SELECT {qn} AS v, COUNT(*) AS c FROM sample GROUP BY 1 ORDER BY c DESC"
            ).fetchall()
            info['sample_value_counts'] = [{'value': None if v is None else str(v), 'count': int(c)} for v,c in counts]
        else:
            topk = con.execute(
                f"SELECT {qn} AS v, COUNT(*) AS c FROM sample GROUP BY 1 ORDER BY c DESC NULLS LAST LIMIT 5"
            ).fetchall()
            info['top_categories'] = [{'value': None if v is None else str(v)[:200], 'count': int(c)} for v,c in topk]

        summary['columns_summary'][name] = info

    # Full-range min/max for time columns
    full_times: Dict[str, Any] = {}
    for name in time_cols:
        qn = escape_ident(name)
        mn, mx = con.execute(
            f"SELECT min({qn}), max({qn}) FROM read_parquet(?)", [path]
        ).fetchone()
        full_times[name] = {'min': None if mn is None else str(mn), 'max': None if mx is None else str(mx)}
    summary['time_columns_full_range'] = full_times

    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--path', default='data/pipeline_data.parquet')
    ap.add_argument('--sample-rows', type=int, default=100_000)
    ap.add_argument('--max-time-cols', type=int, default=3)
    args = ap.parse_args()

    out = profile(args.path, args.sample_rows, args.max_time_cols)

    os.makedirs('runs', exist_ok=True)
    stamp = time.strftime('%Y%m%d-%H%M%S')
    out_path = os.path.join('runs', f'profile-{stamp}.json')
    with open(out_path, 'w') as f:
        json.dump(out, f, indent=2)

    # concise print
    first_cols = [c['name'] for c in out['schema'][:5]]
    ex_topcats = {}
    for k, v in list(out['columns_summary'].items())[:3]:
        if 'top_categories' in v:
            ex_topcats[k] = v['top_categories']
    print(json.dumps({
        'saved': out_path,
        'rows': out['rows'],
        'columns': out['columns'],
        'first_columns': first_cols,
        'time_columns_full_range': out.get('time_columns_full_range', {}),
        'example_top_categories': ex_topcats
    }, indent=2))


if __name__ == '__main__':
    main()
