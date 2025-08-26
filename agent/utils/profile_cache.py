from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from agent.exec.duck import DuckDBExecutor


@dataclass
class ColumnProfile:
    null_rate: float
    approx_distinct: int


class ProfileCache:
    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, int], Dict[str, ColumnProfile]] = {}

    def get_or_profile(self, executor: DuckDBExecutor, parquet_path: str, sample_rows: int = 10000) -> Dict[str, ColumnProfile]:
        key = (parquet_path, sample_rows)
        if key in self._cache:
            return self._cache[key]
        # Build sample view
        executor.query("CREATE OR REPLACE TEMP VIEW sample AS SELECT * FROM read_parquet(?) USING SAMPLE {} ROWS".format(sample_rows), [parquet_path])
        # Get schema from sample (column names)
        schema_tbl = executor.query("DESCRIBE SELECT * FROM sample LIMIT 0")
        schema_df = schema_tbl.to_pandas()
        columns: List[str] = schema_df["column_name"].tolist() if "column_name" in schema_df.columns else []
        # Approx distinct counts in one pass over full data
        if columns:
            exprs = ", ".join([f"approx_count_distinct(\"{c.replace('\\', '\\\\').replace('"', '""')}\") AS c{i}" for i, c in enumerate(columns)])
            row = executor.query(f"SELECT {exprs} FROM read_parquet(?)", [parquet_path]).fetchone()
        else:
            row = []
        # Null rates from sample
        profile: Dict[str, ColumnProfile] = {}
        for idx, col in enumerate(columns):
            qn = '"' + col.replace('"', '""') + '"'
            nulls = executor.query(f"SELECT SUM(CASE WHEN {qn} IS NULL THEN 1 ELSE 0 END) FROM sample").fetchone()[0]
            # sample size of view may be < sample_rows; compute actual count
            sample_ct = executor.query("SELECT COUNT(*) FROM sample").fetchone()[0]
            null_rate = float((nulls or 0) / sample_ct) if sample_ct else 0.0
            approx_distinct = int(row[idx] or 0) if idx < len(row) else 0
            profile[col] = ColumnProfile(null_rate=null_rate, approx_distinct=approx_distinct)
        self._cache[key] = profile
        return profile

    def summarize(self, parquet_path: str, sample_rows: int) -> Dict[str, Any]:
        prof = self._cache.get((parquet_path, sample_rows), {})
        return {
            "sample_rows": sample_rows,
            "columns_profiled": len(prof),
        }
