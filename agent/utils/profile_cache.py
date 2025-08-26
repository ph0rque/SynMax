from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from agent.exec.duck import DuckDBExecutor


@dataclass
class ColumnProfile:
    null_rate: float
    approx_distinct: int


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _first_scalar(obj: Any) -> Any:
    try:
        import pyarrow as pa  # type: ignore
        if isinstance(obj, pa.Table):
            df = obj.to_pandas()
            return df.iloc[0, 0]
    except Exception:
        pass
    try:
        import pandas as pd  # type: ignore
        if isinstance(obj, pd.DataFrame):
            return obj.iloc[0, 0]
    except Exception:
        pass
    try:
        return obj[0][0]
    except Exception:
        return None


class ProfileCache:
    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, int], Dict[str, ColumnProfile]] = {}

    def get_or_profile(self, executor: DuckDBExecutor, parquet_path: str, sample_rows: int = 10000) -> Dict[str, ColumnProfile]:
        key = (parquet_path, sample_rows)
        if key in self._cache:
            return self._cache[key]
        # Build sample view (inline path to avoid prepared param limitation)
        path_sql = parquet_path.replace("'", "''")
        executor.query(f"CREATE OR REPLACE TEMP VIEW sample AS SELECT * FROM read_parquet('{path_sql}') USING SAMPLE {sample_rows} ROWS")
        # Get schema from sample (column names)
        schema_tbl = executor.query("DESCRIBE SELECT * FROM sample LIMIT 0")
        schema_df = schema_tbl.to_pandas()
        columns: List[str] = schema_df["column_name"].tolist() if "column_name" in schema_df.columns else []
        # Approx distinct counts in one pass over full data
        if columns:
            exprs = ", ".join([f"approx_count_distinct({_quote_ident(c)}) AS c{i}" for i, c in enumerate(columns)])
            row_tbl = executor.query(f"SELECT {exprs} FROM read_parquet(?)", [parquet_path])
            row_df = row_tbl.to_pandas()
            row_vals = row_df.iloc[0].tolist()
        else:
            row_vals = []
        # Null rates from sample
        profile: Dict[str, ColumnProfile] = {}
        sample_ct = int(_first_scalar(executor.query("SELECT COUNT(*) FROM sample")) or 0)
        for idx, col in enumerate(columns):
            qn = _quote_ident(col)
            nulls_val = int(_first_scalar(executor.query(f"SELECT SUM(CASE WHEN {qn} IS NULL THEN 1 ELSE 0 END) FROM sample")) or 0)
            null_rate = float((nulls_val or 0) / sample_ct) if sample_ct else 0.0
            approx_distinct = int(row_vals[idx] or 0) if idx < len(row_vals) else 0
            profile[col] = ColumnProfile(null_rate=null_rate, approx_distinct=approx_distinct)
        self._cache[key] = profile
        return profile

    def summarize(self, parquet_path: str, sample_rows: int) -> Dict[str, Any]:
        prof = self._cache.get((parquet_path, sample_rows), {})
        return {
            "sample_rows": sample_rows,
            "columns_profiled": len(prof),
        }
