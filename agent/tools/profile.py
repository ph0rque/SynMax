from __future__ import annotations

from typing import Dict, Any, List, Optional

from agent.exec.duck import DuckDBExecutor


def profile_dataset(executor: DuckDBExecutor, parquet_path: str, sample_rows: int = 1000) -> Dict[str, Any]:
    # Schema (names and types) via DuckDB's DESCRIBE
    schema = executor.query("DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [parquet_path])
    schema_df = schema.to_pandas()
    cols = [c for c in schema_df["column_name"].tolist() if c and c != ""] if "column_name" in schema_df.columns else []

    # Null counts and basic stats for numeric columns (sampled)
    stats: Dict[str, Any] = {}
    if cols:
        # Use sampling for speed
        sample_tbl = executor.query("SELECT * FROM read_parquet(?) USING SAMPLE {rows} ROWS".format(rows=sample_rows), [parquet_path]).to_pandas()
        for col in cols:
            s = sample_tbl[col] if col in sample_tbl.columns else None
            if s is None:
                continue
            non_null = int(s.notna().sum())
            nulls = int(s.isna().sum())
            col_stats = {"non_null": non_null, "nulls": nulls}
            try:
                col_stats.update({
                    "min": float(s.min()),
                    "max": float(s.max()),
                    "mean": float(s.mean()),
                })
            except Exception:
                pass
            stats[col] = col_stats

    return {"columns": cols, "stats": stats}
