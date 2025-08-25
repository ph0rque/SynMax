from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import duckdb


@dataclass
class DuckDBConfig:
    timeout_sec: int = 30


class DuckDBExecutor:
    def __init__(self, config: Optional[DuckDBConfig] = None):
        self.config = config or DuckDBConfig()
        self._con = duckdb.connect(database=':memory:')

    def query(self, sql: str, params: Optional[List[Any]] = None):
        # DuckDB Python API doesn't expose per-query timeout directly; callers should control complexity
        result = self._con.execute(sql, params or [])
        try:
            return result.fetch_arrow_table()
        except Exception:
            return result.fetchall()

    def read_parquet(self, path: str, columns: Optional[List[str]] = None, where: Optional[str] = None, limit: Optional[int] = None):
        projection = ", ".join(columns) if columns else "*"
        sql = f"SELECT {projection} FROM read_parquet(?)"
        params: List[Any] = [path]
        if where:
            sql += f" WHERE {where}"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self.query(sql, params)

