from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from agent.exec.duck import DuckDBExecutor


@dataclass
class ColumnInfo:
    name: str
    type: str


@dataclass
class SchemaSnapshot:
    columns: List[ColumnInfo]
    datetime_columns: List[str] = field(default_factory=list)


class SchemaCache:
    def __init__(self) -> None:
        self._cache: Dict[str, SchemaSnapshot] = {}

    def get_or_load(self, executor: DuckDBExecutor, parquet_path: str) -> SchemaSnapshot:
        key = parquet_path
        if key in self._cache:
            return self._cache[key]
        # Load schema via DESCRIBE
        arrow_tbl = executor.query("DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [parquet_path])
        df = arrow_tbl.to_pandas()
        columns = [ColumnInfo(name=row["column_name"], type=row["column_type"]) for _, row in df.iterrows()]
        datetime_cols = [c.name for c in columns if any(t in c.type.upper() for t in ["DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME"])]
        snapshot = SchemaSnapshot(columns=columns, datetime_columns=datetime_cols)
        self._cache[key] = snapshot
        return snapshot

    def list_column_names(self, parquet_path: str) -> List[str]:
        snap = self._cache.get(parquet_path)
        return [c.name for c in snap.columns] if snap else []
