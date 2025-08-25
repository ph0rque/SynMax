from __future__ import annotations

from typing import Any, Dict, List, Tuple

from agent.exec.duck import DuckDBExecutor
from agent.exec.sql_builder import escape_ident


def daily_totals(executor: DuckDBExecutor, parquet_path: str) -> Any:
    sql = "SELECT eff_gas_day::DATE AS day, SUM(scheduled_quantity) AS total_qty FROM read_parquet(?) GROUP BY 1 ORDER BY 1"
    return executor.query(sql, [parquet_path])


def top_k_by(executor: DuckDBExecutor, parquet_path: str, column: str, k: int = 10) -> Any:
    sql = f"SELECT {escape_ident(column)} AS key, SUM(scheduled_quantity) AS total_qty FROM read_parquet(?) GROUP BY 1 ORDER BY 2 DESC LIMIT ?"
    return executor.query(sql, [parquet_path, k])


def anomaly_candidates(executor: DuckDBExecutor, parquet_path: str, z: float = 3.5) -> Any:
    sql = (
        "WITH d AS ("
        "  SELECT eff_gas_day::DATE AS day, SUM(scheduled_quantity) AS total_qty FROM read_parquet(?) GROUP BY 1"
        ") SELECT day, total_qty, (total_qty - AVG(total_qty) OVER())/NULLIF(STDDEV_POP(total_qty) OVER(),0) AS zscore"
        " FROM d WHERE ABS((total_qty - AVG(total_qty) OVER())/NULLIF(STDDEV_POP(total_qty) OVER(),0)) >= ? ORDER BY ABS(zscore) DESC"
    )
    return executor.query(sql, [parquet_path, z])
