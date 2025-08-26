from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import silhouette_score

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


def anomalies_vs_category(
    executor: DuckDBExecutor,
    parquet_path: str,
    z_threshold: float = 3.0,
    min_anomaly_days: int = 3,
    limit: int = 50,
    year: Optional[int] = None,
    state: Optional[str] = None,
    rec_del_sign: Optional[int] = None,
) -> pa.Table:
    """
    Identify locations whose daily totals deviate from category baselines (per-day category mean/std).
    Filters: optional year, state_abb, and rec_del_sign (-1 receipts, 1 deliveries).
    Returns top locations by max |z| with counts.
    """
    where_clauses: List[str] = []
    params: List[Any] = [parquet_path]
    if year is not None:
        where_clauses.append("eff_gas_day BETWEEN ? AND ?")
        params.extend([f"{year}-01-01", f"{year}-12-31"]) 
    if state is not None:
        where_clauses.append("state_abb = ?")
        params.append(state)
    if rec_del_sign is not None:
        where_clauses.append("rec_del_sign = ?")
        params.append(int(rec_del_sign))
    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    sql = (
        "WITH base AS ("
        "  SELECT eff_gas_day::DATE AS day, category_short, loc_name, SUM(scheduled_quantity) AS total_qty"
        "  FROM read_parquet(?)" + where_sql + " GROUP BY 1,2,3"
        "), cat_stats AS ("
        "  SELECT day, category_short, AVG(total_qty) AS cat_avg, STDDEV_POP(total_qty) AS cat_std, COUNT(*) AS n_locs"
        "  FROM base GROUP BY 1,2"
        "), scored AS ("
        "  SELECT b.day, b.category_short, b.loc_name, b.total_qty, c.cat_avg, c.cat_std, c.n_locs,"
        "         (b.total_qty - c.cat_avg)/NULLIF(c.cat_std,0) AS zscore"
        "  FROM base b JOIN cat_stats c USING(day, category_short)"
        ")"
        " SELECT category_short, loc_name,"
        "        COUNT_IF(ABS(zscore) >= ?) AS anomaly_days,"
        "        MAX(ABS(zscore)) AS max_abs_z,"
        "        AVG(ABS(zscore)) AS avg_abs_z,"
        "        COUNT(*) AS days_observed"
        " FROM scored"
        " WHERE zscore IS NOT NULL"
        " GROUP BY 1,2"
        " HAVING anomaly_days >= ?"
        " ORDER BY max_abs_z DESC"
        " LIMIT ?"
    )
    params2 = params + [z_threshold, min_anomaly_days, limit]
    return executor.query(sql, params2)


def _top_pipelines(executor: DuckDBExecutor, parquet_path: str, k: int = 20) -> List[str]:
    tbl = executor.query(
        "SELECT pipeline_name, SUM(scheduled_quantity) AS total FROM read_parquet(?) GROUP BY 1 ORDER BY 2 DESC LIMIT ?",
        [parquet_path, k],
    )
    df = tbl.to_pandas()
    return df["pipeline_name"].dropna().astype(str).tolist()


def correlation_pipelines(executor: DuckDBExecutor, parquet_path: str, top_k_pipelines: int = 20, top_pairs: int = 20) -> pa.Table:
    pipelines = _top_pipelines(executor, parquet_path, top_k_pipelines)
    if not pipelines:
        return pa.table({"a": [], "b": [], "corr": []})
    placeholders = ", ".join(["?"] * len(pipelines))
    sql = (
        f"SELECT eff_gas_day::DATE AS day, pipeline_name, SUM(scheduled_quantity) AS total_qty "
        f"FROM read_parquet(?) WHERE pipeline_name IN ({placeholders}) GROUP BY 1,2 ORDER BY 1"
    )
    params = [parquet_path] + pipelines
    tbl = executor.query(sql, params)
    df = tbl.to_pandas()
    if df.empty:
        return pa.table({"a": [], "b": [], "corr": []})
    pivot = df.pivot(index="day", columns="pipeline_name", values="total_qty").fillna(0)
    corr = pivot.corr(method="pearson")
    # Rename axes to avoid duplicate names, then extract upper off-diagonal pairs
    corr.index.name = "a"
    corr.columns.name = "b"
    mask = np.triu(np.ones(corr.shape), k=0).astype(bool)
    corr_pairs = corr.where(~mask).stack().reset_index(name="corr").sort_values("corr", ascending=False).head(top_pairs)
    return pa.Table.from_pandas(corr_pairs, preserve_index=False)


def cluster_pipelines_monthly(
    executor: DuckDBExecutor,
    parquet_path: str,
    k: int = 5,
    top_k_pipelines: int = 50,
    scaling: str = "standard",
) -> pa.Table:
    """
    Cluster pipelines by monthly total profiles.
    scaling: 'standard' | 'minmax' | 'none'
    Returns a table with pipeline_name, cluster, k, scaling, silhouette.
    """
    pipelines = _top_pipelines(executor, parquet_path, top_k_pipelines)
    if not pipelines:
        return pa.table({"pipeline_name": [], "cluster": [], "k": [], "scaling": [], "silhouette": []})
    placeholders = ", ".join(["?"] * len(pipelines))
    sql = (
        f"SELECT date_trunc('month', eff_gas_day)::DATE AS month, pipeline_name, SUM(scheduled_quantity) AS total_qty "
        f"FROM read_parquet(?) WHERE pipeline_name IN ({placeholders}) GROUP BY 1,2 ORDER BY 1"
    )
    params = [parquet_path] + pipelines
    tbl = executor.query(sql, params)
    df = tbl.to_pandas()
    if df.empty:
        return pa.table({"pipeline_name": [], "cluster": [], "k": [], "scaling": [], "silhouette": []})
    pivot = df.pivot(index="pipeline_name", columns="month", values="total_qty").fillna(0)
    scaler = StandardScaler(with_mean=True, with_std=True) if scaling == "standard" else (MinMaxScaler() if scaling == "minmax" else None)
    X = scaler.fit_transform(pivot.values) if scaler is not None else pivot.values
    n_samples = X.shape[0]
    k_eff = max(1, min(k, n_samples))
    km = KMeans(n_clusters=k_eff, n_init=10, random_state=42)
    labels = km.fit_predict(X)
    sil = None
    try:
        if len(set(labels)) > 1 and X.shape[0] > k_eff and k_eff > 1:
            sil = float(silhouette_score(X, labels))
    except Exception:
        sil = None
    out = pd.DataFrame({"pipeline_name": pivot.index.tolist(), "cluster": labels.tolist(), "k": [k_eff]*len(labels), "scaling": [scaling]*len(labels), "silhouette": [sil]*len(labels)})
    return pa.Table.from_pandas(out, preserve_index=False)
