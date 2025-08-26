from __future__ import annotations

from typing import Any, Dict, Optional


def _first_row_dict(result: Any) -> Optional[Dict[str, Any]]:
    try:
        import pyarrow as pa  # type: ignore
        if isinstance(result, pa.Table) and result.num_rows > 0:
            row0 = {name: result.column(i)[0].as_py() for i, name in enumerate(result.column_names)}
            return row0
    except Exception:
        pass
    try:
        import pandas as pd  # type: ignore
        if isinstance(result, pd.DataFrame) and not result.empty:
            return result.iloc[0].to_dict()
    except Exception:
        pass
    return None


def make_concise_answer(result: Any, context: Dict[str, Any]) -> str:
    kind = context.get("analytics") or context.get("intent")
    row = _first_row_dict(result) or {}

    # Analytics cases first
    if kind == "correlation":
        a, b, corr = row.get("a"), row.get("b"), row.get("corr")
        if a is not None and b is not None and corr is not None:
            return f"Answer: strongest correlation pair = {a} ↔ {b} (corr={corr:.3f})"
        return "Answer: computed top correlation pairs."

    if kind == "clustering":
        # Expect list of rows with columns: pipeline_name, cluster, k, scaling, silhouette
        try:
            import pyarrow as pa  # type: ignore
            if isinstance(result, pa.Table) and result.num_rows > 0:
                clusters = [int(result.column(result.column_names.index("cluster"))[i].as_py()) for i in range(result.num_rows)]
                from collections import Counter
                cnt = Counter(clusters)
                parts = ", ".join([f"c{c}={n}" for c, n in sorted(cnt.items())])
                k_eff = row.get("k")
                return f"Answer: clustering complete (k={k_eff}) — {parts}"
        except Exception:
            pass
        return "Answer: clustering complete."

    if kind == "anomalies_vs_category":
        loc = row.get("loc_name")
        cat = row.get("category_short")
        z = row.get("max_abs_z")
        days = row.get("anomaly_days")
        if loc is not None:
            return f"Answer: top anomalous location = {loc} ({cat}), max|z|={z:.2f}, anomaly_days={days}"
        return "Answer: identified anomalous locations vs category baselines."

    # Deterministic patterns
    if "row_count" in row:
        return f"Answer: row_count = {row['row_count']}"
    if "distinct_count" in row:
        return f"Answer: distinct_count = {row['distinct_count']}"
    if "total_scheduled_quantity" in row:
        # Try to include a dimension column if present (pipeline_name, month, loc_name, etc.)
        for dim in ("pipeline_name", "month", "loc_name", "state_abb", "category_short"):
            if dim in row:
                return f"Answer: top {dim} = {row[dim]} (total_scheduled_quantity={row['total_scheduled_quantity']})"
        return f"Answer: total_scheduled_quantity = {row['total_scheduled_quantity']}"

    # Fallback
    return "Answer: results computed."
