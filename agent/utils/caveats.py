from __future__ import annotations

from typing import Any, Dict, List


def _rows_cols(result: Any) -> Dict[str, int]:
    try:
        import pyarrow as pa  # type: ignore
        if isinstance(result, pa.Table):
            return {"rows": result.num_rows, "cols": result.num_columns}
    except Exception:
        pass
    try:
        import pandas as pd  # type: ignore
        if isinstance(result, pd.DataFrame):
            return {"rows": len(result), "cols": len(result.columns)}
    except Exception:
        pass
    return {"rows": 0, "cols": 0}


def build_caveats(result: Any, context: Dict[str, Any]) -> List[str]:
    notes: List[str] = []
    meta = _rows_cols(result)
    if meta.get("rows", 0) < 5:
        notes.append("Small sample of rows; interpret with caution.")
    prof = context.get("profile", {})
    # Example: flag high null-rate columns
    high_null_cols = [c for c, p in prof.items() if getattr(p, 'null_rate', 0.0) > 0.5]
    if high_null_cols:
        notes.append("High null rates detected in: " + ", ".join(high_null_cols[:5]))
    if context.get("analytics") == "correlation":
        notes.append("Correlation does not imply causation; trends may be confounded.")
    if context.get("analytics") == "clustering":
        notes.append("Cluster memberships depend on scaling and k; verify stability.")
    if context.get("analytics") == "anomalies_vs_category":
        notes.append("Anomalies are relative to category baselines; investigate data quality and one-off events.")
    return notes
