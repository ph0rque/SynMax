from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional


class Reporter:
    def __init__(self, base_dir: str = "runs"):
        self.base_dir = base_dir

    def _run_dir(self) -> Path:
        ts = time.strftime("%Y%m%d-%H%M%S")
        p = Path(self.base_dir) / ts
        p.mkdir(parents=True, exist_ok=True)
        return p

    def save_artifacts(self, plan: Dict[str, Any], sql: Optional[str], results: Any, markdown_summary: str) -> str:
        run_dir = self._run_dir()
        (run_dir / "plan.json").write_text(json.dumps(plan, indent=2))
        if sql:
            (run_dir / "query.sql").write_text(sql)
        (run_dir / "results.json").write_text(json.dumps(self._safe_json(results), indent=2))
        (run_dir / "summary.md").write_text(markdown_summary)
        return str(run_dir)

    def _safe_json(self, obj: Any):
        try:
            import pandas as pd  # type: ignore
            if isinstance(obj, pd.DataFrame):
                return obj.head(100).to_dict(orient="records")
        except Exception:
            pass
        try:
            import pyarrow as pa  # type: ignore
            if isinstance(obj, pa.Table):
                return obj.slice(0, 100).to_pydict()
        except Exception:
            pass
        try:
            json.dumps(obj)
            return obj
        except Exception:
            return str(obj)
