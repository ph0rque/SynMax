from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, time as dtime
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

    def _prune_runs(self) -> None:
        max_runs = int(os.environ.get("RUNS_RETENTION", "50"))
        base = Path(self.base_dir)
        if not base.exists():
            return
        dirs = sorted([d for d in base.iterdir() if d.is_dir()], key=lambda d: d.name, reverse=True)
        for old in dirs[max_runs:]:
            try:
                for child in old.rglob('*'):
                    if child.is_file():
                        child.unlink(missing_ok=True)  # type: ignore[arg-type]
                old.rmdir()
            except Exception:
                # best-effort prune
                pass

    def save_artifacts(self, plan: Dict[str, Any], sql: Optional[str], results: Any, markdown_summary: str, latency_sec: Optional[float] = None) -> str:
        run_dir = self._run_dir()
        (run_dir / "plan.json").write_text(json.dumps(plan, indent=2))
        if sql:
            (run_dir / "query.sql").write_text(sql)
        (run_dir / "results.json").write_text(json.dumps(self._safe_json(results), indent=2))
        if latency_sec is not None:
            markdown_summary = f"Latency: {latency_sec:.2f}s\n\n" + markdown_summary
        (run_dir / "summary.md").write_text(markdown_summary)
        # Prune older runs
        self._prune_runs()
        return str(run_dir)

    def _safe_json(self, obj: Any):
        # Normalize common datetime types
        if isinstance(obj, (date, datetime, dtime)):
            return str(obj)
        if isinstance(obj, list):
            return [self._safe_json(x) for x in obj[:100]]
        if isinstance(obj, dict):
            return {k: self._safe_json(v) for k, v in obj.items()}
        try:
            import pandas as pd  # type: ignore
            if isinstance(obj, pd.DataFrame):
                return obj.head(100).to_dict(orient="records")
        except Exception:
            pass
        try:
            import pyarrow as pa  # type: ignore
            if isinstance(obj, pa.Table):
                # Convert to python dict and normalize nested values
                pyd = obj.slice(0, 100).to_pydict()
                return {k: [self._safe_json(v) for v in vals] for k, vals in pyd.items()}
            if isinstance(obj, pa.Scalar):
                return self._safe_json(obj.as_py())
        except Exception:
            pass
        try:
            json.dumps(obj)
            return obj
        except Exception:
            return str(obj)
