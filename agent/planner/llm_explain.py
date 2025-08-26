from __future__ import annotations

import os
from typing import Any, Dict, Optional

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


def _result_metadata(result: Any) -> str:
    try:
        import pyarrow as pa  # type: ignore
        if isinstance(result, pa.Table):
            cols = ", ".join(result.column_names[:20])
            return f"rows={result.num_rows}, cols={result.num_columns} ({cols})"
    except Exception:
        pass
    try:
        import pandas as pd  # type: ignore
        if isinstance(result, pd.DataFrame):
            cols = ", ".join(result.columns[:20])
            return f"rows={len(result)}, cols={len(result.columns)} ({cols})"
    except Exception:
        pass
    return "result metadata unavailable"


def _safe_preview(result: Any, max_rows: int = 20) -> Optional[str]:
    if os.environ.get("ALLOW_LLM_RAW_PREVIEW", "0") not in {"1", "true", "True"}:
        return None
    try:
        import pyarrow as pa  # type: ignore
        if isinstance(result, pa.Table):
            import pandas as pd  # type: ignore
            df = result.slice(0, max_rows).to_pandas()
            return df.to_markdown(index=False)
    except Exception:
        pass
    try:
        import pandas as pd  # type: ignore
        if isinstance(result, pd.DataFrame):
            return result.head(max_rows).to_markdown(index=False)
    except Exception:
        pass
    return None


def summarize_answer(question: str, sql: str, result: Any, model: Optional[str] = None) -> Optional[str]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return None
    client = OpenAI(api_key=api_key)
    model_name = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

    metadata = _result_metadata(result)
    preview = _safe_preview(result)

    system = (
        "You are a data analyst assistant. Explain results clearly, conservatively, and concisely. "
        "Include: what was computed, key patterns, and short caveats. If a preview is not provided, reason using metadata only."
    )
    user = (
        f"Question:\n{question}\n\n"
        f"Executed SQL (truncated if long):\n{sql[:4000]}\n\n"
        f"Result metadata:\n{metadata}\n"
    )
    if preview:
        user += f"\nPreview (first rows):\n{preview}\n"
    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
            max_tokens=400,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return None
