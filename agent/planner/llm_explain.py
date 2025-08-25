from __future__ import annotations

import os
from typing import Any, Dict, Optional

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


def _table_preview(result: Any, max_rows: int = 50) -> str:
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
    # Fallback string
    return str(result)[:8000]


def summarize_answer(question: str, sql: str, result: Any, model: Optional[str] = None) -> Optional[str]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return None
    client = OpenAI(api_key=api_key)
    model_name = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    preview = _table_preview(result)

    system = (
        "You are a data analyst assistant. Explain results clearly, conservatively, and concisely. "
        "Include: what was computed, key figures/patterns, and short caveats. Use bullet points where helpful."
    )
    user = (
        f"Question:\n{question}\n\n"
        f"Executed SQL (truncated if long):\n{sql[:4000]}\n\n"
        f"Result preview (first rows):\n{preview}\n"
    )
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
