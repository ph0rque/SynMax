from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


def choose_analytic_tool(question: str, schema_columns: list[str], model: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Ask OpenAI to pick an analytics tool and parameters for complex queries.
    Returns a dict like: {"tool": "correlation"|"clustering"|"anomalies_vs_category", "params": {...}}
    Returns None if no API key or planner not available.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return None
    client = OpenAI(api_key=api_key)
    model_name = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

    system = (
        "You are a planning assistant that maps natural-language questions to one of the allowed analytics tools. "
        "Only respond with valid JSON. Do NOT invent columns or tools."
    )
    tools_desc = (
        "Allowed tools:\n"
        "- correlation: compute correlations across pipeline daily totals. params: {method:'pearson'|'spearman', include_pvalue:bool}\n"
        "- clustering: cluster pipelines by monthly totals. params: {k:int (1-20), scaling:'standard'|'minmax'|'none', algorithm:'kmeans'|'minibatch', seed:int}\n"
        "- anomalies_vs_category: flag loc_name anomalies vs category baselines. params: {z:float (1-10), min_days:int (1-365), year:int?, state:str?, rec_del_sign:int?}\n"
        "- anomalies_iqr: flag daily total outliers via IQR. params: {k:float (0.5-5), limit:int}\n"
        "- sudden_shifts: detect rolling deviations. params: {window:int (3-60), sigma:float (1-10), limit:int}\n"
        "- trends: summarize trends by 'month' or 'day' with moving averages. params: {by:'month'|'day', window_ma:int[]?, yoy:bool?}"
    )
    user = (
        f"Schema columns: {', '.join(schema_columns)}\n\n"
        f"Question: {question}\n\n"
        f"{tools_desc}\n"
        "Respond with JSON object with keys 'tool' and 'params'."
    )
    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.1,
            max_tokens=200,
        )
        content = resp.choices[0].message.content or "{}"
        data = json.loads(content)
        if isinstance(data, dict) and "tool" in data and "params" in data:
            return data
    except Exception:
        return None
    return None
