# Heuristics and LLM Panels

This agent surfaces its decision path after each answer via two short panels:
- "Heuristic:" — which rule/trigger ran and with what parameters
- "LLM(explain|planner):" — which LLM role was used and whether a key was present

## How to view

- During tests: set SHOW_IT=1 and run with -s to disable capture, for example:
  - SHOW_IT=1 pytest -q -s
  - SHOW_IT=1 pytest tests/test_cli_integration.py -q -s
- During CLI use: run with --query and observe printed panels

## Heuristic mapping (NL → action)

- Deterministic rule-based:
  - Patterns like `count`, `sum scheduled_quantity by <dim>`, `top N <dim> by scheduled_quantity`, or `sum ... by month` map to validated SQL plans.
  - Panel: `Heuristic: deterministic rule plan (<notes>)`

- Analytics triggers (direct keyword routing):
  - Correlation: contains `correlation|correlat` → `correlation_pipelines`
    - Params: `method=pearson|spearman`, `pvalue=true|false`
  - Clustering: contains `cluster|clustering` → `cluster_pipelines_monthly`
    - Params: `k`, `scale=standard|minmax|none`, `algorithm=kmeans|minibatch`, `seed`
  - Trends: contains `trend|trends` → `trends_summary` (`by=month|day`)
  - Seasonality: contains `seasonality|seasonal` → `seasonality_summary` (`group_col` optional)
  - Top trending: contains `top trending|top trend` → `top_trending_segments` (`group_col`, `top`, `min-months`)
  - Anomalies (IQR): contains `IQR` and `anomal|outlier` → `anomalies_iqr` (`k`)
  - Sudden shifts: contains `sudden|shift` → `sudden_shifts` (`window`, `sigma`)
  - Category baseline anomalies: contains `anomal*` and `category|categories` → `anomalies_vs_category` (z, min_days; optional state/year/receipts-deliveries parsed)

- LLM planner (fallback when rule parse fails):
  - If `OPENAI_API_KEY` is set, the planner may select one of the above tools with parameters.
  - Panel: `Heuristic: LLM planner tool='<tool>' (...)`

## Transparency and artifacts

- Panels accompany the concise Answer and precede the tabular result.
- Artifacts include: selected method/tool, parameters, pseudo-steps, executed SQL (when applicable), latency, caveats, and (if enabled) hypotheses.
- Missing-value handling is disclosed (e.g., `COALESCE(...,0)` for totals).
