# SynMax Data Agent

CLI-based, chat-style agent to analyze local Parquet data with deterministic queries and analytics (trends, anomalies, correlations, clustering). Evidence-first: shows executed SQL and saves artifacts with latency and optional LLM explanations.

## Installation
- Python 3.11+ recommended
- Create venv and install deps:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pandas
```

## Dataset
- Place your Parquet under `./data/` (gitignored). Example: `./data/pipeline_data.parquet`.
- Or pass `--path /absolute/path/to/file.parquet`.

## Environment variables
- `OPENAI_API_KEY` (optional):
  - Used to: (1) generate concise explanations of results, and (2) plan complex queries when the rule-based planner cannot map a request (OpenAI planner picks an analytics tool + params).
  - Not used for: deterministic SQL generation or local analytics execution, which always run locally in DuckDB/Sklearn.
  - Disable by omitting the key (features gracefully fallback). Control preview sharing with `ALLOW_LLM_RAW_PREVIEW` (off by default).
- `OPENAI_MODEL` (optional): default `gpt-4o-mini`.
- `RUNS_RETENTION` (optional): number of run folders to keep (default 50).
- `ALLOW_LLM_RAW_PREVIEW` (optional): set to `1` to allow first-rows preview to be sent to LLM; otherwise metadata-only.

Copy `.env.sample` to `.env` and edit as needed.

## Quick start
Preview and ask a one-shot question:
```
.venv/bin/python -m agent.cli.main --query "top 5 pipeline_name by scheduled_quantity"
```
Monthly totals in 2024:
```
.venv/bin/python -m agent.cli.main --query "sum scheduled_quantity by month in 2024"
```
Correlations / Clusters:
```
.venv/bin/python -m agent.cli.main --query "show pipeline correlation"
.venv/bin/python -m agent.cli.main --query "cluster pipelines monthly k=6 scale=minmax"
```
Anomalies vs category baseline (flags locations that deviate vs their category):
```
.venv/bin/python -m agent.cli.main --query "identify anomalous points that behave outside of their point categories in 2024 state TX deliveries z=3.5 min_days=5"
```
Each answer prints a concise `Answer:` line first, then shows executed SQL (if applicable), prints a result table and latency in seconds, and saves artifacts under `./runs/<timestamp>/`.

## Features
- DuckDB over Parquet with projection pruning and predicate pushdown
- Rule-based planner for common questions; SQL builder with validation
- OpenAI-backed planner for complex queries (tool selection) when available
- Analytics: trends, z-score anomalies, correlation of pipeline daily totals, k-means clustering of monthly profiles (scaling options, silhouette)
- Category-baseline anomaly detection (per-day category mean/std) with filters/thresholds
- LLM explanations (metadata-only by default; optional row preview)
- Artifacts saved under `./runs/<timestamp>/` (plan.json, query.sql, results.json, summary.md) and retention via `RUNS_RETENTION`

## Privacy
- Experimental project; avoid sharing the dataset externally.
- No raw rows sent to LLM unless `ALLOW_LLM_RAW_PREVIEW=1`.

## Testing
- Create venv and install requirements, then run:
```
source .venv/bin/activate
pytest -q
```
- Tests include unit tests (SQL builder, planner, analytics) and CLI integration tests using a synthetic Parquet fixture under `tests/fixtures/`.

## Development
- Interactive mode: run without --query and ask questions
- Experiments and scripts under `./experiments/`
- Planned questions in `experiments/planned-questions.md`
