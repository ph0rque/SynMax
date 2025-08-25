# SynMax Data Agent

CLI-based, chat-style agent to analyze local Parquet data with deterministic queries and analytics (trends, anomalies, correlations, clustering). Evidence-first: shows executed SQL and saves artifacts.

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
- OpenAI (optional for future LLM planner expansion): set `OPENAI_API_KEY`.
  - Create a `.env` file at project root (see `.env.sample`) and add:
    `OPENAI_API_KEY=sk-your-key-here`
  - The CLI features implemented so far do not require the key, but providing it prepares for LLM-powered planning.

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
.venv/bin/python -m agent.cli.main --query "cluster pipelines monthly"
```

## Features
- DuckDB over Parquet with projection pruning and predicate pushdown
- Rule-based planner for common questions; SQL builder with validation
- Analytics: trends, z-score anomalies, correlation of pipeline daily totals, k-means clustering of monthly profiles
- Artifacts saved under `./runs/<timestamp>/` (plan.json, query.sql, results.json, summary.md)

## Privacy
- No raw rows sent to LLMs (OpenAI for future planner expansion). Only schema and aggregates allowed.

## Limitations
- Single Parquet file v1 (no joins)
- Analytics use heuristics (simple z-score, Pearson corr, k-means); interpret with caveats
- Lat/lon missingness may limit geospatial insights

## Development
- Interactive mode: run without --query and ask questions
- Experiments and scripts under `./experiments/`
- Planned questions in `experiments/planned-questions.md`
