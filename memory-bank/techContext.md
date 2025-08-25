# Tech Context

## Languages & Runtimes
- Python 3.10+

## Core Libraries (candidate set)
- duckdb (SQL on Parquet)
- pyarrow (Arrow memory format; Parquet IO)
- polars (optional: fast DataFrame ops)
- numpy, scipy, scikit-learn (stats, clustering, anomaly detection)
- rich (CLI formatting), typer or argparse (CLI)
- openai / anthropic (LLM clients)

## Setup & Keys
- Keys via env vars: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` (optional; support either).
- Dataset path provided via CLI flag, prompt, or config; or auto-download to `./data/` (gitignored).

## Constraints
- No GUI/API; single-user CLI flow.
- Do not commit datasets; ensure `./data/` and `*.parquet` are ignored.
- Operate efficiently on ~25M rows without exhausting RAM.

## Packaging
- `requirements.txt` for dependencies; `README.md` with install and usage.

## Testing & Validation
- Golden test queries for deterministic accuracy.
- Spot checks for analytics with sanity constraints and synthetic data.
