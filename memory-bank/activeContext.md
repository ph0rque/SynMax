# Active Context

## Current focus
- Integrate planner and executor for deterministic queries with evidence.
- Implement analytics modules (trends, anomalies, correlations, clustering).
- Enforce privacy and save artifacts per run.

Decision: Use DuckDB as the primary engine for Parquet queries; optionally use Polars for specific analytics where beneficial. Always project needed columns and push filters down. Adopt argparse-based CLI with Rich for console output.

## Next steps
1) Wire CLI to planner, profiler, and executor for end-to-end deterministic queries.
2) Implement SQL template library and parameter binding with validation against schema cache.
3) Integrate reporter to save plan, SQL, and results under `./runs/<timestamp>/`.
4) Build analytics tools: trends, anomalies (z-score/IQR), correlations, clustering.
5) Add privacy guardrails (schema/aggregates only to LLM) and compliance logging.
6) Write README quick start and example sessions.

## Open questions
- Do we need join support across multiple Parquet files for v1? (Assume no.)
- Default OpenAI model choice and token budgets.

## Recent decisions
- Repository created as private on GitHub.
- `.env` and `data/` are excluded from version control.
- Directory structure implemented under `./agent/`.
- CLI uses argparse, with Rich for output.
