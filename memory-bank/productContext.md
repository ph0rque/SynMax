# Product Context

## Why this project exists
Analysts need fast, reliable answers from large local Parquet datasets without spinning up notebooks or BI dashboards. The agent provides a conversational interface that integrates planning, execution, and evidence, enabling both quick facts and exploratory insights.

## Problems it solves
- Bridging natural language questions and executable analytics on large columnar data.
- Consistent evidence with each answer for trust and reproducibility.
- Detecting patterns/anomalies and suggesting plausible causes with clear caveats.

## How it should work
- User provides a dataset path or enables auto-download to a gitignored `./data/` directory.
- User asks a question; the agent plans steps and executes via DuckDB/Polars.
- The agent returns a concise answer plus the exact SQL/code and filters used.

## User experience goals
- Minimal setup; works on macOS/Linux with Python 3.10+.
- Low-latency deterministic queries; reasonable latency for analytics.
- Transparent: always show what was executed; never hide assumptions.
- Safe failure modes with helpful guidance and next-step suggestions.

## Success indicators
- Correctness on evaluator's pre-written queries.
- Insightful findings for non-trivial analytics with business-relevant interpretations.
- Robust performance on ~25M rows without exhausting memory.
