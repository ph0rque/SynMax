# Data Agent PRD (Project Brief)

## Overview
Build a CLI-first, chat-based Python agent that can ingest and analyze a large Parquet dataset (~25M rows) and answer natural-language questions ranging from simple deterministic queries (counts, filters, aggregations) to analytical tasks (patterns, anomalies, clustering, correlations) and hypothesis generation with evidence and caveats.

- Language: Python 3.10+
- No frontend/API required (CLI only)
- LLM providers: OpenAI and/or Anthropic via env vars (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`)
- Dataset: Local `.parquet` file provided at runtime (not committed to repo)
- Performance: Favor columnar/lazy execution and predicate pushdown for speed

## Goals & Non-Goals
### Goals
- Ingest Parquet efficiently and infer schema/types; handle missing values.
- Understand natural-language questions and plan an executable analysis.
- Execute analysis with efficient engines (e.g., DuckDB/Polars/Arrow).
- Return:
  - Concise answer/conclusion
  - Supporting evidence: methods used, selected columns, filters, SQL or code steps
- Support both deterministic queries and deeper analytics (patterns, anomalies, causal hypotheses with caveats).

### Non-Goals
- No GUI/web app, no multi-user server.
- No heavy/long-running ML training pipelines. Lightweight analytics only.
- No uploading the dataset to external services; keep local.

## Primary Users
- Analyst/PM/Engineer needing quick answers and investigative analysis on local Parquet data without spinning up a BI tool or notebook.

## Key Use Cases
- Deterministic: counts by filter/time, top-K, distinct counts, simple joins (single file assumed for v1), percentiles, missingness.
- Patterns: trend detection, seasonality hints, clustering of segments, correlation analysis.
- Anomalies: outlier detection, rule violations, sudden shifts.
- Hypotheses: propose plausible causes with explicit assumptions/limitations and point to evidence.

## Functional Requirements
1) Data Access
- Load Parquet lazily with schema inference and type normalization.
- Support absolute/relative path input or auto-download to `./data/` (gitignored).

2) NL Understanding & Planning
- Classify user query intent (deterministic vs analytic).
- Produce a plan: steps, transformations, metrics, and checks.

3) Execution Engine
- Execute planned steps using DuckDB (preferred) or Polars; leverage predicate pushdown and column pruning.
- Provide safe query execution (guardrails, error handling, clear messages on failures).

4) Evidence & Reporting
- Return a concise answer plus: selected columns, applied filters, SQL/code used, and summary stats/visual hints (ASCII tables/sparklines if helpful).
- Communicate uncertainty and assumptions, especially for hypotheses.

5) Scale & Performance
- Handle ~25M rows on a developer laptop: avoid full materialization, stream/limit where possible, sample intelligently for exploratory analytics while allowing full precise runs for deterministic answers.

## Non-Functional Requirements
- Accuracy prioritized over verbosity; avoid hallucinations by reflecting exact executed steps.
- Latency target: most deterministic queries < 5s; analytic tasks < 15s when feasible.
- Resource-aware: avoid loading entire dataset into memory.
- Reproducibility: emit the SQL or code steps required to reproduce answers.

## Inputs & Outputs
- Input: natural-language question; optional dataset path (CLI arg, prompt, or config).
- Output: structured response with:
  - Final answer
  - Evidence (columns, filters, SQL/code snippet)
  - Method summary and caveats

## Acceptance Criteria
- CLI can accept dataset path or auto-download to `./data/` and ignore in git.
- Deterministic queries (e.g., count with filters/time constraints) produce correct results with emitted SQL/code.
- Analytics features: pattern detection, anomaly identification, and correlation/clustering on selected columns with defensible methods and caveats.
- Evidence is always included and consistent with the executed steps.
- README contains installation, quick start, dataset supply instructions, examples, and limitations.

## Risks & Mitigations
- Large file performance: use DuckDB/Polars with predicate pushdown, project only required columns, push filters early.
- LLM reliability: instruction-tune prompts, add guardrails, retry with smaller plans, and show executed SQL/code as source of truth.
- Memory limits: avoid full DataFrame materialization; use streaming and LIMIT for previews; run heavy ops on subsets when appropriate with clear disclosure.

## Milestones (High-Level)
- M1: Skeleton CLI, config, dataset path handling, basic profiling (schema/head).
- M2: Deterministic query planner + execution + evidence output.
- M3: Analytical modules (patterns/anomalies/correlations/clustering) with caveat framework.
- M4: README polish, examples, and performance tuning.
