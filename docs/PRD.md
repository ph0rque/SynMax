# SynMax Data Agent — Product Requirements Document (PRD)

Version: 0.1 • Date: 2025-08-25
Owner: SynMax
References: `docs/project-requirements.md`

## 1) Overview
Build a CLI-first, chat-based Python agent that analyzes a large local Parquet dataset and answers executive questions ranging from deterministic metrics (counts, aggregates) to analytical insights (trends, anomalies, correlations, clustering) and plausible-cause hypotheses. Responses must include concise conclusions plus transparent evidence: selected columns, filters, and the executed SQL/code.

- Language: Python 3.10+
- Interface: CLI only (`synmax-agent`); no GUI/API
- LLM: OpenAI only (env: `OPENAI_API_KEY`); no offline fallback
- Dataset: Local `.parquet` in `./data/` (never committed)
- Scale: 25M rows today; plan for up to 100M rows
- Privacy: Do not send raw rows/values to LLM. Allow schema, column stats, and aggregates only.

## 2) Primary persona
- Executive decision-maker (e.g., CTO) seeking accurate, fast, and defensible insights without notebook/BI overhead.
- Priorities: correctness, speed, transparency, and actionable synthesis.

## 3) Scope
### In-scope (v1)
- Single Parquet file analytics (no joins), schema inference, missing-value handling.
- NL→plan→execute loop with LLM planning and tool calls.
- Deterministic and analytic queries with evidence and caveats.
- Artifact saving: plan, executed SQL, selected columns/filters, summary results.

### Out-of-scope (v1)
- Web UI, multi-user server, scheduling.
- Uploading data to third-party services.
- Long-running training pipelines.

## 4) Functional requirements
1. Data ingestion & profiling
   - Accept dataset path (default prompt; looks under `./data/`).
   - Infer schema/types; normalize dtypes (dates, categoricals, numerics).
   - Lightweight profiling: column counts, distincts (sampled for wide columns), null rates.

2. NL understanding & planning
   - Intent classification (deterministic vs analytic).
   - Produce step-by-step plan with columns, filters, metrics, methods.
   - Enforce privacy policy (only schema/aggregates to LLM).

3. Execution engine
   - DuckDB-first on Parquet (`read_parquet`), pushdown filters, project only needed columns.
   - Optional Polars/Arrow for advanced stats when beneficial (operate on aggregates or sampled subsets with disclosure).

4. Analytics features (v1 priority ranking)
   1) Trends/pattern recognition (time-based/grouped evolution)
   2) Anomaly/outlier detection (z-score/IQR, sudden shifts)
   3) Correlations (Pearson/Spearman as appropriate; warn on spurious correlations)
   4) Clustering (k-means/mini-batch on selected features; input scaling; silhouette check)

5. Evidence & reproducibility
   - Always include executed SQL or code snippet, selected columns, filters, and summary stats.
   - Save run artifacts under `./runs/<timestamp>/` (plan.json, query.sql, results.json/markdown).

6. CLI UX
   - Command: `synmax-agent`
   - Minimal flags (best practice defaults):
     - `-p, --path PATH` (Parquet path; default prompt, search `./data/`)
     - `--model MODEL` (OpenAI model; default sensible value)
     - `--save-run/--no-save-run` (default: save)
     - `--max-preview-rows N` (limits local previews; default 1000)
     - `--timeout-sec N` (execution guardrail)
   - Interactive chat loop; `:exit` to quit.

## 5) Non-functional requirements
- Accuracy: numerical answers must match executed results; summaries cannot contradict evidence.
- Latency targets (8-core M2, 16GB):
  - Deterministic aggregates on 25M rows: < 5s typical; 100M: < 10s typical.
  - Analytics (correlations/anomalies/clustering) on aggregates/subsets: < 15s typical.
- Resource efficiency: avoid full materialization; operate on projections/aggregates.
- Reliability: input validation, clear error messages, safe fallbacks.

## 6) Privacy & compliance policy
- Never send raw rows or identifiers to LLM.
- Allowed to send: schema (column names/types), aggregated statistics, query plans, and high-level summaries.
- If a request would require raw rows, either:
  - perform locally and summarize results for the LLM, or
  - refuse with guidance and offer an aggregate alternative.

## 7) Architecture
- Components:
  - CLI Orchestrator (Typer/argparse + Rich)
  - Planner (OpenAI prompts + tool selection)
  - Tools: `profile_data`, `run_sql`, `compute_stats`, `detect_outliers`, `compute_correlations`, `cluster_segments`
  - Executor: DuckDB on Parquet; Polars for advanced stats as needed
  - Reporter: assembles answer, evidence, caveats; writes artifacts
- Data flow:
  - NL query → Planner → Tool calls (SQL/stats) → Results → Reporter (answer + evidence)

## 8) Directory layout (proposed)
- `./agent/` (application code)
  - `cli/`, `planner/`, `tools/`, `exec/`, `report/`, `utils/`
- `./docs/` (PRD, README, design notes)
- `./data/` (local datasets; gitignored)
- `./runs/` (artifacts; gitignored)

## 9) LLM configuration (OpenAI only)
- Env: `OPENAI_API_KEY` required.
- Default model: specify in config (e.g., `gpt-4o-mini`) with override via `--model`.
- Prompting: system prompts enforce privacy rules; planner must reference evidence from executed steps.

## 10) Acceptance criteria
- CLI starts, prompts for dataset path or finds one in `./data/`.
- Deterministic queries (counts, group-bys, filtered aggregates) return accurate results and emitted SQL.
- Analytics features deliver prioritized insights with caveats and evidence.
- No raw rows are sent to LLM; logs prove compliance.
- Artifacts saved to `./runs/<timestamp>/`.
- README includes install, quick start, dataset instructions, examples, assumptions/limitations.

## 11) Risks & mitigations
- Performance on 100M rows: strict projection, predicate pushdown, pre-aggregation, sampling with disclosure.
- LLM plan unreliability: guardrails, retries, smaller subplans, show executed SQL as source of truth.
- Spurious correlations: significance checks, rank by strength and sample size, caveats.

## 12) Milestones
- M1: CLI skeleton, dataset path handling, basic profiling.
- M2: Deterministic planner + DuckDB execution + evidence output.
- M3: Analytics (trends, anomalies, correlations, clustering) + caveat framework.
- M4: Artifact saving, README polish, example sessions, tuning for 100M.

## 13) Open items (TBD)
- Top 3 evaluation scenarios (to be provided by stakeholder).
- Exact default OpenAI model and token budgets.
- Optional CSV export of result tables.

## 14) Example interactions (illustrative)
- "What is total count in 2024 vs 2023?"
- "Which segment grew fastest month-over-month in Q2 2025?"
- "Flag any anomalous spikes by region last 90 days and suggest checks."
