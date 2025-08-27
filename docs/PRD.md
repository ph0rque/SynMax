# SynMax Data Agent — Product Requirements Document (PRD)

Version: 0.1 • Date: 2025-08-25
Owner: SynMax
References: `docs/project-requirements.md`

## 1) Overview
Build a CLI-first, chat-based Python agent that analyzes a large local Parquet dataset and answers analyst questions ranging from deterministic metrics (counts, aggregates) to analytical insights (trends, anomalies, correlations, clustering) and plausible-cause hypotheses. Responses must include a concise one-line answer first, then transparent evidence: selected columns, filters, executed SQL/code, and latency in seconds.

- Language: Python 3.10+
- Interface: CLI only (`synmax-agent`)
- LLM: OpenAI (env: `OPENAI_API_KEY`) to explain results and plan complex analyses (“talk to the data”).
- Dataset: Local `.parquet` in `./data/` (never committed)
- Scale: 25M rows today; design toward 100M+ and beyond
- Privacy: Experimental project; avoid sharing dataset externally. No restrictive policy required.

## 2) Primary persona
- Analyst with domain familiarity wanting: (1) easy retrieval/summarization, (2) analytic exploration with agent support.
- Example intents: “recent trend for production points in LA”, “which point categories have the strongest seasonal correlation”, “identify anomalous points that behave outside their point categories”.

## 3) Scope
### In-scope (v1)
- Single Parquet file analytics (no joins), schema inference, missing-value handling.
- NL→plan→execute loop with LLM explanation and tool calls.
- Deterministic and analytic queries with concise answer, evidence, latency, and caveats.
- Artifact saving: plan, executed SQL, selected columns/filters, summary results, latency. Retain latest N runs.

### Out-of-scope (v1)
- Web UI, multi-user server, scheduling.
- Uploading data to third-party services.
- Long-running training pipelines.

## 4) Functional requirements
1. Data ingestion & profiling
   - Accept dataset path (default prompt; looks under `./data/`).
   - Infer schema/types; normalize dtypes (dates, categoricals, numerics).
   - Lightweight profiling: column counts, distincts (sampled), null rates.
   - Missing values: define strategy per metric (drop, zero-fill for strictly non-negative aggregations, forward-fill for time series where appropriate); always disclose handling in evidence.

2. NL understanding & planning
   - Classify intent (deterministic vs analytic) and produce a plan.
   - LLM explanations: summarize results/methods/caveats; optional plan refinement.
   - Hypothesis generation: propose 1–3 plausible causes for findings (never assertions), each linked to observed evidence and accompanied by explicit caveats/assumptions.

3. Execution engine
   - DuckDB-first on Parquet (`read_parquet`); pushdown filters; project needed columns.
   - Optional Polars/Arrow for advanced stats on aggregates/subsets; prefer aggregate-first execution to reduce memory footprint.

4. Analytics features (v1 priorities)
   1) Trends/patterns (time-based/grouped evolution)
   2) Anomalies (z-score/IQR, sudden shifts)
   3) Correlations (Pearson/Spearman; caution on spurious)
   4) Clustering (k-means/minibatch; scaling options; silhouette)
   - Trends details:
     - Compute MoM/QoQ/YoY growth; 7/30-day moving averages; rolling summaries.
     - Seasonality hints via month-of-year aggregates and simple decomposition on aggregates.
     - Top-K trending segments by growth rate with minimum sample thresholds.
   - Anomalies details:
     - Support z-score and IQR (1.5×/3× fences) methods; parameterized thresholds.
     - Sudden shift detection via rolling mean/variance or CUSUM-style change flags.
     - Report anomaly counts, max/avg deviation magnitude, and observation counts.
   - Correlations details:
     - Pearson and Spearman; include sample sizes and p-values where feasible.
     - Warn about multiple comparisons and require minimum observations per pair.
   - Clustering details:
     - Standard and MiniBatch KMeans; scaling options (standard/minmax/none).
     - Record random seed, k, scaling, and silhouette score for reproducibility.

5. Evidence, latency & reproducibility
   - Always include executed SQL/code, selected columns, filters, summary stats.
   - Display wall-clock latency per query and store it in artifacts.
   - Save under `./runs/<timestamp>/` (plan.json, query.sql, results.json, summary.md). Keep latest N (env `RUNS_RETENTION`).
   - For non-SQL analytics (e.g., clustering, correlation tests), save a parameters JSON (method, thresholds, k, scaling, seed, sample sizes, significance levels) and a concise code snippet or pseudo-SQL describing steps.
   - Include explicit notes on missing-value handling and any sampling used.

6. CLI UX
   - Command: `synmax-agent`
   - Non-interactive `--query` and interactive chat loop.
   - Print concise answer first, then show executed SQL and tabular results; print latency.
   - Transparency panels: print `Heuristic:` (rule/trigger and parameters) and `LLM(explain|planner):` (model usage) after the answer.
   - Test visibility: set `SHOW_IT=1` and run with `-s` to see CLI output during tests.

## 5) Non-functional requirements
- Accuracy first; summaries cannot contradict executed results.
- Latency targets (8-core M2, 16GB): deterministic < 5–10s; analytics < 15s typical.
- Resource efficiency: avoid full materialization; operate on projections/aggregates.
- Reliability: input validation, clear error messages, safe fallbacks.

## 6) Architecture
- CLI Orchestrator, Planner (rule-based + LLM explanation), Tools (profile/run_sql/stats/anomaly/corr/cluster), Executor (DuckDB), Reporter (answer+evidence+latency), Utils (schema cache, privacy helpers).
- Data flow: NL query → plan/execute → results → concise answer → LLM explanation → reporter.

## 7) LLM configuration (OpenAI)
- Env: `OPENAI_API_KEY` (optional; enables explanations and future planning). `OPENAI_MODEL` optional.

## 8) Acceptance criteria
- CLI prints a concise answer first for every query; then shows results, executed SQL, and latency.
- Analytics deliver insights with caveats and evidence.
- Artifacts saved per run; only latest N retained by env `RUNS_RETENTION`.
- README documents install, dataset, env, examples, assumptions.
 - Hypothesis generation produces 1–3 plausible, evidence-linked causes with clear caveats and never contradicts executed results.
 - Analytics artifacts capture parameters, seeds, sample sizes, and methods sufficient for reproduction.

## 9) Risks & mitigations
- Performance: strict projection/pushdown; sampled exploration; aggregate-first.
- Spurious correlations: rank by strength and sample size; caveats.
- Scale-up path: partitioned/extern tables and streaming for billion+ rows (future).

## 10) Milestones
- M1: CLI skeleton, dataset path handling, basic profiling.
- M2: Deterministic planner + DuckDB execution + evidence output + latency.
- M3: Analytics (trends, anomalies, correlations, clustering) + concise answers + LLM explanations.
- M3.5: Hypothesis generation and NL planning enhancements (trend parsing, tool parameterization, significance reporting).
- M4: README/Examples and performance tuning.
