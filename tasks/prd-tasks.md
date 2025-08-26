# PRD Tasks

This checklist derives from `docs/PRD.md` and guides implementation order.

## 1) Scaffold agent structure under `./agent/`
Subtasks:
- [x] 1.1 Create package layout: `agent/cli/`, `agent/planner/`, `agent/tools/`, `agent/exec/`, `agent/report/`, `agent/utils/`.
- [x] 1.2 Add `__init__.py` files and placeholders.
- [x] 1.3 Decide on config structure (YAML/JSON/env) and location. (env vars documented; defaults in code)

## 2) Implement CLI `synmax-agent`
Subtasks:
- [x] 2.1 Choose CLI lib (Typer or argparse); scaffold entrypoint.
- [x] 2.2 Add `--path` detection with default search in `./data/` and prompt fallback.
- [x] 2.3 Add `--model`, `--save-run`, `--max-preview-rows`, `--timeout-sec` flags.
- [x] 2.4 Implement interactive loop with `:exit` and help.
- [x] 2.5 Pretty console output using Rich.

## 3) DuckDB executor for Parquet
Subtasks:
- [x] 3.1 Initialize DuckDB connection and safe `read_parquet` wrapper.
- [x] 3.2 Implement projection pruning (select needed columns only).
- [x] 3.3 Implement predicate pushdown for filters.
- [x] 3.4 Add timeout/row-limit guardrails. (defaults via SQL LIMIT; placeholder timeout)
- [x] 3.5 Unit tests on sample Parquet.

## 4) Data profiling tool
Subtasks:
- [x] 4.1 Schema/dtype inference and normalization.
- [x] 4.2 Null rate and basic descriptive stats.
- [x] 4.3 Distinct counts (sampled for high-cardinality) with warnings.
- [x] 4.4 `head`/preview with row limit.
- [x] 4.5 Cache profiling results for planning.

## 5) Deterministic planner (NL → SQL)
Subtasks:
- [x] 5.1 Intent classifier (deterministic vs analytic) prompt and guardrails.
- [x] 5.2 Template library for count, group-by, filters, time windows. (rule patterns implemented)
- [x] 5.3 Column/metric validation against schema cache.
- [x] 5.4 Emit reproducible SQL and parameter bindings.
- [x] 5.5 Error handling: unknown columns, ambiguous terms. (fallback message)

## 6) Evidence reporter
Subtasks:
- [x] 6.1 Compose final answer + evidence package (columns, filters, SQL/code, stats). (summary.md + optional LLM explanation)
- [x] 6.2 Write artifacts to `./runs/<timestamp>/` as `plan.json`, `query.sql`, `results.json/md`.
- [x] 6.3 Enforce privacy (no raw rows to LLM); redact samples. (metadata-only unless ALLOW_LLM_RAW_PREVIEW=1)

## 7) Analytics tools
Subtasks:
- [x] 7.1 Trends: group-by time windows, growth rates, MoM/QoQ summaries.
- [x] 7.2 Anomalies: z-score/IQR; spike detection with thresholds.
- [x] 7.3 Correlations: Pearson/Spearman; significance and warnings.
- [x] 7.4 Clustering: k-means/mini-batch; scaling and silhouette sanity.
  - [x] 7.4.1 Implement k-means over monthly pipeline profiles
  - [x] 7.4.2 Add scaling options and silhouette reporting
- [x] 7.5 Caveat framework: uncertainty and limitations in outputs.

## 8) Privacy guardrails
Subtasks:
- [x] 8.1 Static checks on planner outputs to block raw row leakage. (metadata-only LLM by default)
- [x] 8.2 Allow only schema/stats/aggregates in LLM context. (enforced in llm_explain)
- [x] 8.3 Logging to prove compliance per run. (artifacts include SQL and summary)

## 9) Artifact saving
Subtasks:
- [x] 9.1 Directory layout under `./runs/<ts>/`.
- [x] 9.2 File writers for JSON, SQL, and Markdown summary.
- [x] 9.3 Retention policy and `.gitignore` coverage. (`RUNS_RETENTION` env; gitignore already covers runs/)

## 10) README & examples
Subtasks:
- [x] 10.1 Installation & key setup.
- [x] 10.2 Dataset supply instructions and auto-discovery from `./data/`.
- [x] 10.3 Example queries and expected outputs.
- [x] 10.4 Assumptions and limitations disclosure.

## Dependencies and Parallelization Plan (Swarming)
- Prereqs: 1.1–1.2 must land before active work; 1.3 can proceed in parallel.
- Wave 1 (parallel): 2.1–2.5 (CLI), 3.1–3.5 (DuckDB executor), 4.1–4.5 (profiling).
- Wave 2 (parallel): 5.1–5.2 (planner skeleton), 6.1–6.2 (reporter/artifacts), 8.1–8.3 (privacy checks), 9.1–9.2 (artifact writers).
- Wave 3 (parallel): 5.3–5.5 (planner validation/errors), 7.1–7.4 (analytics modules split by subagent), 9.3 (retention), 10.1–10.3 (docs/examples first pass).
- Finalization: 6.3 (privacy enforcement), 7.5 (caveat framework), 10.4 (limitations), integration test pass.

Dependency edges (summary):
- 1 → {2, 3, 4}
- 3 → {5, 6, 7}
- 4 → {5, 7}
- 2 → {6, 10}
- 6 → {9}
- 8 → release gate

Parallelization notes:
- Assign separate subagents to CLI (2), Executor (3), Profiling (4) in Wave 1.
- Planner (5) can start on stubs while 3/4 finish; switch to real executor once ready.
- Analytics subagents own 7.1 (Trends), 7.2 (Anomalies), 7.3 (Correlations), 7.4 (Clustering) concurrently; share schema cache from (4).
- Reporter (6) and Artifacts (9) can develop concurrently; integrate with privacy checks (8) before release.
- Docs (10) iterate continuously; publish examples after 2/3 are stable; finalize after 7/8/9.
