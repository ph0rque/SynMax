# PRD Tasks

This checklist derives from `docs/PRD.md` and guides implementation order.

## 1) Scaffold agent structure under `./agent/`
Subtasks:
- Create package layout: `agent/cli/`, `agent/planner/`, `agent/tools/`, `agent/exec/`, `agent/report/`, `agent/utils/`.
- Add `__init__.py` files and placeholders.
- Decide on config structure (YAML/JSON/env) and location.

## 2) Implement CLI `synmax-agent`
Subtasks:
- Choose CLI lib (Typer or argparse); scaffold entrypoint.
- Add `--path` detection with default search in `./data/` and prompt fallback.
- Add `--model`, `--save-run`, `--max-preview-rows`, `--timeout-sec` flags.
- Implement interactive loop with `:exit` and help.
- Pretty console output using Rich.

## 3) DuckDB executor for Parquet
Subtasks:
- Initialize DuckDB connection and safe `read_parquet` wrapper.
- Implement projection pruning (select needed columns only).
- Implement predicate pushdown for filters.
- Add timeout/row-limit guardrails.
- Unit tests on sample Parquet.

## 4) Data profiling tool
Subtasks:
- Schema/dtype inference and normalization.
- Null rate and basic descriptive stats.
- Distinct counts (sampled for high-cardinality) with warnings.
- `head`/preview with row limit.
- Cache profiling results for planning.

## 5) Deterministic planner (NL → SQL)
Subtasks:
- Intent classifier (deterministic vs analytic) prompt and guardrails.
- Template library for count, group-by, filters, time windows.
- Column/metric validation against schema cache.
- Emit reproducible SQL and parameter bindings.
- Error handling: unknown columns, ambiguous terms.

## 6) Evidence reporter
Subtasks:
- Compose final answer + evidence package (columns, filters, SQL/code, stats).
- Write artifacts to `./runs/<timestamp>/` as `plan.json`, `query.sql`, `results.json/md`.
- Enforce privacy (no raw rows to LLM); redact samples.

## 7) Analytics tools
Subtasks:
- Trends: group-by time windows, growth rates, MoM/QoQ summaries.
- Anomalies: z-score/IQR; spike detection with thresholds.
- Correlations: Pearson/Spearman; significance and warnings.
- Clustering: k-means/mini-batch; scaling and silhouette sanity.
- Caveat framework: uncertainty and limitations in outputs.

## 8) Privacy guardrails
Subtasks:
- Static checks on planner outputs to block raw row leakage.
- Allow only schema/stats/aggregates in LLM context.
- Logging to prove compliance per run.

## 9) Artifact saving
Subtasks:
- Directory layout under `./runs/<ts>/`.
- File writers for JSON, SQL, and Markdown summary.
- Retention policy and `.gitignore` coverage.

## 10) README & examples
Subtasks:
- Installation & key setup.
- Dataset supply instructions and auto-discovery from `./data/`.
- Example queries and expected outputs.
- Assumptions and limitations disclosure.
