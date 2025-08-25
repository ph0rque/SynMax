# System Architecture & Patterns

## High-level architecture
- CLI entrypoint orchestrates: config, dataset path resolution, and session loop.
- LLM-driven Planner interprets NL queries into a structured plan (intent, columns, filters, metrics, methods).
- Executor runs the plan using DuckDB (preferred) or Polars with Arrow, emphasizing column pruning and predicate pushdown.
- Reporter composes the final answer, evidence (SQL/code, selected columns, filters), and caveats.

## Key patterns
- Toolformer pattern: expose safe tools (profile_data, run_sql, compute_stats, detect_outliers, compute_correlations, cluster_segments) callable by the planner.
- Guardrails: validate SQL, limit row counts for previews, cap compute time, fallback to sampled analysis with disclosure.
- Evidence-first: executed SQL/code is the source of truth; LLM summaries cannot contradict results.
- Schema introspection cache: cache column types/stats to speed up planning.

## Data & execution
- Prefer `DuckDB` for SQL on Parquet; use `read_parquet` with filters and projection.
- For advanced stats, use Polars/NumPy/Scikit-learn on subsets or aggregations.
- Avoid full in-memory loads; operate on grouped/aggregated views.

## Error handling
- Clear exceptions mapped to user-friendly messages.
- When plans fail, return partial results, diagnostics, and next-step suggestions.

## Extensibility
- Add new tools (e.g., join external lookups, feature engineering) behind stable interfaces.
- Swap LLM providers via adapters.
