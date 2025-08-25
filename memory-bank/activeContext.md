# Active Context

## Current focus
- Initialize Memory Bank and draft PRD.
- Choose execution engine defaults that scale to ~25M rows.

Decision: Use DuckDB as the primary engine for Parquet queries; optionally use Polars for specific analytics where beneficial. Always project needed columns and push filters down.

## Next steps
1) Implement CLI skeleton: config, dataset path resolution, and session loop.
2) Implement data profiling: schema, dtypes, basic stats, sample rows (with limits).
3) Build deterministic query planner and executor (SQL generation via templates/AST).
4) Add analytics tools: correlations, clustering (k-means/mini-batch), anomaly detection (z-score/IQR), with caveats.
5) Evidence reporter: include SQL/code, filters, selected columns, and method notes in outputs.
6) README quick start and examples.

## Open questions
- Do we need join support across multiple Parquet files for v1? (Assume no.)
- Which LLM provider should be default if both keys are set? (TBD.)
