# Active Context

## Current focus
- Analytics gap-closure completed: trends (MoM/YoY, moving averages), seasonality, top trending; anomalies (IQR and sudden shifts); correlations (Pearson/Spearman with optional p-values); clustering (KMeans/MiniBatch with scaling, seed). CLI routes and planner directives added. Artifacts now include params, pseudo-code, missing-value handling notes, and optional hypotheses.
- Enforce privacy and save artifacts per run.

Decision: Use DuckDB as the primary engine for Parquet queries; optionally use Polars for specific analytics where beneficial. Always project needed columns and push filters down. Adopt argparse-based CLI with Rich for console output.

## Next steps
1) Performance tuning on large datasets; validate USE_POLARS gains.
2) Extend examples as new datasets/segments are added.

## Open questions
- Do we need join support across multiple Parquet files for v1? (Assume no.)
- Default OpenAI model choice and token budgets.

## Recent decisions
- Repository created as private on GitHub.
- `.env` and `data/` are excluded from version control.
- Directory structure implemented under `./agent/`.
- CLI uses argparse, with Rich for output.
