# Planned Questions — SynMax Data Agent

This document lists planned questions from simple to complex for validating the agent.

## Level 1 — Basics (deterministic)
- How many rows, columns, and gas-day coverage?
- Distinct counts: `pipeline_name`, `loc_name`, `connecting_pipeline`, `category_short`, `state_abb`.
- Missingness: which columns have highest null rates? Completeness by column.
- Total scheduled volume overall; split by `rec_del_sign` (receipts vs deliveries).
- Top 10 pipelines by total `scheduled_quantity` (and by row count).

## Level 2 — Descriptive breakdowns
- Totals by `category_short`, by `state_abb`, by `county_name`.
- Top 20 `loc_name` by total and by median/p95 `scheduled_quantity`.
- Share of flows by `rec_del_sign` within each `category_short`.
- Top 15 `connecting_pipeline` by total/median `scheduled_quantity`.
- Distribution stats (median, p75, p95, p99) of `scheduled_quantity` per category and per pipeline.

## Level 3 — Time trends
- Daily/monthly totals; 7/30‑day moving averages.
- MoM and YoY growth by pipeline and by category.
- Seasonality by month and by day‑of‑week.
- Top increasing/decreasing pipelines over last 3/6/12 months.
- Regions (`state_abb`) with strongest sustained growth/decline.

## Level 4 — Anomalies and change points
- Spike/drop detection (z‑score/IQR) by pipeline/day and by location/day.
- Structural breaks/change points in key pipelines’ time series.
- Locations with repeated outliers across months (watchlist).
- Net flow anomalies per pipeline/day: sum(`rec_del_sign` * `scheduled_quantity`).
- Sudden composition shifts (category mix change) within a pipeline.

## Level 5 — Relationships and associations
- Correlation of pipeline daily totals (pairwise; rolling windows).
- Association between `category_short` and magnitude distribution (effect sizes).
- Cross‑correlation (lead/lag) between “Production” receipts and “LDC/Power” deliveries.
- State‑level flow co‑movement clusters (correlation matrix on state aggregates).
- Relationship between location activity concentration (Herfindahl) and volatility.

## Level 6 — Segmentation and clustering
- Cluster `loc_name` by flow profile (volume, volatility, seasonality).
- Cluster pipelines by segment mix and variability.
- Segment states/counties by trend/seasonality to find similar regions.
- Identify “hub” interconnects (high degree/volume centrality via `connecting_pipeline`).

## Level 7 — Executive hypotheses (with evidence/caveats)
- Winter vs summer: do LDC deliveries peak in winter and Power in summer? Quantify effect sizes.
- Have interconnect flows shifted from Pipeline A to B since date X? Show before/after.
- Which states are emerging demand centers in the last 12 months? Rank with confidence.
- Are certain categories systematically more volatile? Tie to operational risk.
- Where are persistent net inflows/outflows suggesting supply basins vs demand sinks?

## Level 8 — Data quality and integrity
- Duplicate checks on (`pipeline_name`, `loc_name`, `eff_gas_day`).
- Inconsistent `rec_del_sign` patterns relative to `category_short`.
- Out‑of‑range or improbable `scheduled_quantity` spikes (p99+ factor vs median).
- Lat/long completeness by pipeline/state; geospatial enrichment targets.
- Null patterns over time (data pipeline gaps vs real zeros).

## Level 9 — Operational monitoring candidates
- Top pipelines/locations by variability (std/mean) and by anomaly frequency.
- “Hot” states/counties this month vs trailing quarter (contribution to total delta).
- Weekly anomaly digest: biggest spikes/drops, and segments driving them.
- Stability score per pipeline/location (combines trend, variance, anomalies).
