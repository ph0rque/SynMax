from agent.exec.duck import DuckDBExecutor
from agent.tools.analytics import daily_totals, anomalies_vs_category, correlation_pipelines, cluster_time_series_shapes


def test_daily_totals_runs():
    ex = DuckDBExecutor()
    tbl = daily_totals(ex, 'tests/fixtures/sample.parquet')
    assert tbl is not None


def test_anomalies_vs_category_runs():
    ex = DuckDBExecutor()
    tbl = anomalies_vs_category(ex, 'tests/fixtures/sample.parquet', z_threshold=2.0, min_anomaly_days=1, year=2024)
    assert tbl is not None


def test_correlation_min_obs_guardrail():
    ex = DuckDBExecutor()
    tbl = correlation_pipelines(ex, 'tests/fixtures/sample.parquet', method='pearson', include_pvalue=True, min_obs=1000)
    # With an impossible min_obs, expect empty result but valid schema
    assert tbl is not None
    assert tbl.num_rows == 0


def test_temporal_shape_clustering():
    ex = DuckDBExecutor()
    tbl = cluster_time_series_shapes(ex, 'tests/fixtures/sample.parquet', entity_col='pipeline_name', freq='month', k=2, algorithm='kmeans')
    assert tbl is not None
