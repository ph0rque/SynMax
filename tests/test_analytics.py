from agent.exec.duck import DuckDBExecutor
from agent.tools.analytics import daily_totals, anomalies_vs_category


def test_daily_totals_runs():
    ex = DuckDBExecutor()
    tbl = daily_totals(ex, 'tests/fixtures/sample.parquet')
    assert tbl is not None


def test_anomalies_vs_category_runs():
    ex = DuckDBExecutor()
    tbl = anomalies_vs_category(ex, 'tests/fixtures/sample.parquet', z_threshold=2.0, min_anomaly_days=1, year=2024)
    assert tbl is not None
