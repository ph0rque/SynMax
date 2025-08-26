from agent.planner.rule_planner import parse_simple
from agent.utils.schema_cache import SchemaSnapshot, ColumnInfo

schema = SchemaSnapshot(columns=[
    ColumnInfo('eff_gas_day','DATE'),
    ColumnInfo('state_abb','VARCHAR'),
    ColumnInfo('rec_del_sign','BIGINT'),
    ColumnInfo('pipeline_name','VARCHAR'),
    ColumnInfo('scheduled_quantity','DOUBLE'),
])


def test_detect_anomalies_vs_category():
    q = "identify anomalous points that behave outside of their point categories in 2024 state TX"
    res = parse_simple(q, schema)
    assert res.intent == 'analytic'
    assert res.special and res.special['type'] == 'anomalies_vs_category'


def test_sum_by_month():
    q = "sum scheduled_quantity by month in 2024"
    res = parse_simple(q, schema)
    assert res.plan is not None


def test_unknown_column_suggestions():
    q = "top 5 pipline_name by scheduled_quantity"  # misspelled
    res = parse_simple(q, schema)
    assert res.intent == 'unknown'
    assert res.suggestions and any('pipeline_name' in s for s in res.suggestions)
