from agent.exec.sql_builder import QueryPlan, Filter, build_sql
from agent.utils.schema_cache import SchemaSnapshot, ColumnInfo

schema = SchemaSnapshot(columns=[
    ColumnInfo('eff_gas_day','DATE'),
    ColumnInfo('pipeline_name','VARCHAR'),
    ColumnInfo('scheduled_quantity','DOUBLE'),
])


def test_build_sum_by_group():
    plan = QueryPlan(
        columns=['pipeline_name'],
        filters=[Filter(column='eff_gas_day', op='BETWEEN', value=['2024-01-01','2024-12-31'])],
        group_by=['pipeline_name'],
        aggregations={'total':'SUM(scheduled_quantity)'},
        order_by=[('total','DESC')],
        limit=5,
    )
    sql, params = build_sql('tests/fixtures/sample.parquet', plan, schema)
    assert 'GROUP BY' in sql
    assert params[0].endswith('sample.parquet')
    assert params[-1] == 5
