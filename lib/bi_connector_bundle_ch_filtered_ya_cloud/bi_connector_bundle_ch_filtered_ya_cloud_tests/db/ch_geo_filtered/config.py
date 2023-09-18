from dl_testing.containers import get_test_container_hostport

DB_NAME = 'test_data'
TABLE_NAME = 'sample'

CONNECTION_PARAMS = dict(
    db_name=DB_NAME,
    host=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).host,
    port=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).port,
    username='datalens',
    password='qwerty',
    mp_product_id='mp_product_test_superstore',
    allowed_tables=[TABLE_NAME],
    data_export_forbidden=True,
)
DOWNLOADABLE_CONNECTION_PARAMS = CONNECTION_PARAMS | dict(
    data_export_forbidden=False,
)
