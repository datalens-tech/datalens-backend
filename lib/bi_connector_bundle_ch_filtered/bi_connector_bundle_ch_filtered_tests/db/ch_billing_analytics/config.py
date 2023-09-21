from dl_testing.containers import get_test_container_hostport

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.settings import BillingConnectorSettings


SR_CONNECTION_DB_NAME = "test_data"
SR_CONNECTION_TABLE_NAME = "sample"
SR_CONNECTION_SETTINGS = BillingConnectorSettings(
    SECURE=False,
    DB_NAME=SR_CONNECTION_DB_NAME,
    HOST=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).host,
    PORT=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).port,
    USERNAME="datalens",
    PASSWORD="qwerty",
    USE_MANAGED_NETWORK=False,
    ALLOWED_TABLES=[],
    SUBSELECT_TEMPLATES=(
        {
            "title": SR_CONNECTION_TABLE_NAME,
            "sql_query": "SELECT 'some_ba_id' as billing_account_id "
            "WHERE billing_account_id IN :billing_account_id_list",
        },
    ),
)
