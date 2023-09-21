from dl_testing.containers import get_test_container_hostport

from bi_connector_usage_tracking_ya_team.core.settings import UsageTrackingYaTeamConnectionSettings


SR_CONNECTION_TABLE_NAME = "sample"
SR_CONNECTION_SETTINGS = UsageTrackingYaTeamConnectionSettings(
    SECURE=False,
    DB_NAME="test_data",
    HOST=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).host,
    PORT=get_test_container_hostport("db-clickhouse-22-10", fallback_port=52204).port,
    USERNAME="datalens",
    PASSWORD="qwerty",
    USE_MANAGED_NETWORK=True,
    ALLOWED_TABLES=[],
    SUBSELECT_TEMPLATES=(
        {"title": SR_CONNECTION_TABLE_NAME, "sql_query": "SELECT 'user_id' as user_id WHERE user_id = :user_id"},
    ),
)
