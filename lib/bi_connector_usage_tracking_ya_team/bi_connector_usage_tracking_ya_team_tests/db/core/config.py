from bi_connector_usage_tracking_ya_team.core.settings import UsageTrackingYaTeamConnectionSettings


SR_CONNECTION_TABLE_NAME = "sample"
SR_CONNECTION_SETTINGS = UsageTrackingYaTeamConnectionSettings(
    DB_NAME="test_data",
    HOST="localhost",
    PORT=52204,
    USERNAME="datalens",
    PASSWORD="qwerty",
    USE_MANAGED_NETWORK=True,
    ALLOWED_TABLES=[],
    SUBSELECT_TEMPLATES=(
        {"title": SR_CONNECTION_TABLE_NAME, "sql_query": "SELECT 'user_id' as user_id WHERE user_id = :user_id"},
    ),
)
