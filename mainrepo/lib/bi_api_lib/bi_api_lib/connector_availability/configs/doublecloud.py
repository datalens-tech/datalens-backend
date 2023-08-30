from bi_constants.enums import ConnectionType as CT

from bi_api_lib.connector_availability.base import (
    ConnectorAvailabilityConfig,
    Connector,
)

# TODO: Remove direct connector dependencies
from bi_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE


CONFIG = ConnectorAvailabilityConfig(
    uncategorized=[
        Connector(conn_type=CT.clickhouse, hidden=False),
        Connector(conn_type=CONNECTION_TYPE_POSTGRES, hidden=False),
        Connector(conn_type=CONNECTION_TYPE_MYSQL, hidden=False),
        Connector(conn_type=CT.chyt, hidden=True),
        Connector(conn_type=CONNECTION_TYPE_MSSQL, hidden=False),
        Connector(conn_type=CT.file, hidden=False),
        Connector(conn_type=CT.promql, hidden=False),
        Connector(conn_type=CONNECTION_TYPE_BIGQUERY, hidden=False),
        Connector(conn_type=CONNECTION_TYPE_SNOWFLAKE, hidden=False),
    ],
)
