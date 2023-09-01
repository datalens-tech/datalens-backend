from bi_constants.enums import ConnectionType as CT

from bi_api_lib.connector_availability.base import (
    ConnectorAvailabilityConfig,
    Connector,
)

# TODO: Remove direct connector dependencies
from bi_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import CONNECTION_TYPE_CH_FROZEN_DEMO


CONFIG = ConnectorAvailabilityConfig(
    uncategorized=[
        Connector(conn_type=CT.clickhouse),
        Connector(conn_type=CONNECTION_TYPE_POSTGRES),
        Connector(conn_type=CONNECTION_TYPE_MYSQL),
        Connector(conn_type=CT.chyt),
        Connector(conn_type=CONNECTION_TYPE_MSSQL),
        Connector(conn_type=CONNECTION_TYPE_BIGQUERY),
        Connector(conn_type=CT.file),
        Connector(conn_type=CONNECTION_TYPE_SNOWFLAKE),
        Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_DEMO),
        Connector(conn_type=CT.monitoring),
        Connector(conn_type=CT.chyt),
    ],
    visible_connectors={
        CT.clickhouse,
        CONNECTION_TYPE_POSTGRES,
        CONNECTION_TYPE_MYSQL,
        CONNECTION_TYPE_MSSQL,
        CONNECTION_TYPE_BIGQUERY,
        CONNECTION_TYPE_SNOWFLAKE,
        CT.chyt,
    },
)
