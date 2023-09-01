from bi_constants.enums import ConnectionType as CT

from bi_api_lib.connector_availability.base import (
    ConnectorAvailabilityConfig,
    Section,
    Connector,
    ConnectorContainer,
)
from bi_api_lib.i18n.localizer import Translatable

# TODO: Remove direct connector dependencies
from bi_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from bi_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API
from bi_connector_yql.core.ydb.constants import CONNECTION_TYPE_YDB
from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ


CONFIG = ConnectorAvailabilityConfig(
    sections=[
        Section(
            title_translatable=Translatable('section_title-db'),
            connectors=[
                ConnectorContainer(
                    alias='chyt_connectors',
                    title_translatable=Translatable('label_connector-ch_over_yt'),
                    includes=[
                        Connector(conn_type=CT.ch_over_yt),
                        Connector(conn_type=CT.ch_over_yt_user_auth),
                    ],
                ),
                Connector(conn_type=CT.clickhouse),
                Connector(conn_type=CONNECTION_TYPE_POSTGRES),
                Connector(conn_type=CONNECTION_TYPE_MYSQL),
                Connector(conn_type=CONNECTION_TYPE_GREENPLUM),
                Connector(conn_type=CONNECTION_TYPE_MSSQL),
                Connector(conn_type=CONNECTION_TYPE_ORACLE),
                Connector(conn_type=CONNECTION_TYPE_YDB),
                Connector(conn_type=CT.promql),
                Connector(conn_type=CONNECTION_TYPE_BIGQUERY),
                Connector(conn_type=CONNECTION_TYPE_SNOWFLAKE),
            ],
        ),
        Section(
            title_translatable=Translatable('section_title-files_and_services'),
            connectors=[
                Connector(conn_type=CT.file),
                Connector(conn_type=CT.gsheets),
                Connector(conn_type=CT.usage_tracking_ya_team),
                Connector(conn_type=CONNECTION_TYPE_METRICA_API),
                Connector(conn_type=CONNECTION_TYPE_APPMETRICA_API),
                Connector(conn_type=CONNECTION_TYPE_YQ),
            ],
        ),
    ],
    visible_connectors={
        CT.ch_over_yt,
        CT.ch_over_yt_user_auth,
        CT.clickhouse,
        CONNECTION_TYPE_POSTGRES,
        CONNECTION_TYPE_MYSQL,
        CONNECTION_TYPE_GREENPLUM,
        CONNECTION_TYPE_MSSQL,
        CONNECTION_TYPE_ORACLE,
        CONNECTION_TYPE_YDB,
        CT.promql,
        CT.file,
        CT.gsheets,
        CT.usage_tracking_ya_team,
        CONNECTION_TYPE_METRICA_API,
        CONNECTION_TYPE_APPMETRICA_API,
    },
)
