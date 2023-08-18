from bi_constants.enums import ConnectionType as CT

from bi_api_lib.connector_availability.base import (
    ConnectorAvailabilityConfig,
    Section,
    Connector,
    ConnectorAvailability,
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
from bi_connector_postgresql.core.greenplum.constants import CONNECTION_TYPE_GREENPLUM
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API
from bi_connector_yql.core.ydb.constants import CONNECTION_TYPE_YDB
from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ


CONFIG = ConnectorAvailabilityConfig(
    sections=[
        Section(
            title_translatable=Translatable('section_title-db'),
            connectors=[
                ConnectorContainer(
                    availability=ConnectorAvailability.free,
                    hidden=False,
                    alias='chyt_connectors',
                    title_translatable=Translatable('label_connector-ch_over_yt'),
                    includes=[
                        Connector(conn_type=CT.ch_over_yt, hidden=False),
                        Connector(conn_type=CT.ch_over_yt_user_auth, hidden=False),
                    ],
                ),
                Connector(conn_type=CT.clickhouse, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_POSTGRES, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_MYSQL, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_GREENPLUM, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_MSSQL, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_ORACLE, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_YDB, hidden=False),
                Connector(conn_type=CT.promql, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_BIGQUERY, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_SNOWFLAKE, hidden=False),
            ],
        ),
        Section(
            title_translatable=Translatable('section_title-files_and_services'),
            connectors=[
                Connector(conn_type=CT.file, hidden=False),
                Connector(conn_type=CT.gsheets, hidden=False),
                Connector(conn_type=CT.usage_tracking_ya_team, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_METRICA_API, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_APPMETRICA_API, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_YQ, hidden=False),
            ],
        ),
    ],
)
