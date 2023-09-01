from bi_constants.enums import ConnectionType as CT

from bi_api_lib.connector_availability.base import (
    ConnectorAvailabilityConfig,
    Section,
    Connector,
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
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.constants import (
    CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import (
    CONNECTION_TYPE_MARKET_COURIERS,
)
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.constants import (
    CONNECTION_TYPE_SCHOOLBOOK_JOURNAL,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import (
    CONNECTION_TYPE_SMB_HEATMAPS,
)
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS,
)
from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_COVID,
)
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_DEMO,
)
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_DTP,
)
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_GKH,
)
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_HORECA,
)
from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_SAMPLES,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY,
)
from bi_connector_bundle_ch_frozen.ch_frozen_weather.core.constants import (
    CONNECTION_TYPE_CH_FROZEN_WEATHER,
)
from bi_connector_yql.core.ydb.constants import CONNECTION_TYPE_YDB
from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ
from bi_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE


CONFIG = ConnectorAvailabilityConfig(
    sections=[
        Section(
            title_translatable=Translatable('section_title-db'),
            connectors=[
                Connector(conn_type=CT.clickhouse),
                Connector(conn_type=CONNECTION_TYPE_POSTGRES),
                Connector(conn_type=CONNECTION_TYPE_MYSQL),
                Connector(conn_type=CONNECTION_TYPE_YDB),
                Connector(conn_type=CT.chyt),
                Connector(conn_type=CONNECTION_TYPE_GREENPLUM),
                Connector(conn_type=CONNECTION_TYPE_MSSQL),
                Connector(conn_type=CONNECTION_TYPE_ORACLE),
                Connector(conn_type=CONNECTION_TYPE_BIGQUERY),
                Connector(conn_type=CONNECTION_TYPE_SNOWFLAKE),
                Connector(conn_type=CT.promql),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_COVID),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_DEMO),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_DTP),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_GKH),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_SAMPLES),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_WEATHER),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_HORECA),
            ],
        ),
        Section(
            title_translatable=Translatable('section_title-files_and_services'),
            connectors=[
                Connector(conn_type=CONNECTION_TYPE_FILE),
                Connector(conn_type=CONNECTION_TYPE_GSHEETS_V2),
                Connector(conn_type=CONNECTION_TYPE_YQ),
                Connector(conn_type=CONNECTION_TYPE_METRICA_API),
                Connector(conn_type=CONNECTION_TYPE_APPMETRICA_API),
                Connector(conn_type=CT.ch_billing_analytics),
                Connector(conn_type=CT.monitoring),
                Connector(conn_type=CT.usage_tracking),
            ],
        ),
        Section(
            title_translatable=Translatable('section_title-partner'),
            connectors=[
                Connector(conn_type=CT.bitrix24),
                Connector(conn_type=CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS),
                Connector(conn_type=CT.moysklad),
                Connector(conn_type=CT.equeo),
                Connector(conn_type=CT.kontur_market),
                Connector(conn_type=CONNECTION_TYPE_MARKET_COURIERS),
                Connector(conn_type=CONNECTION_TYPE_SCHOOLBOOK_JOURNAL),
                Connector(conn_type=CONNECTION_TYPE_SMB_HEATMAPS),
            ],
        ),
    ],
    visible_connectors={
        CT.clickhouse,
        CONNECTION_TYPE_POSTGRES,
        CONNECTION_TYPE_MYSQL,
        CONNECTION_TYPE_YDB,
        CT.chyt,
        CONNECTION_TYPE_GREENPLUM,
        CONNECTION_TYPE_MSSQL,
        CONNECTION_TYPE_ORACLE,
        CONNECTION_TYPE_BIGQUERY,
        CONNECTION_TYPE_SNOWFLAKE,
        CT.promql,
        CONNECTION_TYPE_FILE,
        CONNECTION_TYPE_GSHEETS_V2,
        CONNECTION_TYPE_YQ,
        CONNECTION_TYPE_METRICA_API,
        CONNECTION_TYPE_APPMETRICA_API,
        CT.ch_billing_analytics,
        CT.monitoring,
        CT.bitrix24,
        CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS,
        CT.moysklad,
    },
)
