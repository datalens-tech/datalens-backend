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
from bi_connector_postgresql.core.greenplum.constants import CONNECTION_TYPE_GREENPLUM
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
                Connector(conn_type=CT.clickhouse, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_POSTGRES, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_MYSQL, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_YDB, hidden=False),
                Connector(conn_type=CT.chyt, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_GREENPLUM, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_MSSQL, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_ORACLE, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_BIGQUERY, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_SNOWFLAKE, hidden=False),
                Connector(conn_type=CT.promql, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_COVID, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_DEMO, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_DTP, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_GKH, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_SAMPLES, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_TRANSPARENCY, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_WEATHER, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_CH_FROZEN_HORECA, hidden=True),
            ],
        ),
        Section(
            title_translatable=Translatable('section_title-files_and_services'),
            connectors=[
                Connector(conn_type=CONNECTION_TYPE_FILE, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_GSHEETS_V2, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_YQ, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_METRICA_API, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_APPMETRICA_API, hidden=False),
                Connector(conn_type=CT.ch_billing_analytics, hidden=False),
                Connector(conn_type=CT.monitoring, hidden=False),
                Connector(conn_type=CT.usage_tracking, hidden=True),
            ],
        ),
        Section(
            title_translatable=Translatable('section_title-partner'),
            connectors=[
                Connector(conn_type=CT.bitrix24, hidden=False),
                Connector(conn_type=CONNECTION_TYPE_CH_YA_MUSIC_PODCAST_STATS, hidden=False),
                Connector(conn_type=CT.moysklad, hidden=False),
                Connector(conn_type=CT.equeo, hidden=True),
                Connector(conn_type=CT.kontur_market, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_MARKET_COURIERS, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_SCHOOLBOOK_JOURNAL, hidden=True),
                Connector(conn_type=CONNECTION_TYPE_SMB_HEATMAPS, hidden=True),
            ],
        ),
    ],
)
