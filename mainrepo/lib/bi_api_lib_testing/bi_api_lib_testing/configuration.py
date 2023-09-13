from __future__ import annotations

from typing import Optional

import attr

from bi_configs.connector_availability import (
    ConnectorAvailabilityConfigSettings,
    SectionSettings,
    TranslatableSettings,
    ConnectorContainerSettings,
    ConnectorSettings,
)
from bi_core_testing.configuration import CoreTestEnvironmentConfigurationBase


CONNECTOR_AVAILABILITY = ConnectorAvailabilityConfigSettings(
    sections=[
        SectionSettings(
            title_translatable=TranslatableSettings(text='section_title-db'),
            connectors=[
                ConnectorContainerSettings(
                    alias='chyt_connectors',
                    title_translatable=TranslatableSettings(text='label_connector-ch_over_yt'),
                    includes=[
                        ConnectorSettings(conn_type='ch_over_yt'),
                        ConnectorSettings(conn_type='ch_over_yt_user_auth'),
                    ],
                ),
                ConnectorSettings(conn_type='clickhouse'),
                ConnectorSettings(conn_type='postgres'),
                ConnectorSettings(conn_type='mysql'),
                ConnectorSettings(conn_type='greenplum'),
                ConnectorSettings(conn_type='mssql'),
                ConnectorSettings(conn_type='oracle'),
                ConnectorSettings(conn_type='bigquery'),
                ConnectorSettings(conn_type='snowflake'),
                ConnectorSettings(conn_type='ydb'),
                ConnectorSettings(conn_type='promql'),
                ConnectorSettings(conn_type='chyt'),
                ConnectorSettings(conn_type='ch_frozen_bumpy_roads'),
                ConnectorSettings(conn_type='ch_frozen_covid'),
                ConnectorSettings(conn_type='ch_frozen_demo'),
                ConnectorSettings(conn_type='ch_frozen_dtp'),
                ConnectorSettings(conn_type='ch_frozen_gkh'),
                ConnectorSettings(conn_type='ch_frozen_samples'),
                ConnectorSettings(conn_type='ch_frozen_transparency'),
                ConnectorSettings(conn_type='ch_frozen_weather'),
                ConnectorSettings(conn_type='ch_frozen_horeca'),
            ],
        ),
        SectionSettings(
            title_translatable=TranslatableSettings(text='section_title-files_and_services'),
            connectors=[
                ConnectorSettings(conn_type='file'),
                ConnectorSettings(conn_type='gsheets_v2'),
                ConnectorSettings(conn_type='yq'),
                ConnectorSettings(conn_type='metrika_api'),
                ConnectorSettings(conn_type='appmetrica_api'),
                ConnectorSettings(conn_type='ch_billing_analytics'),
                ConnectorSettings(conn_type='monitoring'),
                ConnectorSettings(conn_type='usage_tracking'),
            ],
        ),
        SectionSettings(
            title_translatable=TranslatableSettings(text='section_title-partner'),
            connectors=[
                ConnectorSettings(conn_type='bitrix24'),
                ConnectorSettings(conn_type='ch_ya_music_podcast_stats'),
                ConnectorSettings(conn_type='moysklad'),
                ConnectorSettings(conn_type='equeo'),
                ConnectorSettings(conn_type='market_couriers'),
                ConnectorSettings(conn_type='schoolbook_journal'),
                ConnectorSettings(conn_type='smb_heatmaps'),
                ConnectorSettings(conn_type='kontur_market'),
            ],
        ),
    ],
)


CONNECTOR_WHITELIST = [
    'clickhouse',
    'postgresql',
    'mysql',
    'greenplum',
    'mssql',
    'oracle',
    'bigquery',
    'snowflake',
    'ydb',
    'promql',
    'chyt',
    'chyt_internal',

    'ch_frozen_bumpy_roads',
    'ch_frozen_covid',
    'ch_frozen_demo',
    'ch_frozen_dtp',
    'ch_frozen_gkh',
    'ch_frozen_samples',
    'ch_frozen_transparency',
    'ch_frozen_weather',
    'ch_frozen_horeca',

    'file',
    'gsheets',
    'gsheets_v2',
    'yq',
    'metrica_api',
    'appmetrica_api',
    'ch_billing_analytics',
    'monitoring',
    'solomon',
    'usage_tracking',

    'bitrix_gds',
    'ch_ya_music_podcast_stats',
    'moysklad',
    'equeo',
    'kontur_market',
    'market_couriers',
    'schoolbook',
    'smb_heatmaps',
    'ch_geo_filtered',
]


@attr.s(kw_only=True)
class BiApiTestEnvironmentConfiguration:
    core_test_config: CoreTestEnvironmentConfigurationBase = attr.ib()

    ext_query_executer_secret_key: str = attr.ib()

    mutation_cache_enabled: bool = attr.ib(default=True)

    bi_compeng_pg_url: str = attr.ib(default='')

    redis_host: str = attr.ib(default='')
    redis_port: int = attr.ib(default=6379)
    redis_password: str = attr.ib(default='')
    redis_db_default: int = attr.ib(default=0)
    redis_db_cache: int = attr.ib(default=1)
    redis_db_mutation: int = attr.ib(default=2)
    redis_db_arq: int = attr.ib(default=11)

    connector_availability_settings: ConnectorAvailabilityConfigSettings = attr.ib(default=CONNECTOR_AVAILABILITY)
    bi_api_connector_whitelist: Optional[list[str]] = attr.ib(default=CONNECTOR_WHITELIST)
    core_connector_whitelist: Optional[list[str]] = attr.ib(default=CONNECTOR_WHITELIST)
