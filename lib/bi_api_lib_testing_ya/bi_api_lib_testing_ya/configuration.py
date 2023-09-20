from __future__ import annotations

import attr

from bi_defaults.environments import (
    DOMAIN_BI_API_LIB_YA,
    InternalTestingInstallation,
)
from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_configs.connector_availability import (
    ConnectorAvailabilityConfigSettings,
    ConnectorContainerSettings,
    ConnectorSettings,
    SectionSettings,
    TranslatableSettings,
)


CONNECTOR_AVAILABILITY = ConnectorAvailabilityConfigSettings(
    sections=[
        SectionSettings(
            title_translatable=TranslatableSettings(text="section_title-db", domain=DOMAIN_BI_API_LIB_YA),
            connectors=[
                ConnectorContainerSettings(
                    alias="chyt_connectors",
                    title_translatable=TranslatableSettings(text="label_connector-ch_over_yt"),
                    includes=[
                        ConnectorSettings(conn_type="ch_over_yt"),
                        ConnectorSettings(conn_type="ch_over_yt_user_auth"),
                    ],
                ),
                ConnectorSettings(conn_type="clickhouse"),
                ConnectorSettings(conn_type="postgres"),
                ConnectorSettings(conn_type="mysql"),
                ConnectorSettings(conn_type="greenplum"),
                ConnectorSettings(conn_type="mssql"),
                ConnectorSettings(conn_type="oracle"),
                ConnectorSettings(conn_type="bigquery"),
                ConnectorSettings(conn_type="snowflake"),
                ConnectorSettings(conn_type="ydb"),
                ConnectorSettings(conn_type="promql"),
                ConnectorSettings(conn_type="chyt"),
                ConnectorSettings(conn_type="ch_frozen_bumpy_roads"),
                ConnectorSettings(conn_type="ch_frozen_covid"),
                ConnectorSettings(conn_type="ch_frozen_demo"),
                ConnectorSettings(conn_type="ch_frozen_dtp"),
                ConnectorSettings(conn_type="ch_frozen_gkh"),
                ConnectorSettings(conn_type="ch_frozen_samples"),
                ConnectorSettings(conn_type="ch_frozen_transparency"),
                ConnectorSettings(conn_type="ch_frozen_weather"),
                ConnectorSettings(conn_type="ch_frozen_horeca"),
            ],
        ),
        SectionSettings(
            title_translatable=TranslatableSettings(
                text="section_title-files_and_services", domain=DOMAIN_BI_API_LIB_YA
            ),
            connectors=[
                ConnectorSettings(conn_type="file"),
                ConnectorSettings(conn_type="gsheets_v2"),
                ConnectorSettings(conn_type="yq"),
                ConnectorSettings(conn_type="metrika_api"),
                ConnectorSettings(conn_type="appmetrica_api"),
                ConnectorSettings(conn_type="ch_billing_analytics"),
                ConnectorSettings(conn_type="monitoring"),
                ConnectorSettings(conn_type="usage_tracking"),
            ],
        ),
        SectionSettings(
            title_translatable=TranslatableSettings(text="section_title-partner", domain=DOMAIN_BI_API_LIB_YA),
            connectors=[
                ConnectorSettings(conn_type="bitrix24"),
                ConnectorSettings(conn_type="ch_ya_music_podcast_stats"),
                ConnectorSettings(conn_type="moysklad"),
                ConnectorSettings(conn_type="equeo"),
                ConnectorSettings(conn_type="market_couriers"),
                ConnectorSettings(conn_type="schoolbook_journal"),
                ConnectorSettings(conn_type="smb_heatmaps"),
                ConnectorSettings(conn_type="kontur_market"),
            ],
        ),
    ],
)


CONNECTOR_WHITELIST = [
    "clickhouse",
    "postgresql",
    "mysql",
    "greenplum",
    "mssql",
    "oracle",
    "bigquery",
    "snowflake",
    "ydb",
    "promql",
    "chyt",
    "chyt_internal",
    "ch_frozen_bumpy_roads",
    "ch_frozen_covid",
    "ch_frozen_demo",
    "ch_frozen_dtp",
    "ch_frozen_gkh",
    "ch_frozen_samples",
    "ch_frozen_transparency",
    "ch_frozen_weather",
    "ch_frozen_horeca",
    "file",
    "gsheets",
    "gsheets_v2",
    "yq",
    "metrica_api",
    "appmetrica_api",
    "ch_billing_analytics",
    "monitoring",
    "solomon",
    "usage_tracking",
    "bitrix_gds",
    "ch_ya_music_podcast_stats",
    "moysklad",
    "equeo",
    "kontur_market",
    "market_couriers",
    "schoolbook",
    "smb_heatmaps",
    "ch_geo_filtered",
]


@attr.s(kw_only=True)
class BiApiTestEnvironmentConfigurationPrivate(BiApiTestEnvironmentConfiguration):
    connector_availability_settings: ConnectorAvailabilityConfigSettings = attr.ib(default=CONNECTOR_AVAILABILITY)

    dls_host: str = attr.ib(default=InternalTestingInstallation.DATALENS_API_LB_DLS_BASE_URL)
    dls_key: str = attr.ib(default="_tests_dls_api_key_")
