"""
Information about the outside context for all environments.

All the classes (instead of dicts) for static checking.
"""

from __future__ import annotations

from enum import IntEnum, unique
from typing import ClassVar, Tuple, Optional, ItemsView, Set, TypeAlias, Union

import attr

from bi_configs.environments import LegacyDefaults, LegacyEnvAliasesMap, BaseInstallationsMap
from bi_defaults import connectors_data as cd
from bi_configs.connector_availability import (
    ConnectorAvailabilityConfigSettings,
    SectionSettings,
    ConnectorSettings,
    TranslatableSettings,
    ConnectorContainerSettings,
)
from bi_configs.enums import EnvType, RedisMode
from bi_constants.api_constants import DLHeadersCommon


class InstallationBase(LegacyDefaults):
    ENV_TYPE: ClassVar[EnvType]


class IAMAwareInstallation:
    # Documentation:
    # https://docs.yandex-team.ru/iam-cookbook/6.appendices/endpoints#resource-manager
    YC_API_ENDPOINT_IAM: ClassVar[str]
    YC_API_ENDPOINT_RM: ClassVar[str]
    YC_AS_ENDPOINT: ClassVar[str]
    YC_SS_ENDPOINT: ClassVar[str]
    YC_TS_ENDPOINT: ClassVar[str]


class CORSAwareInstallation:
    # TODO: move this values to a separate key
    CORS_ALLOWED_ORIGINS: ClassVar[tuple[str, ...]] = ()
    CORS_ALLOWED_HEADERS: ClassVar[tuple[str, ...]] = ()
    CORS_EXPOSE_HEADERS: ClassVar[tuple[str, ...]] = (DLHeadersCommon.REQUEST_ID,)


class CSRFAwareInstallation:
    # TODO: move this values to a separate key
    CSRF_METHODS: ClassVar[tuple[str, ...]] = ('POST', 'PUT', 'DELETE')
    CSRF_HEADER_NAME: ClassVar[str] = DLHeadersCommon.CSRF_TOKEN
    CSRF_TIME_LIMIT: ClassVar[int] = 3600 * 12  # 12 hours


class DataCloudInstallation(
        CORSAwareInstallation,
        CSRFAwareInstallation,
        InstallationBase,
):
    ENV_TYPE: ClassVar[EnvType] = EnvType.dc_any

    CORS_ALLOWED_HEADERS: ClassVar[tuple[str, ...]] = (
        DLHeadersCommon.CONTENT_TYPE,
        DLHeadersCommon.REQUEST_ID,
        DLHeadersCommon.FOLDER_ID,
        DLHeadersCommon.IAM_TOKEN,
        DLHeadersCommon.TENANT_ID,
        DLHeadersCommon.PROJECT_ID,
        DLHeadersCommon.CSRF_TOKEN,
    )

    ENABLE_REGULAR_S3_LC_RULES_CLEANUP: ClassVar[bool] = True

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings] = ConnectorAvailabilityConfigSettings(
        uncategorized=[
            ConnectorSettings(conn_type='clickhouse'),
            ConnectorSettings(conn_type='postgres'),
            ConnectorSettings(conn_type='mysql'),
            ConnectorSettings(conn_type='chyt'),
            ConnectorSettings(conn_type='mssql'),
            ConnectorSettings(conn_type='file'),
            ConnectorSettings(conn_type='promql'),
            ConnectorSettings(conn_type='bigquery'),
            ConnectorSettings(conn_type='snowflake'),
        ],
    )
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse_mdb',
        'postgresql_mdb',
        'mysql_mdb',
        'chyt',
        'mssql',
        'file',
        'promql',
        'bigquery',
        'snowflake',
    ]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse',
        'postgresql_mdb',
        'mysql',
        'chyt',
        'mssql',
        'file',
        'promql',
        'bigquery',
        'snowflake',
    ]


class DataCloudExposedInstallation(IAMAwareInstallation, DataCloudInstallation):
    # IAMAwareInstallation:
    YC_AUTHORIZE_PERMISSION: ClassVar[str]


class DataCloudExposedInstallationTesting(DataCloudExposedInstallation):
    YC_AUTHORIZE_PERMISSION: ClassVar[str] = "datalens.objects.read"

    YC_API_ENDPOINT_IAM: ClassVar[str] = "iam.expose.bi.yadc.io:4283"
    YC_API_ENDPOINT_RM: ClassVar[str] = "iam.expose.bi.yadc.io:4284"
    YC_AS_ENDPOINT: ClassVar[str] = "iam.expose.bi.yadc.io:4286"
    YC_SS_ENDPOINT: ClassVar[str] = "iam.expose.bi.yadc.io:8655"
    YC_TS_ENDPOINT: ClassVar[str] = "iam.expose.bi.yadc.io:4282"


class CommonInstallation(
        IAMAwareInstallation,
        CORSAwareInstallation,
        CSRFAwareInstallation,
        InstallationBase,
):
    """ Everything that is statically defined for YaTeam and YaCloud installations """

    ENV_TYPE: ClassVar[EnvType]

    US_BASE_URL: ClassVar[str]

    DATALENS_API_LB_MAIN_BASE_URL: ClassVar[str]
    DATALENS_API_LB_UPLOADS_BASE_URL: ClassVar[str]
    DATALENS_API_LB_UPLOADS_STATUS_URL: ClassVar[str]
    DATALENS_API_LB_DLS_BASE_URL: ClassVar[str]

    @property
    def US_HOST(self):
        return self.US_BASE_URL

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings]
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]]

    # Note: `PING_…` values are not currently used.
    # Intended for liveness+connectivity check, e.g. into juggler.
    PING_URL_S3: ClassVar[str]
    PING_URL_UNITED_STORAGE: ClassVar[str]
    PING_URL_MATERIALIZER: ClassVar[str]

    REDIS_CACHES_CLUSTER_NAME: ClassVar[str]
    REDIS_CACHES_HOSTS: ClassVar[tuple[str, ...]]
    REDIS_CACHES_PORT: ClassVar[int] = 26379
    REDIS_CACHES_SSL: ClassVar[Optional[bool]] = None

    # Redis CACHES databases:
    #  1 - dataset-api data cache
    #  2 - dataset-api mutations cache
    REDIS_CACHES_DB: ClassVar[int] = 1
    REDIS_MUTATIONS_CACHES_DB: ClassVar[int] = 2

    REDIS_PERSISTENT_MODE: ClassVar[RedisMode] = RedisMode.sentinel
    REDIS_PERSISTENT_CLUSTER_NAME: ClassVar[str]
    REDIS_PERSISTENT_HOSTS: ClassVar[tuple[str, ...]]
    REDIS_PERSISTENT_PORT: ClassVar[int] = 26379
    REDIS_PERSISTENT_SSL: ClassVar[Optional[bool]] = None

    # Redis PERSISTENT databases:
    #  0 - materializer locks
    #  1 - materializer celery
    #  7 - billing celery & locks
    #  8 - test-project-worker ARQ
    #  9 - file-uploader data (RedisModel) & locks
    # 11 - file-uploader task processor (ARQ)
    REDIS_FILE_UPLOADER_DATA_DB: ClassVar[int] = 9
    REDIS_FILE_UPLOADER_TASKS_DB: ClassVar[int] = 11
    REDIS_TEST_PROJECT_WORKER_TASKS_DB: ClassVar[int] = 11

    SENTRY_ENABLED: ClassVar[bool] = True
    SENTRY_DSN_DATASET_API: ClassVar[str]
    SENTRY_DSN_TEST_TASK_PROCESSOR_WORKER: ClassVar[str]
    SENTRY_DSN_FILE_UPLOADER_API: ClassVar[str]
    SENTRY_DSN_FILE_UPLOADER_WORKER: ClassVar[str]

    S3_ENDPOINT_URL: ClassVar[str]
    FILE_UPLOADER_S3_TMP_BUCKET_NAME: ClassVar[str] = 'bi-file-uploader-tmp'
    FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME: ClassVar[str] = 'bi-file-uploader'
    ENABLE_REGULAR_S3_LC_RULES_CLEANUP: ClassVar[bool] = False


class CommonInternalInstallation(
        cd.ConnectorsDataCHYTInternalInstallation,
        cd.ConnectorsDataYQInternalInstallation,
        cd.ConnectorsDataUsageTrackingYaTeamInternalInstallation,
        CommonInstallation
):
    """ YaTeam installation common configuration """

    PING_URL_BLACKBOX: ClassVar[str] = "https://blackbox.yandex-team.ru/ping"

    # IAMAwareInstallation:
    # Using the same for all yateam installations. Interface:
    # https://yc.yandex-team.ru/?tab=service-accounts
    YC_API_ENDPOINT_IAM: ClassVar[str] = 'iamcp.cloud.yandex-team.ru:443'
    YC_API_ENDPOINT_RM: ClassVar[str] = 'rm.cloud.yandex-team.ru:443'
    YC_AS_ENDPOINT: ClassVar[str] = 'as.cloud.yandex-team.ru:4286'
    YC_SS_ENDPOINT: ClassVar[str] = 'ss.cloud.yandex-team.ru:443'
    YC_TS_ENDPOINT: ClassVar[str] = 'ts.cloud.yandex-team.ru:4282'

    CORS_ALLOWED_HEADERS: ClassVar[Tuple[str, ...]] = (
        DLHeadersCommon.CONTENT_TYPE,
        DLHeadersCommon.REQUEST_ID,
        DLHeadersCommon.TENANT_ID,
        DLHeadersCommon.CSRF_TOKEN,
        DLHeadersCommon.ALLOW_SUPERUSER,
        DLHeadersCommon.SUDO,
    )


class InternalTestingInstallation(cd.ConnectorsDataFileIntTesting, CommonInternalInstallation):
    """ datalens-preprod.yandex-team.ru """

    ENV_TYPE: ClassVar[EnvType] = EnvType.int_testing

    US_BASE_URL: ClassVar[str] = "https://united-storage-beta.yandex-team.ru"

    DATALENS_API_LB_MAIN_BASE_URL: ClassVar[str] = "https://back.datalens-beta.yandex-team.ru"
    DATALENS_API_LB_UPLOADS_BASE_URL: ClassVar[str] = "https://upload.datalens-beta.yandex-team.ru"
    DATALENS_API_LB_UPLOADS_STATUS_URL: ClassVar[str] = "https://upload.datalens-beta.yandex-team.ru"
    DATALENS_API_LB_DLS_BASE_URL: ClassVar[str] = "https://dls-int-test.yandex.net"

    PING_URL_S3: ClassVar[str] = "http://s3.mdst.yandex.net/ping"
    PING_URL_UNITED_STORAGE: ClassVar[str] = "https://united-storage-beta.yandex-team.ru/ping"
    PING_URL_MATERIALIZER: ClassVar[str] = "https://back.datalens-beta.yandex-team.ru/materializer/ping"

    REDIS_CACHES_CLUSTER_NAME: ClassVar[str] = 'caches-int-testing'
    REDIS_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'sas-ujxtc4evffxesenx.db.yandex.net',
        'vla-w420dojupq0i9lmy.db.yandex.net',
        'man-zmxng5x68wurj7gv.db.yandex.net',
    )

    REDIS_PERSISTENT_CLUSTER_NAME: ClassVar[str] = 'int-testing'
    REDIS_PERSISTENT_HOSTS: ClassVar[Tuple[str, ...]] = (
        'man-cw8kvrsuv6f5a3mi.db.yandex.net',
        'sas-4jok4qpiwx2dga5t.db.yandex.net',
        'vla-35wm6bgrtqk2ceyb.db.yandex.net'
    )

    SENTRY_DSN_DATASET_API: ClassVar[str] = "https://de140f6b8a2046b6a9a9de26db7d9716@sentry.stat.yandex-team.ru/394"
    SENTRY_DSN_TEST_TASK_PROCESSOR_WORKER: ClassVar[str] = "https://73342d1c7a6b419bb61d584a55ad1920@sentry.stat.yandex-team.ru/546"
    SENTRY_DSN_FILE_UPLOADER_API: ClassVar[str] = "https://fd9661c6d3ff46408b5bf230ae67c503@sentry.stat.yandex-team.ru/548"
    SENTRY_DSN_FILE_UPLOADER_WORKER: ClassVar[str] = "https://4f1a5b79e0f4445bab103aaf312e0b38@sentry.stat.yandex-team.ru/547"

    CORS_ALLOWED_ORIGINS: ClassVar[Tuple[str, ...]] = ('*',)

    S3_ENDPOINT_URL: ClassVar[str] = 'http://s3.mds.yandex.net'
    FILE_UPLOADER_S3_TMP_BUCKET_NAME: ClassVar[str] = 'bi-file-uploader-tmp-testing'
    FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME: ClassVar[str] = 'bi-file-uploader-testing'

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings] = ConnectorAvailabilityConfigSettings(
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
                    ConnectorSettings(conn_type='ydb'),
                    ConnectorSettings(conn_type='promql'),
                    ConnectorSettings(conn_type='bigquery'),
                    ConnectorSettings(conn_type='snowflake'),
                ],
            ),
            SectionSettings(
                title_translatable=TranslatableSettings(text='section_title-files_and_services'),
                connectors=[
                    ConnectorSettings(conn_type='file'),
                    ConnectorSettings(conn_type='gsheets'),
                    ConnectorSettings(conn_type='usage_tracking_ya_team'),
                    ConnectorSettings(conn_type='metrika_api'),
                    ConnectorSettings(conn_type='appmetrica_api'),
                    ConnectorSettings(conn_type='yq'),
                ],
            ),
        ],
    )
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'chyt_internal',
        'clickhouse',
        'postgresql',
        'mysql',
        'greenplum',
        'mssql',
        'oracle',
        'ydb',
        'promql',
        'bigquery',
        'snowflake',

        'file',
        'gsheets',
        'usage_tracking_ya_team',
        'metrica_api',
        'appmetrica_api',
        'yq',
        'solomon',
    ]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'chyt_internal',
        'clickhouse',
        'postgresql',
        'mysql',
        'greenplum',
        'mssql',
        'oracle',
        'ydb',
        'promql',
        'bigquery',
        'snowflake',

        'file',
        'gsheets',
        'usage_tracking_ya_team',
        'metrica_api',
        'appmetrica_api',
        'yq',
        'solomon',
    ]


class InternalProductionInstallation(cd.ConnectorsDataFileIntProduction, CommonInternalInstallation):
    """ datalens.yandex-team.ru """

    ENV_TYPE: ClassVar[EnvType] = EnvType.int_production

    US_BASE_URL: ClassVar[str] = "https://united-storage.yandex-team.ru"

    DATALENS_API_LB_MAIN_BASE_URL: ClassVar[str] = "https://back.datalens.yandex-team.ru"
    DATALENS_API_LB_UPLOADS_BASE_URL: ClassVar[str] = "https://upload.datalens.yandex-team.ru"
    DATALENS_API_LB_UPLOADS_STATUS_URL: ClassVar[str] = "https://upload.datalens.yandex-team.ru"
    DATALENS_API_LB_DLS_BASE_URL: ClassVar[str] = "https://dls-int-prod.yandex.net"

    PING_URL_S3: ClassVar[str] = "http://s3.mds.yandex.net/ping"
    PING_URL_UNITED_STORAGE: ClassVar[str] = "https://united-storage.yandex-team.ru/ping"
    PING_URL_MATERIALIZER: ClassVar[str] = "https://back.datalens.yandex-team.ru/materializer/ping"

    REDIS_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'sas-iulc9b604w3chy2q.db.yandex.net',
        'vla-1wu6mlmdge55n6iu.db.yandex.net',
        'myt-axexufcxe1j8j9as.db.yandex.net',
    )
    REDIS_CACHES_CLUSTER_NAME: ClassVar[str] = 'caches-int-production'

    REDIS_PERSISTENT_CLUSTER_NAME: ClassVar[str] = 'int-prod'
    REDIS_PERSISTENT_HOSTS: ClassVar[tuple[str, ...]] = (
        'man-mmd1gwak92u0g1eb.db.yandex.net',
        'sas-88be0fnhjns81m4j.db.yandex.net',
        'vla-pur84i7hluf0krum.db.yandex.net',
    )

    SENTRY_DSN_DATASET_API: ClassVar[str] = "https://71326f2a7f15410b94d56918c8e42ea8@sentry.stat.yandex-team.ru/396"
    SENTRY_DSN_FILE_UPLOADER_API: ClassVar[str] = "https://7e0c1d1acdab440f8ea879f46cfaffa6@sentry.stat.yandex-team.ru/549"
    SENTRY_DSN_FILE_UPLOADER_WORKER: ClassVar[str] = "https://693332c6fa164c16b16e88d8196da7d5@sentry.stat.yandex-team.ru/550"

    CORS_ALLOWED_ORIGINS: ClassVar[Tuple[str, ...]] = (
        'https://datalens.yandex-team.ru',
        'https://datalens-staging.yandex-team.ru',
    )

    S3_ENDPOINT_URL: ClassVar[str] = 'https://s3.mds.yandex.net'

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings] = ConnectorAvailabilityConfigSettings(
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
                    ConnectorSettings(conn_type='ydb'),
                    ConnectorSettings(conn_type='promql'),
                ],
            ),
            SectionSettings(
                title_translatable=TranslatableSettings(text='section_title-files_and_services'),
                connectors=[
                    ConnectorSettings(conn_type='file'),
                    ConnectorSettings(conn_type='gsheets'),
                    ConnectorSettings(conn_type='usage_tracking_ya_team'),
                    ConnectorSettings(conn_type='metrika_api'),
                    ConnectorSettings(conn_type='appmetrica_api'),
                ],
            ),
        ],
    )
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'chyt_internal',
        'clickhouse',
        'postgresql',
        'mysql',
        'greenplum',
        'mssql',
        'oracle',
        'ydb',
        'promql',
        'bigquery',
        'snowflake',

        'file',
        'gsheets',
        'usage_tracking_ya_team',
        'metrica_api',
        'appmetrica_api',
        'yq',
        'solomon',
    ]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'chyt_internal',
        'clickhouse',
        'postgresql',
        'mysql',
        'greenplum',
        'mssql',
        'oracle',
        'ydb',
        'promql',
        'bigquery',
        'snowflake',

        'file',
        'gsheets',
        'usage_tracking_ya_team',
        'metrica_api',
        'appmetrica_api',
        'yq',
        'solomon',
    ]


class CommonExternalInstallation(
        cd.ConnectorsDataCHYTExternalInstallation,
        cd.ConnectorsDataMusicExternalInstallation,
        cd.ConnectorsDataMarketCouriersExternalInstallation,
        cd.ConnectorsDataSMBHeatmapsExternalInstallation,
        cd.ConnectorsDataEqueoExternalInstallation,
        cd.ConnectorsDataKonturMarketExternalInstallation,
        CommonInstallation,
):
    """ YaCloud installation common configuration """

    YC_AUTHORIZE_PERMISSION: ClassVar[str] = "datalens.instances.use"

    YC_DEPLOYMENT_CLOUD_ID: ClassVar[str]
    YC_DEPLOYMENT_FOLDER_ID: ClassVar[str]

    BLACKBOX_NAME: ClassVar[str]

    SENTRY_DSN_PUBLIC_DATASET_API: ClassVar[str]
    SENTRY_DSN_SEC_EMBEDS_DATASET_API: ClassVar[str]

    REDIS_RQE_CACHES_CLUSTER_NAME: ClassVar[str] = 'rqecaches'
    REDIS_RQE_CACHES_HOSTS: ClassVar[Tuple[str, ...]]
    REDIS_RQE_CACHES_PORT: ClassVar[int] = 6379
    REDIS_RQE_CACHES_SSL: ClassVar[Optional[bool]] = None
    REDIS_RQE_CACHES_DB: ClassVar[int] = 1

    CORS_ALLOWED_HEADERS: ClassVar[tuple[str, ...]] = (
        DLHeadersCommon.CONTENT_TYPE,
        DLHeadersCommon.REQUEST_ID,
        DLHeadersCommon.FOLDER_ID,
        DLHeadersCommon.IAM_TOKEN,
        DLHeadersCommon.TENANT_ID,
        DLHeadersCommon.CSRF_TOKEN,
    )

    ENABLE_REGULAR_S3_LC_RULES_CLEANUP: ClassVar[bool] = True
    YC_MDB_API_ENDPOINT: ClassVar[str]


class ExternalTestingInstallation(
        cd.ConnectorsDataBillingExtTesting,
        cd.ConnectorsDataFileExtTesting,
        cd.ConnectorsDataMonitoringExtTesting,
        cd.ConnectorsDataMoyskladExtTesting,
        cd.ConnectorsDataMusicExtTesting,
        cd.ConnectorsDataMarketCouriersExtTesting,
        cd.ConnectorsDataSMBHeatmapsExtTesting,
        cd.ConnectorsDataSchoolbookExtTesting,
        cd.ConnectorsDataYQExtTesting,
        cd.ConnectorsDataUsageTrackingExtTesting,
        CommonExternalInstallation,
):
    """ datalens-preprod.yandex.ru """

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings] = ConnectorAvailabilityConfigSettings(
        sections=[
            SectionSettings(
                title_translatable=TranslatableSettings(text='section_title-db'),
                connectors=[
                    ConnectorSettings(conn_type='clickhouse'),
                    ConnectorSettings(conn_type='postgres'),
                    ConnectorSettings(conn_type='mysql'),
                    ConnectorSettings(conn_type='ydb'),
                    ConnectorSettings(conn_type='chyt'),
                    ConnectorSettings(conn_type='greenplum'),
                    ConnectorSettings(conn_type='mssql'),
                    ConnectorSettings(conn_type='oracle'),
                    ConnectorSettings(conn_type='bigquery'),
                    ConnectorSettings(conn_type='snowflake'),
                    ConnectorSettings(conn_type='promql'),
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
                    ConnectorSettings(conn_type='kontur_market'),
                    ConnectorSettings(conn_type='market_couriers'),
                    ConnectorSettings(conn_type='schoolbook_journal'),
                    ConnectorSettings(conn_type='smb_heatmaps'),
                ],
            ),
        ],
    )
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse_mdb',
        'postgresql_mdb',
        'mysql_mdb',
        'ydb',
        'chyt',
        'greenplum_mdb',
        'mssql',
        'oracle',
        'bigquery',
        'snowflake',
        'promql',

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
        'gsheets_v2',
        'yq',
        'metrica_api',
        'appmetrica_api',
        'ch_billing_analytics',
        'monitoring',
        'usage_tracking',

        'bitrix_gds',
        'ch_ya_music_podcast_stats',
        'moysklad',
        'equeo',
        'kontur_market',
        'market_couriers',
        'schoolbook',
        'smb_heatmaps',
    ]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse',
        'postgresql_mdb',
        'mysql',
        'ydb',
        'chyt',
        'greenplum_mdb',
        'mssql',
        'oracle',
        'bigquery',
        'snowflake',
        'promql',

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
        'gsheets_v2',
        'yq',
        'metrica_api',
        'appmetrica_api',
        'ch_billing_analytics',
        'monitoring',
        'usage_tracking',

        'bitrix_gds',
        'ch_ya_music_podcast_stats',
        'moysklad',
        'equeo',
        'kontur_market',
        'market_couriers',
        'schoolbook',
        'smb_heatmaps',
    ]

    ENV_TYPE: ClassVar[EnvType] = EnvType.yc_testing

    US_BASE_URL: ClassVar[str] = "https://us-dl.private-api.ycp.cloud-preprod.yandex.net"

    DATALENS_API_LB_MAIN_BASE_URL: ClassVar[str] = "https://back.datalens.cloud-preprod.yandex.net"
    DATALENS_API_LB_UPLOADS_BASE_URL: ClassVar[str] = "https://upload.datalens-preprod.yandex.ru"
    DATALENS_API_LB_UPLOADS_STATUS_URL: ClassVar[str] = "https://back.datalens.cloud-preprod.yandex.net/file-uploader"
    DATALENS_API_LB_DLS_BASE_URL: ClassVar[str] = "https://dls.datalens.cloud-preprod.yandex.net"

    YC_BILLING_HOST = 'https://billing.private-api.cloud-preprod.yandex.net:16465'

    PING_URL_S3: ClassVar[str] = "http://s3.mdst.yandex.net/ping"
    PING_URL_UNITED_STORAGE: ClassVar[str] = "https://us-dl.private-api.ycp.cloud-preprod.yandex.net"
    PING_URL_MATERIALIZER: ClassVar[str] = "https://back.datalens.cloud-preprod.yandex.net/materializer/ping"

    REDIS_CACHES_CLUSTER_NAME: ClassVar[str] = 'caches'
    REDIS_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'rc1b-3kj6zsk0l243g6m9.mdb.cloud-preprod.yandex.net',
    )
    REDIS_CACHES_PORT: ClassVar[int] = 26379

    REDIS_PERSISTENT_CLUSTER_NAME: ClassVar[str] = 'misc'
    REDIS_PERSISTENT_HOSTS: ClassVar[Tuple[str, ...]] = (
        'rc1a-xz1b4vdkidicxyi0.mdb.cloud-preprod.yandex.net',
    )

    SENTRY_DSN_DATASET_API: ClassVar[str] = "https://c46c94cd48a846ed8f4d7af83258ca9f@sentry.stat.yandex-team.ru/393"
    SENTRY_DSN_PUBLIC_DATASET_API: ClassVar[str] = "https://3be7025678354200848954c3c2909ab8@sentry.stat.yandex-team.ru/432"
    SENTRY_DSN_SEC_EMBEDS_DATASET_API: ClassVar[str] = "https://bf28348c503f4638b3a550714e04058c@sentry.stat.yandex-team.ru/584"
    SENTRY_DSN_FILE_UPLOADER_API: ClassVar[str] = "https://c501b388e7884ad2a6f1ffbf85cd5a85@sentry.stat.yandex-team.ru/553"
    SENTRY_DSN_FILE_UPLOADER_WORKER: ClassVar[str] = "https://ef57d44f4c534367a85a5ebd95481a72@sentry.stat.yandex-team.ru/551"

    YC_DEPLOYMENT_CLOUD_ID: ClassVar[str] = 'aoee4gvsepbo0ah4i2j6'
    YC_DEPLOYMENT_FOLDER_ID: ClassVar[str] = 'aoevv1b69su5144mlro3'

    BLACKBOX_NAME: ClassVar[str] = 'Mimino'

    # IAMAwareInstallation:
    YC_API_ENDPOINT_IAM: ClassVar[str] = "iam.private-api.cloud-preprod.yandex.net:4283"
    YC_API_ENDPOINT_RM: ClassVar[str] = "rm.private-api.cloud-preprod.yandex.net:4284"
    YC_AS_ENDPOINT: ClassVar[str] = "as.private-api.cloud-preprod.yandex.net:4286"
    YC_SS_ENDPOINT: ClassVar[str] = "ss.private-api.cloud-preprod.yandex.net:8655"
    YC_TS_ENDPOINT: ClassVar[str] = "ts.private-api.cloud-preprod.yandex.net:4282"

    YC_MDB_API_ENDPOINT: ClassVar[str] = 'mdb-internal-api.private-api.cloud-preprod.yandex.net:443'

    CORS_ALLOWED_ORIGINS: ClassVar[Tuple[str, ...]] = ('*',)

    S3_ENDPOINT_URL: ClassVar[str] = 'https://storage.cloud-preprod.yandex.net'

    REDIS_RQE_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'rc1b-zbbvgpj1d83x05qq.mdb.cloud-preprod.yandex.net',
    )


class ExternalProductionInstallation(
        cd.ConnectorsDataBillingExtProduction,
        cd.ConnectorsDataMonitoringExtProduction,
        cd.ConnectorsDataMoyskladExtProduction,
        cd.ConnectorsDataSchoolbookExtProduction,
        cd.ConnectorsDataYQExtProduction,
        cd.ConnectorsDataFileExtProduction,
        cd.ConnectorsDataCHFrozenBumpyRoadsExtProduction,
        cd.ConnectorsDataCHFrozenCovidExtProduction,
        cd.ConnectorsDataCHFrozenDemoExtProduction,
        cd.ConnectorsDataCHFrozenDTPExtProduction,
        cd.ConnectorsDataCHFrozenGKHExtProduction,
        cd.ConnectorsDataCHFrozenSamplesExtProduction,
        cd.ConnectorsDataCHFrozenTransparencyExtProduction,
        cd.ConnectorsDataCHFrozenWeatherExtProduction,
        cd.ConnectorsDataCHFrozenHorecaExtProduction,
        CommonExternalInstallation,
):
    """ datalens.yandex.ru """

    ENV_TYPE: ClassVar[EnvType] = EnvType.yc_production

    US_BASE_URL: ClassVar[str] = "https://us.datalens-front.cloud.yandex.net"

    DATALENS_API_LB_MAIN_BASE_URL: ClassVar[str] = "https://datalens-back.private-api.ycp.cloud.yandex.net"
    DATALENS_API_LB_UPLOADS_BASE_URL: ClassVar[str] = "https://upload.datalens.yandex.ru"
    DATALENS_API_LB_UPLOADS_STATUS_URL: ClassVar[str] = "https://datalens-back.private-api.ycp.cloud.yandex.net/file-uploader"
    DATALENS_API_LB_DLS_BASE_URL: ClassVar[str] = "https://datalens-dls.private-api.ycp.cloud.yandex.net"

    YC_BILLING_HOST = 'https://billing.private-api.cloud.yandex.net:16465'

    PING_URL_S3: ClassVar[str] = "http://s3.mds.yandex.net/ping"
    PING_URL_UNITED_STORAGE: ClassVar[str] = "https://us.datalens-front.cloud.yandex.net/ping"
    PING_URL_MATERIALIZER: ClassVar[str] = "https://datalens-back.private-api.ycp.cloud.yandex.net/materializer/ping"

    REDIS_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'rc1a-3mb8d995j7n0nr2w.mdb.yandexcloud.net',
        'rc1b-w6rvvkk4ac2k1c4d.mdb.yandexcloud.net',
        'rc1c-1kyeib07e4kfvm0o.mdb.yandexcloud.net',
    )
    REDIS_CACHES_CLUSTER_NAME: ClassVar[str] = 'caches'

    REDIS_PERSISTENT_CLUSTER_NAME: ClassVar[str] = 'misc'
    REDIS_PERSISTENT_HOSTS: ClassVar[tuple[str, ...]] = (
        'rc1a-lwqg51su4cza8tqi.mdb.yandexcloud.net',
        'rc1b-kht2xlpj97oo59l1.mdb.yandexcloud.net',
        'rc1c-a5xnif9j1pxabwkp.mdb.yandexcloud.net',
    )

    SENTRY_DSN_DATASET_API: ClassVar[str] = "https://81e94791c492427f837c91dcb3299ad8@sentry.stat.yandex-team.ru/395"
    SENTRY_DSN_PUBLIC_DATASET_API: ClassVar[str] = "https://d0938ee104bf4f1bbb5b62ee46f0761d@sentry.stat.yandex-team.ru/469"
    SENTRY_DSN_SEC_EMBEDS_DATASET_API: ClassVar[str] = "https://d9549b355056427c8d67271ff8b6fd0e@sentry.stat.yandex-team.ru/585"
    SENTRY_DSN_FILE_UPLOADER_API: ClassVar[str] = "https://3e212fd053dd43f7a4fdb870f17183fb@sentry.stat.yandex-team.ru/554"
    SENTRY_DSN_FILE_UPLOADER_WORKER: ClassVar[str] = "https://af57c891d2a4479e8db7d392a45986ff@sentry.stat.yandex-team.ru/552"

    YC_DEPLOYMENT_CLOUD_ID: ClassVar[str] = 'b1g08s4su5tgce7cpeo5'
    YC_DEPLOYMENT_FOLDER_ID: ClassVar[str] = 'b1g77mbejmj4m6flq848'

    BLACKBOX_NAME: ClassVar[str] = 'Prod'

    # IAMAwareInstallation:
    YC_API_ENDPOINT_IAM: ClassVar[str] = "iam.private-api.cloud.yandex.net:4283"
    YC_API_ENDPOINT_RM: ClassVar[str] = "rm.private-api.cloud.yandex.net:4284"
    YC_AS_ENDPOINT: ClassVar[str] = "as.private-api.cloud.yandex.net:4286"
    YC_SS_ENDPOINT: ClassVar[str] = "ss.private-api.cloud.yandex.net:8655"
    YC_TS_ENDPOINT: ClassVar[str] = "ts.private-api.cloud.yandex.net:4282"

    YC_MDB_API_ENDPOINT: ClassVar[str] = 'mdb-internal-api.private-api.cloud.yandex.net:443'

    CORS_ALLOWED_ORIGINS: ClassVar[Tuple[str, ...]] = (
        'https://datalens.yandex.ru',
        'https://datalens.yandex.com',
        'https://datalens-staging.yandex.ru',
    )

    S3_ENDPOINT_URL: ClassVar[str] = 'https://storage.yandexcloud.net'

    REDIS_RQE_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'rc1a-h0c0k98f4p8imglf.mdb.yandexcloud.net',
    )

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings] = ConnectorAvailabilityConfigSettings(
        sections=[
            SectionSettings(
                title_translatable=TranslatableSettings(text='section_title-db'),
                connectors=[
                    ConnectorSettings(conn_type='clickhouse'),
                    ConnectorSettings(conn_type='postgres'),
                    ConnectorSettings(conn_type='mysql'),
                    ConnectorSettings(conn_type='ydb'),
                    ConnectorSettings(conn_type='chyt'),
                    ConnectorSettings(conn_type='greenplum'),
                    ConnectorSettings(conn_type='mssql'),
                    ConnectorSettings(conn_type='oracle'),
                    ConnectorSettings(conn_type='bigquery'),
                    ConnectorSettings(conn_type='snowflake'),
                    ConnectorSettings(conn_type='promql'),
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
                    ConnectorSettings(conn_type='kontur_market'),
                    ConnectorSettings(conn_type='market_couriers'),
                    ConnectorSettings(conn_type='schoolbook_journal'),
                    ConnectorSettings(conn_type='smb_heatmaps'),
                ],
            ),
        ],
    )
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse_mdb',
        'postgresql_mdb',
        'mysql_mdb',
        'greenplum_mdb',
        'ydb',
        'chyt',
        'mssql',
        'oracle',
        'bigquery',
        'snowflake',
        'promql',

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
        'gsheets_v2',
        'yq',
        'metrica_api',
        'appmetrica_api',
        'ch_billing_analytics',
        'monitoring',
        'usage_tracking',

        'bitrix_gds',
        'ch_ya_music_podcast_stats',
        'moysklad',
        'equeo',
        'kontur_market',
        'market_couriers',
        'schoolbook',
        'smb_heatmaps',
    ]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse',
        'postgresql_mdb',
        'mysql',
        'greenplum_mdb',
        'ydb',
        'chyt',
        'mssql',
        'oracle',
        'bigquery',
        'snowflake',
        'promql',

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
        'gsheets_v2',
        'yq',
        'metrica_api',
        'appmetrica_api',
        'ch_billing_analytics',
        'monitoring',
        'usage_tracking',

        'bitrix_gds',
        'ch_ya_music_podcast_stats',
        'moysklad',
        'equeo',
        'kontur_market',
        'market_couriers',
        'schoolbook',
        'smb_heatmaps',
    ]


class NebiusInstallation(InstallationBase):
    """ Base class for all white-lable installations """


class IsraelInstallation(NebiusInstallation):
    ENV_TYPE: ClassVar[EnvType] = EnvType.israel

    # IAMAwareInstallation:
    YC_API_ENDPOINT_IAM: ClassVar[str] = "iam.private-api.yandexcloud.co.il:14283"
    YC_API_ENDPOINT_RM: ClassVar[str] = "rm.private-api.yandexcloud.co.il:14284"
    YC_AS_ENDPOINT: ClassVar[str] = "as.private-api.yandexcloud.co.il:14286"
    YC_SS_ENDPOINT: ClassVar[str] = "ss.private-api.yandexcloud.co.il:18655"
    YC_TS_ENDPOINT: ClassVar[str] = "ts.private-api.yandexcloud.co.il:14282"

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings] = ConnectorAvailabilityConfigSettings(
        uncategorized=[
            ConnectorSettings(conn_type='clickhouse'),
            ConnectorSettings(conn_type='postgres'),
            ConnectorSettings(conn_type='mysql'),
            ConnectorSettings(conn_type='chyt'),
            ConnectorSettings(conn_type='mssql'),
            ConnectorSettings(conn_type='bigquery'),
            ConnectorSettings(conn_type='file'),
            ConnectorSettings(conn_type='snowflake'),
            ConnectorSettings(conn_type='ch_frozen_demo'),
            ConnectorSettings(conn_type='monitoring'),
        ],
    )
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse_mdb',
        'postgresql_mdb',
        'mysql_mdb',
        'chyt',
        'mssql',
        'bigquery',
        'file',
        'snowflake',
        'ch_frozen_demo',
        'monitoring',
    ]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse',
        'postgresql_mdb',
        'mysql',
        'chyt',
        'mssql',
        'bigquery',
        'file',
        'snowflake',
        'ch_frozen_demo',
        'monitoring',
    ]


class NemaxInstallation(NebiusInstallation):
    ENV_TYPE: ClassVar[EnvType] = EnvType.nemax

    CONNECTOR_AVAILABILITY: ClassVar[ConnectorAvailabilityConfigSettings] = ConnectorAvailabilityConfigSettings(
        uncategorized=[
            ConnectorSettings(conn_type='clickhouse'),
            ConnectorSettings(conn_type='postgres'),
            ConnectorSettings(conn_type='mysql'),
            ConnectorSettings(conn_type='chyt'),
            ConnectorSettings(conn_type='mssql'),
            ConnectorSettings(conn_type='bigquery'),
            ConnectorSettings(conn_type='file'),
            ConnectorSettings(conn_type='snowflake'),
            ConnectorSettings(conn_type='ch_frozen_demo'),
            ConnectorSettings(conn_type='monitoring'),
        ],
    )
    BI_API_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse_mdb',
        'postgresql_mdb',
        'mysql_mdb',
        'chyt',
        'mssql',
        'bigquery',
        'file',
        'snowflake',
        'ch_frozen_demo',
        'monitoring',
    ]
    CORE_CONNECTOR_WHITELIST: ClassVar[list[str]] = [
        'clickhouse',
        'postgresql_mdb',
        'mysql',
        'chyt',
        'mssql',
        'bigquery',
        'file',
        'snowflake',
        'ch_frozen_demo',
        'monitoring',
    ]


AllInstallations: TypeAlias = Union[CommonInstallation, NebiusInstallation, DataCloudInstallation]


class InstallationsMap(BaseInstallationsMap):
    int_testing: ClassVar[CommonInstallation] = InternalTestingInstallation()
    int_prod: ClassVar[CommonInstallation] = InternalProductionInstallation()
    ext_testing: ClassVar[CommonInstallation] = ExternalTestingInstallation()
    ext_prod: ClassVar[CommonInstallation] = ExternalProductionInstallation()
    datacloud: ClassVar[DataCloudInstallation] = DataCloudInstallation()
    israel: ClassVar[IsraelInstallation] = IsraelInstallation()
    nemax: ClassVar[IsraelInstallation] = NemaxInstallation()


class EnvAliasesMap(LegacyEnvAliasesMap):
    # Fill in as you see fit.
    tests: ClassVar[str] = "tests"

    int_beta: ClassVar[str] = "int_testing"
    int_testing: ClassVar[str] = "int_testing"
    int_preprod: ClassVar[str] = "int_testing"

    int_production: ClassVar[str] = "int_prod"
    int_prod: ClassVar[str] = "int_prod"

    testing: ClassVar[str] = "ext_testing"
    ext_testing: ClassVar[str] = "ext_testing"
    ext_preprod: ClassVar[str] = "ext_testing"

    ext_production: ClassVar[str] = "ext_prod"
    production: ClassVar[str] = "ext_prod"
    ext_prod: ClassVar[str] = "ext_prod"

    datacloud: ClassVar[str] = "datacloud"
    datacloud_sec_embeds: ClassVar[str] = "datacloud"

    israel: ClassVar[str] = "israel"
    nemax: ClassVar[str] = "nemax"


@attr.s
class IntegrationTestConfig:
    DATALENS_API_LB_MAIN_BASE_URL: str = attr.ib()
    DATALENS_API_LB_UPLOADS_BASE_URL: str = attr.ib()
    DATALENS_API_LB_UPLOADS_STATUS_URL: str = attr.ib()
    YC_API_ENDPOINT_IAM: str = attr.ib()
    YC_API_ENDPOINT_RM: str = attr.ib()
    YC_AS_ENDPOINT: str = attr.ib()
    YC_TS_ENDPOINT: str = attr.ib()
    DLS_ENABLED: bool = attr.ib(default=False)
    DATALENS_DLS_LB_MAIN_BASE_URL: Optional[str] = attr.ib(default=None)
    WORKBOOK_MGMT_STRATEGY: Optional[str] = attr.ib(default=False)
    TENANT_TYPE: Optional[str] = attr.ib(default=None)
    TENANT_ID: Optional[str] = attr.ib(default=None)
    US_LB_MAIN_BASE_URL: Optional[str] = attr.ib(default=None)

    @staticmethod
    def from_env_vars(env_vars: ItemsView) -> IntegrationTestConfig:
        config_kvs = {k: v for k, v in env_vars if k in IntegrationTestConfig.field_names()}
        return IntegrationTestConfig(**config_kvs)

    @staticmethod
    def field_names() -> Set[str]:
        return {a.name for a in attr.fields(IntegrationTestConfig)}

@unique
class TvmDestination(IntEnum):
    BlackboxProdYateam = 223
    GoZora = 2023123
    GeoSearchTesting = 2008261
    GeoSearchProd = 2001886
    LogbrokerYaTeam = 2001059
    SolomonPre = 2010240
    SolomonProd = 2010242
    SolomonFetcherPre = 2012024
    SolomonFetcherProd = 2012028
