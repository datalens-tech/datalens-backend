"""
Information about the outside context for all environments.

All the classes (instead of dicts) for static checking.
"""

from __future__ import annotations

from enum import Enum, IntEnum, unique
from typing import ClassVar, Tuple, Dict, Optional, ItemsView, Set, TypeAlias, Union

import attr

from bi_configs import connectors_data as cd
from bi_configs.enums import EnvType, RedisMode
from bi_constants.api_constants import DLHeadersCommon


@unique
class YAVSecretsMap(Enum):
    bi_analytics = "sec-01d8rb402ftm1ppxgafc1eea4b"
    datalens_creds_int_testing = "sec-01dc245cgq8jprkct8ybrp9ffx"
    datalens_creds_int_production = "sec-01dc4kgz3ehzxkxdw1z48rp0b2"
    datalens_creds_ext_testing = "sec-01dc24ppx88wd8wjawzp3zwxcg"
    datalens_creds_ext_production = "sec-01dc4kwx555c9cby0rkmv50eym"
    datalens_creds_israel = "sec-01g7f9bepxj47d3n90s9qysvv3"
    dl_billing_master_token = "sec-01dns6vh0wrmteebhk0er0q3x2"
    dl_ext_csrf_key = "sec-01cramk07fz80xzkj80f4nkd82"
    ext_prod_materializer_master_token = "sec-01e126dy1wp4zr0sw9n2t2xf2z"
    ext_testing_materializer_master_token = "sec-01e0wqwn3yyem626wggb9dc0sk"
    robot_datalens_back = "sec-01ekmt2r5p50hp5qw875dewfgw"
    statinfra_sentry = "sec-01ejre46ykgb69dy7ec43xrsky"

    #  2021163  Datalens Backend dev/tests  https://abc.yandex-team.ru/services/yandexbi/resources/?show-resource=20620547
    tvm_dev = "sec-01ec8d1x4ckxq3h1byrh7wk5ax"
    #  2017223  Datalens Backend Internal Testing  https://abc.yandex-team.ru/services/yandexbi/resources/?show-resource=8913719
    tvm_int_testing = "sec-01dv8x203ws5kw7dhdxdynmqke"  # "tvm.secret.2017223"
    #  2017227  Datalens Backend Internal Production  https://abc.yandex-team.ru/services/yandexbi/resources/?show-resource=8913746
    tvm_int_production = "sec-01dv8x5eye1hw18x3akg4b2gb7"  # "tvm.secret.2017223"
    #  2017225  Datalens Backend Testing  https://abc.yandex-team.ru/services/yandexbi/resources/?show-resource=8913721
    tvm_ext_testing = "sec-01dv8x3s847wpz1mjtcgmfzgv7"  # "tvm.secret.2017225"
    #  2017229  Datalens Backend Production  https://abc.yandex-team.ru/services/yandexbi/resources/?show-resource=8913747
    tvm_ext_prod = "sec-01dv8x5xs7jv49pq7fam9d4zx7"  # "tvm.secret.2017229"

    us_master_token = "sec-01csc2x33km5dc7x8p51hfh0nj"
    us_public_token = "sec-01e7ns911m3gez46t6zqb0z5hb"

    # tests:
    datalens_test_data = "sec-01d5pcrfv9xceanxj2jwaed4bm"


class InstallationBase:
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
    CORS_ALLOWED_ORIGINS: ClassVar[tuple[str, ...]] = ()
    CORS_ALLOWED_HEADERS: ClassVar[tuple[str, ...]] = ()
    CORS_EXPOSE_HEADERS: ClassVar[tuple[str, ...]] = (DLHeadersCommon.REQUEST_ID,)


class CSRFAwareInstallation:
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

    DL_CRY_ACTUAL_KEY_ID: ClassVar[str] = "0"
    ENV_VAULT_ID: ClassVar[str]
    DL_CRY_KEYMAP: ClassVar[Dict[str, str]] = {}

    DATALENS_API_LB_MAIN_BASE_URL: ClassVar[str]
    DATALENS_API_LB_UPLOADS_BASE_URL: ClassVar[str]
    DATALENS_API_LB_UPLOADS_STATUS_URL: ClassVar[str]
    DATALENS_API_LB_DLS_BASE_URL: ClassVar[str]

    @property
    def US_HOST(self):
        return self.US_BASE_URL

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

    DL_CRY_ACTUAL_KEY_ID = "int_preprod_1"
    DL_CRY_KEYMAP: ClassVar[Dict[str, str]] = {"1": "int_preprod_1"}
    US_MASTER_SECRET_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.us_master_token.value, "int-testing")
    ENV_VAULT_ID: ClassVar[str] = YAVSecretsMap.datalens_creds_int_testing.value

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
    REDIS_CACHES_PASSWORD_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.datalens_creds_int_testing.value, "REDIS_CACHES_PASSWORD")

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


class InternalProductionInstallation(cd.ConnectorsDataFileIntProduction, CommonInternalInstallation):
    """ datalens.yandex-team.ru """

    ENV_TYPE: ClassVar[EnvType] = EnvType.int_production

    US_BASE_URL: ClassVar[str] = "https://united-storage.yandex-team.ru"

    DL_CRY_ACTUAL_KEY_ID = "int_prod_1"
    DL_CRY_KEYMAP: ClassVar[Dict[str, str]] = {"1": "int_prod_1"}
    US_MASTER_SECRET_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.us_master_token.value, "int-production")
    ENV_VAULT_ID: ClassVar[str] = YAVSecretsMap.datalens_creds_int_production.value

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
    REDIS_CACHES_PASSWORD_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.datalens_creds_int_production.value, "REDIS_CACHES_PASSWORD")

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
    REDIS_RQE_CACHES_PASSWORD_YAV: ClassVar[Tuple[str, str]]
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


class ExternalTestingInstallation(
        cd.ConnectorsDataBillingExtTesting,
        cd.ConnectorsDataBitrixExtTesting,
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

    ENV_TYPE: ClassVar[EnvType] = EnvType.yc_testing

    US_BASE_URL: ClassVar[str] = "https://us-dl.private-api.ycp.cloud-preprod.yandex.net"

    DL_CRY_ACTUAL_KEY_ID: ClassVar[str] = "cloud_preprod_3"
    DL_CRY_KEYMAP: ClassVar[Dict[str, str]] = {"3": "cloud_preprod_3"}
    US_MASTER_SECRET_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.us_master_token.value, "ext-testing")
    ENV_VAULT_ID: ClassVar[str] = YAVSecretsMap.datalens_creds_ext_testing.value

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
    REDIS_CACHES_PASSWORD_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.datalens_creds_ext_testing.value, "REDIS_CACHES_PASSWORD")

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

    CORS_ALLOWED_ORIGINS: ClassVar[Tuple[str, ...]] = ('*',)

    S3_ENDPOINT_URL: ClassVar[str] = 'https://storage.cloud-preprod.yandex.net'

    REDIS_RQE_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'rc1b-zbbvgpj1d83x05qq.mdb.cloud-preprod.yandex.net',
    )
    REDIS_RQE_CACHES_PASSWORD_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.datalens_creds_ext_testing.value, "REDIS_RQE_CACHES_PASSWORD")


class ExternalProductionInstallation(
        cd.ConnectorsDataBillingExtProduction,
        cd.ConnectorsDataBitrixExtProduction,
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

    DL_CRY_ACTUAL_KEY_ID: ClassVar[str] = "cloud_prod_3"
    DL_CRY_KEYMAP: ClassVar[Dict[str, str]] = {"3": "cloud_prod_3"}
    US_MASTER_SECRET_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.us_master_token.value, "ext-production")
    ENV_VAULT_ID: ClassVar[str] = YAVSecretsMap.datalens_creds_ext_production.value

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
    REDIS_CACHES_PASSWORD_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.datalens_creds_ext_production.value, "REDIS_CACHES_PASSWORD")

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

    CORS_ALLOWED_ORIGINS: ClassVar[Tuple[str, ...]] = (
        'https://datalens.yandex.ru',
        'https://datalens.yandex.com',
        'https://datalens-staging.yandex.ru',
    )

    S3_ENDPOINT_URL: ClassVar[str] = 'https://storage.yandexcloud.net'

    REDIS_RQE_CACHES_HOSTS: ClassVar[Tuple[str, ...]] = (
        'rc1a-h0c0k98f4p8imglf.mdb.yandexcloud.net',
    )
    REDIS_RQE_CACHES_PASSWORD_YAV: ClassVar[Tuple[str, str]] = (YAVSecretsMap.datalens_creds_ext_production.value, "REDIS_RQE_CACHES_PASSWORD")


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


class NemaxInstallation(NebiusInstallation):
    ENV_TYPE: ClassVar[EnvType] = EnvType.nemax


class TestsInstallation(cd.ConnectorsDataFileIntTesting, CommonInstallation):
    ENV_TYPE: ClassVar[EnvType] = EnvType.int_testing

    REDIS_CACHES_CLUSTER_NAME: ClassVar[str] = None
    REDIS_CACHES_HOSTS: ClassVar[tuple[str, ...]] = None

    SENTRY_ENABLED: ClassVar[bool] = False
    SENTRY_DSN_DATASET_API: ClassVar[str] = ''

    REDIS_PERSISTENT_CLUSTER_NAME: ClassVar[str] = None
    REDIS_PERSISTENT_HOSTS: ClassVar[tuple[str, ...]] = ('127.0.0.1',)
    REDIS_PERSISTENT_PORT: ClassVar[int] = None
    REDIS_PERSISTENT_MODE: ClassVar[RedisMode] = RedisMode.single_host

    CORS_ALLOWED_ORIGINS: ClassVar[tuple[str, ...]] = ('*',)
    CORS_ALLOWED_HEADERS: ClassVar[tuple[str, ...]] = CommonInternalInstallation.CORS_ALLOWED_HEADERS

    S3_ENDPOINT_URL: ClassVar[str] = None

    CONN_FILE_CH_USERNAME: ClassVar[str] = 'user1'

    ENABLE_REGULAR_S3_LC_RULES_CLEANUP: ClassVar[bool] = True

    BLACKBOX_NAME: ClassVar[str] = 'Test'


class BaseInstallationsMap:
    pass


AllInstallations: TypeAlias = Union[CommonInstallation, NebiusInstallation, DataCloudInstallation]


class InstallationsMap(BaseInstallationsMap):
    tests: ClassVar[CommonInstallation] = TestsInstallation()
    int_testing: ClassVar[CommonInstallation] = InternalTestingInstallation()
    int_prod: ClassVar[CommonInstallation] = InternalProductionInstallation()
    ext_testing: ClassVar[CommonInstallation] = ExternalTestingInstallation()
    ext_prod: ClassVar[CommonInstallation] = ExternalProductionInstallation()
    datacloud: ClassVar[DataCloudInstallation] = DataCloudInstallation()
    israel: ClassVar[IsraelInstallation] = IsraelInstallation()
    nemax: ClassVar[IsraelInstallation] = NemaxInstallation()


class EnvAliasesMap:
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


class CommonEnvironmentSettings:
    YENV_TYPE: ClassVar[str]
    YENV_NAME: ClassVar[str]


class CommonInternalEnvironmentSettings(CommonEnvironmentSettings):
    YENV_NAME: ClassVar[str] = "intranet"


class InternalTestingSettings(InternalTestingInstallation, CommonInternalEnvironmentSettings):
    YENV_TYPE: ClassVar[str] = "int-testing"


class InternalProductionSettings(InternalProductionInstallation, CommonInternalEnvironmentSettings):
    YENV_TYPE: ClassVar[str] = "int-production"


class CommonExternalEnvironmentSettings(CommonEnvironmentSettings):
    YENV_NAME: ClassVar[str] = "cloud"


class ExternalTestingSettings(ExternalTestingInstallation, CommonExternalEnvironmentSettings):
    YENV_TYPE: ClassVar[str] = "testing"


class ExternalPublicTestingSettings(ExternalTestingSettings):
    YENV_TYPE: ClassVar[str] = "testing-public"


class ExternalProductionSettings(ExternalProductionInstallation, CommonExternalEnvironmentSettings):
    YENV_TYPE: ClassVar[str] = "production"


class ExternalPublicProductionSettings(ExternalProductionSettings):
    YENV_TYPE: ClassVar[str] = "production-public"


class TestsEnvironmentSettings(TestsInstallation, CommonEnvironmentSettings):
    YENV_NAME: ClassVar[str] = "intranet"
    YENV_TYPE: ClassVar[str] = "tests"


class EnvironmentsSettingsMap:
    # Effectively `settings_cls.YENV_TYPE.replace("-", "_"): settings_cls`
    tests: ClassVar[CommonEnvironmentSettings] = TestsEnvironmentSettings
    int_testing: ClassVar[CommonEnvironmentSettings] = InternalTestingSettings
    int_production: ClassVar[CommonEnvironmentSettings] = InternalProductionSettings
    testing: ClassVar[CommonEnvironmentSettings] = ExternalTestingSettings
    testing_public: ClassVar[CommonEnvironmentSettings] = ExternalPublicTestingSettings
    production: ClassVar[CommonEnvironmentSettings] = ExternalProductionSettings
    production_public: ClassVar[CommonEnvironmentSettings] = ExternalPublicProductionSettings
    # staging: ClassVar[CommonEnvironmentSettings] = ExternalProductionSettings
    # staging_preprod: ClassVar[CommonEnvironmentSettings] = ExternalProductionSettings


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


def entrypoint_get_env_normalized_name():
    import sys
    env_name = sys.argv[1]
    print(getattr(EnvAliasesMap, env_name))


if __name__ == "__main__":  # Example: `python -m bi_configs.addresses int-production`
    entrypoint_get_env_normalized_name()
