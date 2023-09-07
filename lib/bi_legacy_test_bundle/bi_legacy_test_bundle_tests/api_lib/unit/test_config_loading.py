from __future__ import annotations

from typing import Dict, Any

import attr
import pytest

from bi_constants.enums import ConnectionType
from bi_configs.environments import (
    ExternalTestingInstallation,
    EnvAliasesMap,
    InstallationsMap,
)
from bi_configs.enums import AppType, RedisMode, EnvType
from bi_configs.crypto_keys import CryptoKeysConfig
from bi_configs.rqe import RQEConfig, RQEBaseURL
from bi_configs.settings_loaders.fallback_cfg_resolver import YEnvFallbackConfigResolver
from bi_configs.settings_loaders.loader_env import EnvSettingsLoader, load_connectors_settings_from_env_with_fallback
from bi_configs.connectors_settings import CHFrozenSamplesConnectorSettings

from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.constants import CONNECTION_TYPE_CH_FROZEN_SAMPLES
from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.settings import ch_frozen_samples_settings_fallback

from bi_core.components.ids import FieldIdGeneratorType
from bi_formula.parser.factory import ParserType

from bi_api_lib.app_settings import (
    ControlPlaneAppSettings, AsyncAppSettings, RedisSettings, CachesTTLSettings, YCAuthSettings, MDBSettings
)
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig


@attr.s(frozen=True, auto_attribs=True)
class ConfigLoadingCase:
    env: Dict[str, str]
    expected_config: Any


SEC_REDIS_PASSWORD = "someRedisPassword"
SEC_DLS_API_KEY = "someDLSAPIkey"
SEC_EXT_QUERY_EXECUTER_SECRET_KEY = "_some_rqe_test_secret_key_"
SEC_US_MASTER_TOKEN = "99_us_mas_tok"
SEC_SENTRY_DSN = "asdsdf@asdf.12341"

SEC_CRY_KEY_0 = "2Lrbo3-cmefoRMylo1ARngCBhfTCdKz2Jv6gjDBGmmw="
SEC_CRY_KEY_cloud_preprod_1 = "zphPI9wTF9raXd0YzLAmY4N1ez3FtXmxhixguRVSiFY="

CLOUD_PRE_PROD_DATA_API_CASE = ConfigLoadingCase(
    env=dict(
        ALLOW_SUBQUERY_IN_PREVIEW='1',
        AUTH_MODE='gauthling',

        BI_COMPENG_PG_URL='postgresql://postgres@%2Fvar%2Frun%2Fpostgresql/postgres',
        BI_FORMULA_PARSER_TYPE='antlr_py',

        CACHES_ON='True',
        CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,

        MDB_DOMAINS='.mdb.cloud-preprod.yandex.net,.mdb.cloud.yandex.net',
        MDB_CNAME_DOMAINS='.rw.mdb.yandex.net',
        MDB_MANAGED_NETWORK_ENABLED='1',
        MDB_MANAGED_NETWORK_REMAP='{".mdb.cloud-preprod.yandex.net": ".db.yandex.net",'
                                  ' ".mdb.cloud.yandex.net": ".db.yandex.net"}',

        MUTATIONS_CACHES_ON='True',
        MUTATIONS_CACHES_DEFAULT_TTL='120',
        MUTATIONS_REDIS_PASSWORD=SEC_REDIS_PASSWORD,

        RQE_CACHES_ON='True',
        RQE_CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,

        DLS_API_KEY=SEC_DLS_API_KEY,

        DL_CRY_ACTUAL_KEY_ID='cloud_preprod_1',
        DL_CRY_FALLBACK_KEY_ID='0',
        DL_CRY_KEY_VAL_ID_0=SEC_CRY_KEY_0,
        DL_CRY_KEY_VAL_ID_cloud_preprod_1=SEC_CRY_KEY_cloud_preprod_1,

        DL_USE_JAEGER_TRACER='1',

        EXT_QUERY_EXECUTER_SECRET_KEY=SEC_EXT_QUERY_EXECUTER_SECRET_KEY,

        QLOUD_TVM_CONFIG='...censored...',
        QLOUD_TVM_TOKEN='...censored...',

        SAMPLES_CH_HOST=(
            'myt-g2ucdqpavskt6irw.db.yandex.net,'
            'sas-1h1276u34g7nt0vx.db.yandex.net,'
            'sas-t2pl126yki67ztly.db.yandex.net,'
            'vla-1zwdkyy37cy8iz7f.db.yandex.net,'
            'c-mdbd60phvp3hvq7d3sq6.rw.db.yandex.net,'
            'c-mdbggsqlf0pf2rar6cck.rw.db.yandex.net,'
            'c-mdb636es44gm87hucoip.rw.db.yandex.net,'
            'sas-gvwzxfe1s83fmwex.db.yandex.net,'
            'vla-wwc7qtot5u6hhcqc.db.yandex.net'
        ),

        RQE_EXT_ASYNC_SCHEME='https',

        TVM_INFO='...censored...',
        TVM_SECRET='...censored...',

        US_MASTER_TOKEN=SEC_US_MASTER_TOKEN,

        YA_MUSIC_PODCAST_STATS_PASSWORD='...censored...',
        YC_BILLING_ANALYTICS_PASSWORD='...censored...',
        CH_SCHOOLBOOK_PASSWORD='...censored...',

        FILE_UPLOADER_MASTER_TOKEN='...censored...',

        YENV_NAME='cloud',
        YENV_TYPE='testing',
    ),
    expected_config=AsyncAppSettings(
        APP_TYPE=AppType.CLOUD,
        PUBLIC_API_KEY=None,
        CACHES_ON=True,
        CACHES_REDIS=RedisSettings(
            MODE=RedisMode.sentinel,
            CLUSTER_NAME='caches',
            HOSTS=('rc1b-3kj6zsk0l243g6m9.mdb.cloud-preprod.yandex.net',),
            PORT=26379,
            DB=1,
            PASSWORD=SEC_REDIS_PASSWORD,
        ),
        MDB=MDBSettings(
            DOMAINS=('.mdb.cloud-preprod.yandex.net', '.mdb.cloud.yandex.net'),
            CNAME_DOMAINS=('.rw.mdb.yandex.net',),
            MANAGED_NETWORK_ENABLED=True,
            MANAGED_NETWORK_REMAP={
                '.mdb.cloud-preprod.yandex.net': '.db.yandex.net',
                '.mdb.cloud.yandex.net': '.db.yandex.net'
            }
        ),
        MUTATIONS_CACHES_ON=True,
        MUTATIONS_CACHES_DEFAULT_TTL=120,
        MUTATIONS_REDIS=RedisSettings(
            MODE=RedisMode.sentinel,
            CLUSTER_NAME='caches',
            HOSTS=('rc1b-3kj6zsk0l243g6m9.mdb.cloud-preprod.yandex.net',),
            PORT=26379,
            DB=2,
            PASSWORD=SEC_REDIS_PASSWORD,
        ),
        RQE_CACHES_ON=True,
        RQE_CACHES_TTL=600,
        RQE_CACHES_REDIS=RedisSettings(
            MODE=RedisMode.single_host,
            CLUSTER_NAME='rqecaches',
            HOSTS=('rc1b-zbbvgpj1d83x05qq.mdb.cloud-preprod.yandex.net',),
            PORT=6379,
            DB=1,
            PASSWORD=SEC_REDIS_PASSWORD,
        ),
        CACHES_TTL_SETTINGS=CachesTTLSettings(
            MATERIALIZED=3600,
            OTHER=300,
        ),
        YC_BILLING_HOST='https://billing.private-api.cloud-preprod.yandex.net:16465',
        BLEEDING_EDGE_USERS=(),
        CHYT_MIRRORING=None,
        SENTRY_ENABLED=True,
        SENTRY_DSN=ExternalTestingInstallation.SENTRY_DSN_DATASET_API,
        CRYPTO_KEYS_CONFIG=CryptoKeysConfig(
            actual_key_id="cloud_preprod_1",
            map_id_key={
                "0": SEC_CRY_KEY_0,
                "cloud_preprod_1": SEC_CRY_KEY_cloud_preprod_1,
            }
        ),
        US_BASE_URL='https://us-dl.private-api.ycp.cloud-preprod.yandex.net',
        US_PUBLIC_API_TOKEN=None,
        US_MASTER_TOKEN=SEC_US_MASTER_TOKEN,
        RQE_CONFIG=RQEConfig(
            ext_sync_rqe=RQEBaseURL(
                scheme='http',
                host='[::1]',
                port=9876,
            ),
            ext_async_rqe=RQEBaseURL(
                scheme='https',
                host='[::1]',
                port=9877,
            ),
            int_sync_rqe=RQEBaseURL(
                scheme='http',
                host='[::1]',
                port=9874,
            ),
            int_async_rqe=RQEBaseURL(
                scheme='http',
                host='[::1]',
                port=9875,
            ),
            hmac_key=SEC_EXT_QUERY_EXECUTER_SECRET_KEY.encode("ascii"),
        ),
        SAMPLES_CH_HOSTS=(
            'myt-g2ucdqpavskt6irw.db.yandex.net', 'sas-1h1276u34g7nt0vx.db.yandex.net',
            'sas-t2pl126yki67ztly.db.yandex.net', 'vla-1zwdkyy37cy8iz7f.db.yandex.net',
            'c-mdbd60phvp3hvq7d3sq6.rw.db.yandex.net', 'c-mdbggsqlf0pf2rar6cck.rw.db.yandex.net',
            'c-mdb636es44gm87hucoip.rw.db.yandex.net', 'sas-gvwzxfe1s83fmwex.db.yandex.net',
            'vla-wwc7qtot5u6hhcqc.db.yandex.net',
        ),  # List was replaced with tuple
        BI_COMPENG_PG_ON=True,
        BI_COMPENG_PG_URL='postgresql://postgres@%2Fvar%2Frun%2Fpostgresql/postgres',
        BI_ASYNC_APP_DISABLE_KEEPALIVE=False,  # replaced from None
        PUBLIC_CH_QUERY_TIMEOUT=30,
        YC_AUTH_SETTINGS=YCAuthSettings(
            YC_AUTHORIZE_PERMISSION='datalens.instances.use',
            YC_AS_ENDPOINT='as.private-api.cloud-preprod.yandex.net:4286',
            YC_SS_ENDPOINT='ss.private-api.cloud-preprod.yandex.net:8655',
            YC_TS_ENDPOINT='ts.private-api.cloud-preprod.yandex.net:4282',
            YC_API_ENDPOINT_IAM='iam.private-api.cloud-preprod.yandex.net:4283',
        ),
        YC_RM_CP_ENDPOINT='rm.private-api.cloud-preprod.yandex.net:4284',
        YC_IAM_TS_ENDPOINT='ts.private-api.cloud-preprod.yandex.net:4282',
        BLACKBOX_NAME='Mimino',
        FORMULA_PARSER_TYPE=ParserType.antlr_py,
        FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.readable,
        FILE_UPLOADER_BASE_URL="https://upload.datalens-preprod.yandex.ru",
        FILE_UPLOADER_MASTER_TOKEN='...censored...',
    )
)


DEV_CONTROL_API_CASE = ConfigLoadingCase(
    env=dict(
        ALLOW_SUBQUERY_IN_PREVIEW='1',
        AUTH_MODE='gauthling',

        BI_COMPENG_PG_URL='postgresql://postgres@%2Fvar%2Frun%2Fpostgresql/postgres',
        BI_FORMULA_PARSER_TYPE='antlr_py',

        DL_BLACKBOX_NAME=None,

        DL_SENTRY_DSN=None,

        CACHES_ON='True',
        CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,

        CONNECTOR_AVAILABILITY_VISIBLE='clickhouse,postgres',

        MDB_DOMAINS='.mdb.cloud-preprod.yandex.net,.mdb.cloud.yandex.net',
        MDB_CNAME_DOMAINS='.rw.mdb.yandex.net',
        MDB_MANAGED_NETWORK_ENABLED='1',
        MDB_MANAGED_NETWORK_REMAP='{".mdb.cloud-preprod.yandex.net": ".db.yandex.net",'
                                  ' ".mdb.cloud.yandex.net": ".db.yandex.net"}',

        MUTATIONS_CACHES_ON='True',
        MUTATIONS_CACHES_DEFAULT_TTL='120',
        MUTATIONS_REDIS_PASSWORD=SEC_REDIS_PASSWORD,

        # RQE_CACHES_ON='True',
        # RQE_CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,

        DL_CRY_ACTUAL_KEY_ID='cloud_preprod_1',
        DL_CRY_FALLBACK_KEY_ID='0',
        DL_CRY_KEY_VAL_ID_0=SEC_CRY_KEY_0,
        DL_CRY_KEY_VAL_ID_cloud_preprod_1=SEC_CRY_KEY_cloud_preprod_1,

        DL_USE_JAEGER_TRACER='1',

        EXT_QUERY_EXECUTER_SECRET_KEY=SEC_EXT_QUERY_EXECUTER_SECRET_KEY,

        QLOUD_TVM_CONFIG='...censored...',
        QLOUD_TVM_TOKEN='...censored...',

        SAMPLES_CH_HOST=(
            'myt-g2ucdqpavskt6irw.db.yandex.net,'
            'sas-1h1276u34g7nt0vx.db.yandex.net,'
            'sas-t2pl126yki67ztly.db.yandex.net,'
            'vla-1zwdkyy37cy8iz7f.db.yandex.net,'
            'c-mdbd60phvp3hvq7d3sq6.rw.db.yandex.net,'
            'c-mdbggsqlf0pf2rar6cck.rw.db.yandex.net,'
            'c-mdb636es44gm87hucoip.rw.db.yandex.net,'
            'sas-gvwzxfe1s83fmwex.db.yandex.net,'
            'vla-wwc7qtot5u6hhcqc.db.yandex.net'
        ),

        RQE_EXT_ASYNC_SCHEME='https',

        TVM_INFO='...censored...',
        TVM_SECRET='...censored...',

        US_MASTER_TOKEN=SEC_US_MASTER_TOKEN,

        YA_MUSIC_PODCAST_STATS_PASSWORD='...censored...',
        YC_BILLING_ANALYTICS_PASSWORD='...censored...',
        CH_SCHOOLBOOK_PASSWORD='...censored...',

        FILE_UPLOADER_MASTER_TOKEN='...censored...',
        REDIS_ARQ_PASSWORD='...censored...',
        US_BASE_URL='https://example.com',

        YENV_NAME='development',
        YENV_TYPE='tests',
    ),
    expected_config=ControlPlaneAppSettings(
        CONNECTOR_AVAILABILITY=ConnectorAvailabilityConfig(),
        ENV_TYPE=EnvType.development,
        APP_TYPE=AppType.TESTS,
        MDB=MDBSettings(
            DOMAINS=('.mdb.cloud-preprod.yandex.net', '.mdb.cloud.yandex.net'),
            CNAME_DOMAINS=('.rw.mdb.yandex.net',),
            MANAGED_NETWORK_ENABLED=True,
            MANAGED_NETWORK_REMAP={
                '.mdb.cloud-preprod.yandex.net': '.db.yandex.net',
                '.mdb.cloud.yandex.net': '.db.yandex.net'
            }
        ),
        RQE_CACHES_TTL=600,
        BLEEDING_EDGE_USERS=(),
        CHYT_MIRRORING=None,
        CRYPTO_KEYS_CONFIG=CryptoKeysConfig(
            actual_key_id="cloud_preprod_1",
            map_id_key={
                "0": SEC_CRY_KEY_0,
                "cloud_preprod_1": SEC_CRY_KEY_cloud_preprod_1,
            }
        ),
        US_BASE_URL='https://example.com',
        US_MASTER_TOKEN=SEC_US_MASTER_TOKEN,
        RQE_CONFIG=RQEConfig(
            ext_sync_rqe=RQEBaseURL(
                scheme='http',
                host='[::1]',
                port=9876,
            ),
            ext_async_rqe=RQEBaseURL(
                scheme='https',
                host='[::1]',
                port=9877,
            ),
            int_sync_rqe=RQEBaseURL(
                scheme='http',
                host='[::1]',
                port=9874,
            ),
            int_async_rqe=RQEBaseURL(
                scheme='http',
                host='[::1]',
                port=9875,
            ),
            hmac_key=SEC_EXT_QUERY_EXECUTER_SECRET_KEY.encode("ascii"),
        ),
        SAMPLES_CH_HOSTS=(
            'myt-g2ucdqpavskt6irw.db.yandex.net', 'sas-1h1276u34g7nt0vx.db.yandex.net',
            'sas-t2pl126yki67ztly.db.yandex.net', 'vla-1zwdkyy37cy8iz7f.db.yandex.net',
            'c-mdbd60phvp3hvq7d3sq6.rw.db.yandex.net', 'c-mdbggsqlf0pf2rar6cck.rw.db.yandex.net',
            'c-mdb636es44gm87hucoip.rw.db.yandex.net', 'sas-gvwzxfe1s83fmwex.db.yandex.net',
            'vla-wwc7qtot5u6hhcqc.db.yandex.net',
        ),  # List was replaced with tuple
        BI_COMPENG_PG_ON=True,
        BI_COMPENG_PG_URL='postgresql://postgres@%2Fvar%2Frun%2Fpostgresql/postgres',
        PUBLIC_CH_QUERY_TIMEOUT=30,
        FORMULA_PARSER_TYPE=ParserType.antlr_py,
        FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.readable,
        FILE_UPLOADER_MASTER_TOKEN='...censored...',
        YC_AUTH_SETTINGS=None,
        REDIS_ARQ=RedisSettings(
            PASSWORD='...censored...',
            MODE=RedisMode.single_host,
            CLUSTER_NAME=None,
            HOSTS=('127.0.0.1',),
            PORT=None,
            DB=11,
        ),
    )
)


@pytest.mark.parametrize("case", (
    CLOUD_PRE_PROD_DATA_API_CASE,
    DEV_CONTROL_API_CASE,
))
def test_config_loading(case: ConfigLoadingCase):
    expected_config = case.expected_config

    loader = EnvSettingsLoader(case.env)
    actual_config = loader.load_settings(
        type(expected_config),
        fallback_cfg_resolver=YEnvFallbackConfigResolver(
            env_map=EnvAliasesMap,
            installation_map=InstallationsMap,
        )
    )

    if not isinstance(actual_config, AsyncAppSettings):
        assert actual_config.CONNECTOR_AVAILABILITY.visible_connectors == {ConnectionType('clickhouse'), ConnectionType('postgres')}
        actual_config = attr.evolve(actual_config, CONNECTOR_AVAILABILITY=ConnectorAvailabilityConfig())

    assert actual_config == expected_config


SPECIAL_CLOUD_PRE_PROD_DATA_API_CASE = ConfigLoadingCase(
    env=dict(
        **CLOUD_PRE_PROD_DATA_API_CASE.env,
        **dict(
            CONNECTORS_FILE_PASSWORD='...censored...',
            CONNECTORS_FILE_ACCESS_KEY_ID='...censored...',
            CONNECTORS_FILE_SECRET_ACCESS_KEY='...censored...',

            **{
                env_key: env_value for connector_settings in
                {
                    connector: {
                        f'CONNECTORS_CH_FROZEN_{connector}_HOST': 'host_value',
                        f'CONNECTORS_CH_FROZEN_{connector}_PORT': '8443',
                        f'CONNECTORS_CH_FROZEN_{connector}_DB_NAME': 'db_value',
                        f'CONNECTORS_CH_FROZEN_{connector}_USERNAME': 'username_value',
                        f'CONNECTORS_CH_FROZEN_{connector}_USE_MANAGED_NETWORK': '0',
                        f'CONNECTORS_CH_FROZEN_{connector}_ALLOWED_TABLES': "[\"list\",\"of\",\"tables\"]",
                        f'CONNECTORS_CH_FROZEN_{connector}_SUBSELECT_TEMPLATES': "[{\"sql_query\":\"\\nSELECT\\n    *\\nFROM\\n    samples.orders t1\\n\",\"title\":\"SQL for cohorts\"}]",
                        f'CONNECTORS_CH_FROZEN_{connector}_PASSWORD': '...censored...',
                    }
                    for connector in (
                        'BUMPY_ROADS',
                        'COVID',
                        'DEMO',
                        'DTP',
                        'GKH',
                        'SAMPLES',
                        'TRANSPARENCY',
                        'WEATHER',
                        'HORECA',
                    )
                }.values() for env_key, env_value in connector_settings.items()
            },
            CONNECTORS_MOYSKLAD_PASSWORD='...censored...',
            CONNECTORS_MOYSKLAD_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
            CONNECTORS_EQUEO_PASSWORD='...censored...',
            CONNECTORS_EQUEO_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
            CONNECTORS_BITRIX_PASSWORD='...censored...',
            CONNECTORS_BITRIX_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
            CONNECTORS_KONTUR_MARKET_PASSWORD='...censored...',
            CONNECTORS_KONTUR_MARKET_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
            CONNECTORS_CH_BILLING_ANALYTICS_PASSWORD='...censored...',
            CONNECTORS_MARKET_COURIERS_PASSWORD='...censored...',
            CONNECTORS_SMB_HEATMAPS_PASSWORD='...censored...',
            CONNECTORS_CH_YA_MUSIC_PODCAST_STATS_PASSWORD='...censored...',
            CONNECTORS_SCHOOLBOOK_PASSWORD='...censored...',
            CONNECTORS_USAGE_TRACKING_PASSWORD='...censored...',
        ),
    ),
    expected_config=CLOUD_PRE_PROD_DATA_API_CASE.expected_config,
)
@pytest.mark.parametrize("case", (
    SPECIAL_CLOUD_PRE_PROD_DATA_API_CASE,
))
def test_connectors_settings_loading(case: ConfigLoadingCase):
    settings_registry = {CONNECTION_TYPE_CH_FROZEN_SAMPLES: CHFrozenSamplesConnectorSettings}
    fallbacks = {CONNECTION_TYPE_CH_FROZEN_SAMPLES: ch_frozen_samples_settings_fallback}
    connectors_settings = load_connectors_settings_from_env_with_fallback(
        settings_registry=settings_registry,
        fallbacks=fallbacks,
        env=case.env,
        fallback_cfg_resolver=YEnvFallbackConfigResolver(
            env_map=EnvAliasesMap,
            installation_map=InstallationsMap,
        )
    )

    assert len(connectors_settings) == 1
    samples_settings = connectors_settings[CONNECTION_TYPE_CH_FROZEN_SAMPLES]
    assert isinstance(samples_settings, CHFrozenSamplesConnectorSettings)
    assert samples_settings.PORT == 8443
    assert samples_settings.USE_MANAGED_NETWORK is False
    assert samples_settings.ALLOWED_TABLES == ['list', 'of', 'tables']
    assert samples_settings.SUBSELECT_TEMPLATES == \
           ({'title': 'SQL for cohorts', 'sql_query': '\nSELECT\n    *\nFROM\n    samples.orders t1\n'},)
