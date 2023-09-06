from __future__ import annotations

import json
from typing import Any

import pytest
from cryptography import fernet

from bi_api_lib.app_settings import (
    AsyncAppSettings,
    ControlPlaneAppSettings,
    BaseAppSettings,
)
from bi_configs.enums import AppType
from bi_configs.crypto_keys import CryptoKeysConfig
from bi_configs.environments import (
    CommonInstallation,
    EnvAliasesMap,
    InstallationsMap,
)
from bi_configs.settings_loaders.fallback_cfg_resolver import (
    YEnvFallbackConfigResolver,
    YamlFileConfigResolver, ObjectLikeConfig,
)
from bi_configs.settings_loaders.loader_env import (
    EnvSettingsLoader,
    get_default_resolver,
)


def test_simple():
    assert 1 / 2 == 0.5


@pytest.mark.parametrize("yenv_type,app_type", (
    # Internal
    ("int-testing", AppType.INTRANET),
    ("int-production", AppType.INTRANET),
    # Cloud
    ("testing", AppType.CLOUD),
    ("testing-public", AppType.CLOUD_PUBLIC),
    ("testing-sec-embeds", AppType.CLOUD_EMBED),
    ("production", AppType.CLOUD),
    ("production-public", AppType.CLOUD_PUBLIC),
    ("production-sec-embeds", AppType.CLOUD_EMBED),
    # DataCloud
    # (None, AppType.DATA_CLOUD),
    # (None, AppType.DATA_CLOUD_PUBLIC),
))
def test_extractor_building(yenv_type, app_type):
    fallback_cfg: Any = None

    if yenv_type is not None:
        fallback_cfg = YEnvFallbackConfigResolver(
            env_map=EnvAliasesMap,
            installation_map=InstallationsMap,
        ).resolve(dict(YENV_TYPE=yenv_type))
        # More granular test for correct mapping is implemented in bi_config
        assert isinstance(fallback_cfg, CommonInstallation)

    extractor = EnvSettingsLoader.build_top_level_extractor(
        settings_type=AsyncAppSettings,
        app_cfg_type=app_type,
        fallback_cfg=fallback_cfg,
    )
    # TODO FIX: BI-2497 check that environment variables list build successfully when it become implemented
    assert extractor is not None


EXPECTED_CRY_ACTUAL_KEY_ID = "i8134"
EXPECTED_NON_ACTUAL_CRYPTO_KEY_ID = "i0"
EXPECTED_CRY_KEY_MAP = {
    EXPECTED_CRY_ACTUAL_KEY_ID: fernet.Fernet.generate_key().decode(),
    EXPECTED_NON_ACTUAL_CRYPTO_KEY_ID: fernet.Fernet.generate_key().decode()
}

MIN_REQUIRED_ENV_COMMON = {
    "DL_CRY_ACTUAL_KEY_ID": EXPECTED_CRY_ACTUAL_KEY_ID,
    f"DL_CRY_KEY_VAL_ID_{EXPECTED_CRY_ACTUAL_KEY_ID}": EXPECTED_CRY_KEY_MAP[EXPECTED_CRY_ACTUAL_KEY_ID],
    f"DL_CRY_KEY_VAL_ID_{EXPECTED_NON_ACTUAL_CRYPTO_KEY_ID}": EXPECTED_CRY_KEY_MAP[EXPECTED_NON_ACTUAL_CRYPTO_KEY_ID],
    "RQE_SECRET_KEY": "SomeSecretKey",
    "REDIS_ARQ_PASSWORD": "SomeARQRedisPassword",
}

MIN_REQUIRED_SUB_ENV_CACHES = dict(
    CACHES_REDIS_PASSWORD="SomeRedisPassword",
    MUTATIONS_REDIS_PASSWORD="SomeRedisPassword",
)

MIN_REQUIRED_SUB_ENV_FILE_CONN = dict(
    CONNECTORS_FILE_PASSWORD='...censored...',
    CONNECTORS_FILE_ACCESS_KEY_ID='...censored...',
    CONNECTORS_FILE_SECRET_ACCESS_KEY='...censored...',
)

FROZEN_CONECTORS = (
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

MIN_REQUIRED_SUB_ENV_FROZEN_PASSWORDS = {
    f'CONNECTORS_CH_FROZEN_{connector}_PASSWORD': '...censored...'
    for connector in FROZEN_CONECTORS
}

MIN_REQUIRED_SUB_ENV_FROZEN_ALL_BUT_PASSWORDS = {
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
            }
            for connector in FROZEN_CONECTORS
        }.values() for env_key, env_value in connector_settings.items()
    },
}

# Intranet

MIN_REQUIRED_ENV_INTRANET_CONTROL = dict(
    CONNECTORS_USAGE_TRACKING_YA_TEAM_PASSWORD='...censored...',
    **MIN_REQUIRED_ENV_COMMON,
    **MIN_REQUIRED_SUB_ENV_FILE_CONN,
)

MIN_REQUIRED_ENV_INTRANET_DATA = dict(
    CONNECTORS_USAGE_TRACKING_YA_TEAM_PASSWORD='...censored...',
    **MIN_REQUIRED_ENV_COMMON,
    **MIN_REQUIRED_SUB_ENV_CACHES,
    **MIN_REQUIRED_SUB_ENV_FILE_CONN,
)

# Yandex Cloud

MIN_REQUIRED_ENV_CLOUD_COMMON = dict(
    CONNECTORS_MARKET_COURIERS_PASSWORD='...censored...',
    CONNECTORS_SMB_HEATMAPS_PASSWORD='...censored...',
    CONNECTORS_MOYSKLAD_PASSWORD='...censored...',
    CONNECTORS_MOYSKLAD_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
    CONNECTORS_EQUEO_PASSWORD='...censored...',
    CONNECTORS_EQUEO_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
    CONNECTORS_BITRIX_PASSWORD='...censored...',
    CONNECTORS_BITRIX_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
    CONNECTORS_KONTUR_MARKET_PASSWORD='...censored...',
    CONNECTORS_KONTUR_MARKET_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
    CONNECTORS_CH_BILLING_ANALYTICS_PASSWORD='...censored...',
    CONNECTORS_CH_YA_MUSIC_PODCAST_STATS_PASSWORD='...censored...',
    CONNECTORS_SCHOOLBOOK_PASSWORD='...censored...',
    RQE_CACHES_REDIS_PASSWORD='...censored...',
    **MIN_REQUIRED_ENV_COMMON,
    **MIN_REQUIRED_SUB_ENV_FILE_CONN,
)

MIN_REQUIRED_ENV_CLOUD_CONTROL = dict(
    **MIN_REQUIRED_ENV_CLOUD_COMMON,
    **MIN_REQUIRED_SUB_ENV_FROZEN_PASSWORDS,
)

MIN_REQUIRED_ENV_CLOUD_CONTROL_TESTING = dict(
    CONNECTORS_USAGE_TRACKING_PASSWORD='...censored...',
    **MIN_REQUIRED_ENV_CLOUD_CONTROL,
    **MIN_REQUIRED_SUB_ENV_FROZEN_ALL_BUT_PASSWORDS,
)

MIN_REQUIRED_ENV_CLOUD_DATA = dict(
    **MIN_REQUIRED_ENV_CLOUD_COMMON,
    **MIN_REQUIRED_SUB_ENV_CACHES,
    **MIN_REQUIRED_SUB_ENV_FROZEN_PASSWORDS,
)

MIN_REQUIRED_ENV_CLOUD_DATA_TESTING = dict(
    CONNECTORS_USAGE_TRACKING_PASSWORD='...censored...',
    **MIN_REQUIRED_ENV_CLOUD_DATA,
    **MIN_REQUIRED_SUB_ENV_FROZEN_ALL_BUT_PASSWORDS,
)

# Yandex Cloud public

MIN_REQUIRED_ENV_CLOUD_PUBLIC_DATA = dict(
    **MIN_REQUIRED_ENV_CLOUD_COMMON,
    **MIN_REQUIRED_SUB_ENV_CACHES,
    **MIN_REQUIRED_SUB_ENV_FROZEN_PASSWORDS,
)

MIN_REQUIRED_ENV_CLOUD_PUBLIC_DATA_TESTING = dict(
    CONNECTORS_USAGE_TRACKING_PASSWORD='...censored...',
    **MIN_REQUIRED_ENV_CLOUD_PUBLIC_DATA,
    **MIN_REQUIRED_SUB_ENV_FROZEN_ALL_BUT_PASSWORDS,
)

# DataCloud

MIN_REQUIRED_ENV_DC_COMMON = dict(
    DL_CRY_JSON_VALUE=json.dumps(dict(
        actual_key_id=EXPECTED_CRY_ACTUAL_KEY_ID,
        keys=EXPECTED_CRY_KEY_MAP,
    )),
    RQE_SECRET_KEY="SomeSecretKey",
)

MIN_REQUIRED_SUB_ENV_US = dict(
    US_HOST="https://localhost:30434",
)

MIN_REQUIRED_SUB_ENV_NO_CACHES = dict(
    CACHES_ON="false",  # TODO FIX: https://st.yandex-team.ru/BI-2511 require redis password when become ready
)

MIN_REQUIRED_ENV_DC_CONTROL = dict(
    **MIN_REQUIRED_ENV_DC_COMMON,
    **MIN_REQUIRED_SUB_ENV_US,
)

MIN_REQUIRED_ENV_DC_DATA = dict(
    **MIN_REQUIRED_ENV_DC_COMMON,
    **MIN_REQUIRED_SUB_ENV_US,
    **MIN_REQUIRED_SUB_ENV_NO_CACHES,
)


@pytest.mark.parametrize("yenv_type,app_type,minimal_env,settings_cls", (
    # Internal
    ("int-testing", AppType.INTRANET, MIN_REQUIRED_ENV_INTRANET_DATA, AsyncAppSettings),
    ("int-testing", AppType.INTRANET, MIN_REQUIRED_ENV_INTRANET_CONTROL, ControlPlaneAppSettings),
    ("int-production", AppType.INTRANET, MIN_REQUIRED_ENV_INTRANET_DATA, AsyncAppSettings),
    ("int-production", AppType.INTRANET, MIN_REQUIRED_ENV_INTRANET_CONTROL, ControlPlaneAppSettings),
    # Cloud testing
    ("testing", AppType.CLOUD, MIN_REQUIRED_ENV_CLOUD_DATA_TESTING, AsyncAppSettings),
    ("testing", AppType.CLOUD, MIN_REQUIRED_ENV_CLOUD_CONTROL_TESTING, ControlPlaneAppSettings),
    ("testing-public", AppType.CLOUD_PUBLIC, MIN_REQUIRED_ENV_CLOUD_PUBLIC_DATA_TESTING, AsyncAppSettings),
    # Cloud
    ("production", AppType.CLOUD, MIN_REQUIRED_ENV_CLOUD_DATA, AsyncAppSettings),
    ("production", AppType.CLOUD, MIN_REQUIRED_ENV_CLOUD_CONTROL, ControlPlaneAppSettings),
    ("production-public", AppType.CLOUD_PUBLIC, MIN_REQUIRED_ENV_CLOUD_PUBLIC_DATA, AsyncAppSettings),
    # DataCloud
    ("datacloud", AppType.DATA_CLOUD, MIN_REQUIRED_ENV_DC_DATA, AsyncAppSettings),
    ("datacloud", AppType.DATA_CLOUD, MIN_REQUIRED_ENV_DC_CONTROL, ControlPlaneAppSettings),
))
def test_load_settings_with_minimal_required_environments(yenv_type, app_type, minimal_env, settings_cls):
    env = dict(
        YENV_TYPE=yenv_type,
        **minimal_env,
    )

    loader = EnvSettingsLoader(env)
    settings = loader.load_settings(
        settings_cls,
        fallback_cfg_resolver=YEnvFallbackConfigResolver(
            installation_map=InstallationsMap,
            env_map=EnvAliasesMap,
        ),
    )
    assert isinstance(settings, BaseAppSettings)
    assert settings.CRYPTO_KEYS_CONFIG == CryptoKeysConfig(
        actual_key_id=EXPECTED_CRY_ACTUAL_KEY_ID,
        map_id_key=EXPECTED_CRY_KEY_MAP,
    )


def test_get_default_resolver():
    resolver = get_default_resolver(
        env={}
    )
    assert isinstance(resolver, YEnvFallbackConfigResolver)
    resolver = get_default_resolver(
        env={
            YamlFileConfigResolver.config_path_key: 'bla'
        }
    )
    assert isinstance(resolver, YamlFileConfigResolver)


def test_yenv_fallback_resolver_with_custom_params():
    class SomeDefaults:
        A: str = 'a'
        B: int = 'b'

    class AnotherDefaults:
        A: str = 'a'
        B: int = 'b'

    class _EnvMap:
        foo: str = 'some'
        bar: str = 'another'

    some_defaults = SomeDefaults()
    another_defaults = AnotherDefaults()

    class _InstallationMap:
        some: Any = some_defaults
        another: Any = another_defaults

    resolver = YEnvFallbackConfigResolver(
        installation_map=_InstallationMap,
        env_map=_EnvMap,
    )
    assert resolver.resolve({YEnvFallbackConfigResolver.yenv_type_key: 'foo'}) == some_defaults
    assert resolver.resolve({YEnvFallbackConfigResolver.yenv_type_key: 'bar'}) == another_defaults


def test_yaml_file_config_resolver():
    # it's impossible to read the file if you got crazy and compiled python into a binary
    # I promise to do a normal test in tir1
    assert YamlFileConfigResolver().is_config_enabled(s_dict={}) is False
    assert YamlFileConfigResolver().is_config_enabled(s_dict={
        YamlFileConfigResolver.config_path_key: 'bla'
    }) is True


def test_object_like_config():
    data = {
        'some_key': 1,
        'another_key': ['value1', 'value2', 'value3'],
        'complex_key': {
            'key1': 'key1_value',
            'key2': 'key2_value',
        },
    }
    config = ObjectLikeConfig.from_dict(data)
    assert config.get('some_key') == 1
    assert config['some_key'] == 1
    assert config.some_key == 1
    assert hasattr(config, 'some_key')
    assert config['another_key'] == ['value1', 'value2', 'value3']
    assert config['complex_key']['key1'] == 'key1_value'
    assert config.complex_key.key2 == 'key2_value'

    with pytest.raises(AttributeError) as exc_info:
        assert config.complex_key.key3
    assert str(exc_info.value) == 'There is no record in config by path: "complex_key.key3"'
