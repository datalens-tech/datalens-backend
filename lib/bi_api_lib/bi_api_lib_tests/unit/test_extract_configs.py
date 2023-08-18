from __future__ import annotations

from typing import Type

import attr
import pytest
import yaml

from bi_api_lib.app_settings import (
    AsyncAppSettings,
    ControlPlaneAppSettings, BaseAppSettings,
)
from bi_configs.environments import (
    CommonInstallation,
    InstallationsMap, EnvAliasesMap,
)
from bi_configs.settings_loaders.fallback_cfg_resolver import (
    ObjectLikeConfig,
    ConstantFallbackConfigResolver,
    YEnvFallbackConfigResolver,
)
from bi_configs.settings_loaders.loader_env import EnvSettingsLoader
from bi_configs.settings_loaders.settings_serializers import defaults_to_yaml

SEC_REDIS_PASSWORD = 'someRedisPassowrd'


def test_dump_defaults_to_yaml() -> None:
    """
    The easiest way to dump old defaults to yaml
    Just copy-paste it from test's output
    """
    config = defaults_to_yaml(InstallationsMap.int_testing)
    for i in range(15):
        print('\n')
    print('---')
    print(config)


@attr.s(frozen=True, auto_attribs=True)
class ConfigInstallationCase:
    env: dict[str, str]
    default_settings: CommonInstallation


INT_PROD_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME='intranet',
        YENV_TYPE='int-production',
        CONNECTORS_USAGE_TRACKING_YA_TEAM_PASSWORD='...censored...',
    ),
    default_settings=InstallationsMap.int_prod,
)
INT_TEST_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME='intranet',
        YENV_TYPE='int-testing',
        CONNECTORS_USAGE_TRACKING_YA_TEAM_PASSWORD='...censored...',
    ),
    default_settings=InstallationsMap.int_testing,
)
EXT_PROD_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME='cloud',
        YENV_TYPE='production',
        CONNECTORS_CH_FROZEN_TRANSPARENCY_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_GKH_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_DEMO_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_COVID_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_DTP_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_HORECA_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_SAMPLES_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_WEATHER_PASSWORD='...censored...',
        CONNECTORS_CH_FROZEN_BUMPY_ROADS_PASSWORD='...censored...',
        CONNECTORS_SMB_HEATMAPS_PASSWORD='...censored...',
        CONNECTORS_CH_BILLING_ANALYTICS_PASSWORD='...censored...',
        CONNECTORS_EQUEO_PASSWORD='...censored...',
        CONNECTORS_MOYSKLAD_PASSWORD='...censored...',
        CONNECTORS_MOYSKLAD_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_EQUEO_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_BITRIX_PASSWORD='...censored...',
        CONNECTORS_BITRIX_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_KONTUR_MARKET_PASSWORD='...censored...',
        CONNECTORS_KONTUR_MARKET_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_MARKET_COURIERS_PASSWORD='...censored...',
        CONNECTORS_CH_YA_MUSIC_PODCAST_STATS_PASSWORD='...censored...',
        CONNECTORS_SCHOOLBOOK_PASSWORD='...censored...',
        RQE_CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
    ),
    default_settings=InstallationsMap.ext_prod,
)
EXT_TEST_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME='cloud',
        YENV_TYPE='testing',
        CONNECTORS_USAGE_TRACKING_PASSWORD='...censored...',
        CONNECTORS_SMB_HEATMAPS_PASSWORD='...censored...',
        CONNECTORS_CH_BILLING_ANALYTICS_PASSWORD='...censored...',
        CONNECTORS_MOYSKLAD_PASSWORD='...censored...',
        CONNECTORS_MOYSKLAD_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_EQUEO_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_EQUEO_PASSWORD='...censored...',
        CONNECTORS_BITRIX_PASSWORD='...censored...',
        CONNECTORS_BITRIX_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_KONTUR_MARKET_PASSWORD='...censored...',
        CONNECTORS_KONTUR_MARKET_PARTNER_KEYS='{"dl_private": {"1": "dl_priv_key_pem"}, "partner_public": {"1": "partner_pub_key_pem"}}',
        CONNECTORS_MARKET_COURIERS_PASSWORD='...censored...',
        CONNECTORS_CH_YA_MUSIC_PODCAST_STATS_PASSWORD='...censored...',
        CONNECTORS_SCHOOLBOOK_PASSWORD='...censored...',
        RQE_CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
    ),
    default_settings=InstallationsMap.ext_testing,
)


@attr.s(frozen=True, auto_attribs=True)
class ConfigInstallationCase:
    env: dict[str, str]
    settings_type: Type[BaseAppSettings]


ASYNC_APP_CASE = ConfigInstallationCase(
    env={},
    settings_type=AsyncAppSettings,
)
SYNC_APP_CASE = ConfigInstallationCase(
    env=dict(
        REDIS_ARQ_PASSWORD='...censored...',
    ),
    settings_type=ControlPlaneAppSettings,
)


@pytest.mark.parametrize("installation_case", (
    INT_PROD_INSTALLATION_CASE,
    INT_TEST_INSTALLATION_CASE,
    EXT_PROD_INSTALLATION_CASE,
    EXT_TEST_INSTALLATION_CASE,
))
@pytest.mark.parametrize("app_case", (
    ASYNC_APP_CASE,
    SYNC_APP_CASE,
))
def test_config_diff(installation_case, app_case):
    raw_config = defaults_to_yaml(installation_case.default_settings)
    dict_config = ObjectLikeConfig.from_dict(yaml.safe_load(raw_config), path=[])

    env = dict(
        EXT_QUERY_EXECUTER_SECRET_KEY='123',
        DL_CRY_ACTUAL_KEY_ID='cloud_preprod_1',
        DL_CRY_FALLBACK_KEY_ID='0',
        DL_CRY_KEY_VAL_ID_0='asdasd',
        DL_CRY_KEY_VAL_ID_cloud_preprod_1='asd',
        CONNECTORS_FILE_PASSWORD='...censored...',
        CONNECTORS_FILE_ACCESS_KEY_ID='...censored...',
        CONNECTORS_FILE_SECRET_ACCESS_KEY='...censored...',
        MUTATIONS_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
        CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
        CONNECTORS_FILE_REPLACE_SECRET_SALT='123',
    )
    env.update(installation_case.env)
    env.update(app_case.env)
    settings_by_yaml = EnvSettingsLoader(env).load_settings(
        settings_type=app_case.settings_type,
        fallback_cfg_resolver=ConstantFallbackConfigResolver(config=dict_config)
    )
    settings_by_defaults = EnvSettingsLoader(env).load_settings(
        settings_type=app_case.settings_type,
        fallback_cfg_resolver=YEnvFallbackConfigResolver(
            installation_map=InstallationsMap,
            env_map=EnvAliasesMap,
        ),
    )

    assert settings_by_yaml == settings_by_defaults
