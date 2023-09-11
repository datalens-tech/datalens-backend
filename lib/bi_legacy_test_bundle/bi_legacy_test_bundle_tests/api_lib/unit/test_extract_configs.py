from __future__ import annotations

from typing import Type

import attr
import pytest
import yaml

from bi_api_lib_ya.app_settings import BaseAppSettings, AsyncAppSettings, ControlPlaneAppSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import (
    ObjectLikeConfig,
    ConstantFallbackConfigResolver,
    YEnvFallbackConfigResolver,
)
from bi_configs.settings_loaders.loader_env import EnvSettingsLoader
from bi_configs.settings_loaders.settings_serializers import defaults_to_yaml
from bi_defaults.environments import (
    CommonInstallation,
    InstallationsMap,
    EnvAliasesMap,
)


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
    ),
    default_settings=InstallationsMap.int_prod,
)
INT_TEST_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME='intranet',
        YENV_TYPE='int-testing',
    ),
    default_settings=InstallationsMap.int_testing,
)
EXT_PROD_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME='cloud',
        YENV_TYPE='production',
        RQE_CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
    ),
    default_settings=InstallationsMap.ext_prod,
)
EXT_TEST_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME='cloud',
        YENV_TYPE='testing',
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


@pytest.mark.parametrize(
    "installation_case",
    argvalues=(
        INT_PROD_INSTALLATION_CASE,
        INT_TEST_INSTALLATION_CASE,
        EXT_PROD_INSTALLATION_CASE,
        EXT_TEST_INSTALLATION_CASE,
    ),
    ids=(
        'int_prod',
        'int_testing',
        'ext_prod',
        'ext_testing',
    )
)
@pytest.mark.parametrize(
    "app_case",
    argvalues=(
        ASYNC_APP_CASE,
        SYNC_APP_CASE,
    ),
    ids=(
        'async_app',
        'sync_app',
    )
)
def test_config_diff(installation_case, app_case):
    raw_config = defaults_to_yaml(installation_case.default_settings)
    dict_config = ObjectLikeConfig.from_dict(yaml.safe_load(raw_config), path=[])

    env = dict(
        EXT_QUERY_EXECUTER_SECRET_KEY='123',
        DL_CRY_ACTUAL_KEY_ID='cloud_preprod_1',
        DL_CRY_FALLBACK_KEY_ID='0',
        DL_CRY_KEY_VAL_ID_0='asdasd',
        DL_CRY_KEY_VAL_ID_cloud_preprod_1='asd',
        MUTATIONS_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
        CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
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
