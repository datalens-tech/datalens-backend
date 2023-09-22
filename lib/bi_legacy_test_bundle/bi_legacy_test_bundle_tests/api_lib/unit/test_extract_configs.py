from __future__ import annotations

from collections import defaultdict
import enum
import inspect
from typing import Type

import attr
from dynamic_enum import DynamicEnum
import pytest
import yaml

from bi_api_lib_ya.app_settings import (
    AsyncAppSettings,
    BaseAppSettings,
    ControlPlaneAppSettings,
)
from bi_defaults.environments import (
    CommonInstallation,
    EnvAliasesMap,
    InstallationsMap,
)
from bi_defaults.yenv_type import YEnvFallbackConfigResolver
from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.environments import LegacyEnvAliasesMap
from dl_configs.settings_loaders.fallback_cfg_resolver import (
    ConstantFallbackConfigResolver,
    ObjectLikeConfig,
)
from dl_configs.settings_loaders.loader_env import EnvSettingsLoader


SEC_REDIS_PASSWORD = "someRedisPassowrd"


def object_to_dict(obj: object) -> dict:
    def _transform(obj: object) -> object:
        if isinstance(obj, enum.Enum):
            return obj.value
        if isinstance(obj, tuple):
            return [_transform(v) for v in obj]
        if isinstance(obj, list):
            return [_transform(v) for v in obj]
        return obj

    config = {
        k: _transform(v) if not hasattr(v, "__dict__") or isinstance(v, enum.Enum) else object_to_dict(v.__class__)
        for k, v in inspect.getmembers(obj)
        if not k.startswith("_")
    }
    return config


def defaults_to_dict(default_settings: LegacyEnvAliasesMap) -> dict:
    def _uniq_connector_settings(conn_classes: list[Type[ConnectorsDataBase]]) -> dict[str, Type[ConnectorsDataBase]]:
        conn_cls_by_type = defaultdict(list)
        result: dict[str, Type[ConnectorsDataBase]] = {}
        for cls in conn_classes:
            conn_cls_by_type[cls.connector_name()].append(cls)
        for name, classes in conn_cls_by_type.items():
            if len(classes) == 1:
                result[name] = classes[0]
                continue
            class_for_env = [
                cls for cls in classes if cls.__name__.endswith("Production") or cls.__name__.endswith("Testing")
            ]
            if len(class_for_env) == 1:
                result[name] = class_for_env[0]
                continue
            elif len(class_for_env) > 1:
                raise Exception("Too many envs")
            class_for_installation = [cls for cls in classes if cls.__name__.endswith("Installation")]
            if len(class_for_installation) == 1:
                result[name] = class_for_installation[0]
                continue
            elif len(class_for_installation) > 1:
                raise Exception("Too many installations")
            raise Exception("Can't choose the right class")
        return result

    config = object_to_dict(default_settings)

    if hasattr(default_settings, "CONNECTOR_AVAILABILITY"):
        config["CONNECTOR_AVAILABILITY"] = attr.asdict(
            default_settings.CONNECTOR_AVAILABILITY,
            value_serializer=lambda _, __, v: v.value if isinstance(v, (enum.Enum, DynamicEnum)) else v,
        )

    conn_classes = [
        cls
        for cls in inspect.getmro(default_settings.__class__)
        if (
            cls.__name__.startswith("ConnectorsData")
            and issubclass(cls, ConnectorsDataBase)
            and cls.__name__ != "ConnectorsDataBase"
        )
    ]
    connectors_data = {}
    all_connectors_keys = set()
    for name, cls in _uniq_connector_settings(conn_classes=conn_classes).items():
        connectors_data[name] = object_to_dict(cls)
        for key in connectors_data[name].keys():
            all_connectors_keys.add(key)
    for conn_key in all_connectors_keys:
        del config[conn_key]
    config["CONNECTORS"] = connectors_data
    return config


def defaults_to_yaml(defaults: LegacyEnvAliasesMap) -> str:
    config = defaults_to_dict(defaults)
    return yaml.safe_dump(config)


def test_dump_defaults_to_yaml() -> None:
    """
    The easiest way to dump old defaults to yaml
    Just copy-paste it from test's output
    """
    config = defaults_to_yaml(InstallationsMap.int_testing)
    for _i in range(15):
        print("\n")
    print("---")
    print(config)


@attr.s(frozen=True, auto_attribs=True)
class ConfigInstallationCase:
    env: dict[str, str]
    default_settings: CommonInstallation


INT_PROD_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME="intranet",
        YENV_TYPE="int-production",
    ),
    default_settings=InstallationsMap.int_prod,
)
INT_TEST_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME="intranet",
        YENV_TYPE="int-testing",
    ),
    default_settings=InstallationsMap.int_testing,
)
EXT_PROD_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME="cloud",
        YENV_TYPE="production",
        RQE_CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
    ),
    default_settings=InstallationsMap.ext_prod,
)
EXT_TEST_INSTALLATION_CASE = ConfigInstallationCase(
    env=dict(
        YENV_NAME="cloud",
        YENV_TYPE="testing",
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
        REDIS_ARQ_PASSWORD="...censored...",
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
        "int_prod",
        "int_testing",
        "ext_prod",
        "ext_testing",
    ),
)
@pytest.mark.parametrize(
    "app_case",
    argvalues=(
        ASYNC_APP_CASE,
        SYNC_APP_CASE,
    ),
    ids=(
        "async_app",
        "sync_app",
    ),
)
def test_config_diff(installation_case, app_case):
    raw_config = defaults_to_yaml(installation_case.default_settings)
    dict_config = ObjectLikeConfig.from_dict(yaml.safe_load(raw_config), path=[])

    env = dict(
        EXT_QUERY_EXECUTER_SECRET_KEY="123",
        DL_CRY_ACTUAL_KEY_ID="cloud_preprod_1",
        DL_CRY_FALLBACK_KEY_ID="0",
        DL_CRY_KEY_VAL_ID_0="asdasd",
        DL_CRY_KEY_VAL_ID_cloud_preprod_1="asd",
        MUTATIONS_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
        CACHES_REDIS_PASSWORD=SEC_REDIS_PASSWORD,
    )
    env.update(installation_case.env)
    env.update(app_case.env)
    settings_by_yaml = EnvSettingsLoader(env).load_settings(
        settings_type=app_case.settings_type, fallback_cfg_resolver=ConstantFallbackConfigResolver(config=dict_config)
    )
    settings_by_defaults = EnvSettingsLoader(env).load_settings(
        settings_type=app_case.settings_type,
        fallback_cfg_resolver=YEnvFallbackConfigResolver(
            installation_map=InstallationsMap,
            env_map=EnvAliasesMap,
        ),
    )

    assert settings_by_yaml == settings_by_defaults
