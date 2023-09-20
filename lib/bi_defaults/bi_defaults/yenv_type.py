import enum
import logging
from typing import (
    Any,
    ClassVar,
)

import attr

from dl_configs.environments import LegacyDefaults
from dl_configs.settings_loaders.common import SDict
from dl_configs.settings_loaders.exc import SettingsLoadingException
from dl_configs.settings_loaders.fallback_cfg_resolver import FallbackConfigResolver


LOGGER = logging.getLogger(__name__)


class AppType(enum.Enum):
    CLOUD = enum.auto()
    CLOUD_PUBLIC = enum.auto()
    CLOUD_EMBED = enum.auto()
    INTRANET = enum.auto()
    TESTS = enum.auto()
    DATA_CLOUD = enum.auto()
    NEBIUS = enum.auto()
    DATA_CLOUD_EMBED = enum.auto()


def resolve_by_yenv_type(yenv_type: str, env_map: Any, install_map: Any) -> LegacyDefaults:
    public_env_postfix = "-public"
    sec_embeds_postfix = "-sec-embeds"

    effective_yenv_type = yenv_type.removesuffix(public_env_postfix).removesuffix(sec_embeds_postfix).replace("-", "_")

    if not hasattr(env_map, effective_yenv_type):
        raise SettingsLoadingException(f"Unknown YENV type: {yenv_type!r} ({effective_yenv_type})")

    unaliased_yenv_type = getattr(env_map, effective_yenv_type)
    assert hasattr(install_map, unaliased_yenv_type), (
        f"InstallationMap does not contain key from EnvAliasesMap"
        f" {effective_yenv_type!r} -> {unaliased_yenv_type!r}"
    )

    cfg = getattr(install_map, unaliased_yenv_type)
    return cfg


@attr.s
class YEnvFallbackConfigResolver(FallbackConfigResolver):
    yenv_type_key: ClassVar[str] = "YENV_TYPE"
    _installation_map: Any = attr.ib()
    _env_map: Any = attr.ib()

    def resolve(self, s_dict: SDict) -> LegacyDefaults:
        LOGGER.info("Resolve by YEnvFallbackConfigResolver")
        yenv_type = s_dict.get(self.yenv_type_key)
        if yenv_type is None:
            raise SettingsLoadingException(f"Could not determine YENV type (key: {self.yenv_type_key!r})")
        return resolve_by_yenv_type(
            yenv_type=yenv_type,
            install_map=self._installation_map,
            env_map=self._env_map,
        )


def app_type_env_var_converter(env_value: str) -> AppType:
    return {
        "testing": AppType.CLOUD,
        "production": AppType.CLOUD,
        "testing-public": AppType.CLOUD_PUBLIC,
        "production-public": AppType.CLOUD_PUBLIC,
        "testing-sec-embeds": AppType.CLOUD_EMBED,
        "production-sec-embeds": AppType.CLOUD_EMBED,
        "int-testing": AppType.INTRANET,
        "int-production": AppType.INTRANET,
        "datacloud": AppType.DATA_CLOUD,
        "israel": AppType.NEBIUS,
        "nemax": AppType.NEBIUS,
        "tests": AppType.TESTS,
        "development": AppType.TESTS,
        "datacloud-sec-embeds": AppType.DATA_CLOUD_EMBED,
    }[env_value.lower()]
