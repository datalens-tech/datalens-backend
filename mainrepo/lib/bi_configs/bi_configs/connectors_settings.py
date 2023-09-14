from __future__ import annotations
from typing import Any, Callable

import attr

from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_configs.settings_loaders.meta_definition import s_attrib
from bi_configs.environments import LegacyDefaults
from bi_configs.settings_loaders.settings_obj_base import SettingsBase


def _env_loading_unimplemented(_: str) -> Any:
    raise NotImplementedError("Loading from environment is not supported.")


@attr.s(frozen=True)
class ConnectorSettingsBase(SettingsBase):
    """"""


ConnectorsConfigType = ObjectLikeConfig | LegacyDefaults
SettingsFallbackType = Callable[[ConnectorsConfigType], dict[str, ConnectorSettingsBase]]


@attr.s(frozen=True)
class ClickHouseConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class GreenplumConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class MysqlConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class PostgresConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore
