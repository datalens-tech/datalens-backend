from __future__ import annotations

from typing import Callable

import attr

from dl_configs.environments import LegacyDefaults
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.settings_obj_base import SettingsBase


@attr.s(frozen=True)
class ConnectorSettingsBase(SettingsBase):
    """"""


ConnectorsConfigType = ObjectLikeConfig | LegacyDefaults
SettingsFallbackType = Callable[[ConnectorsConfigType], dict[str, ConnectorSettingsBase]]
