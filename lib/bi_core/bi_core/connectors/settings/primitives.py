from typing import Callable, Type

import attr

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig


SettingsFallbackType = Callable[[ObjectLikeConfig], dict[str, ConnectorSettingsBase]]


@attr.s(frozen=True)
class ConnectorSettingsDefinition:
    settings_class: Type[ConnectorSettingsBase] = attr.ib()
    fallback: ConnectorSettingsBase = attr.ib()
