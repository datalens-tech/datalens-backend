import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class ClickHouseConnectorSettings(ConnectorSettingsBase):
    ...


def clickhouse_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="CLICKHOUSE")
    if cfg is None:
        settings = ClickHouseConnectorSettings()
    else:
        # use cfg to fill settings
        settings = ClickHouseConnectorSettings()

    return dict(CLICKHOUSE=settings)


class ClickHouseSettingDefinition(ConnectorSettingsDefinition):
    settings_class = ClickHouseConnectorSettings
    fallback = clickhouse_settings_fallback
