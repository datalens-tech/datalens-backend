import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.mixins import TableDatasourceSettingsMixin
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class TrinoConnectorSettings(
    ConnectorSettingsBase,
    TableDatasourceSettingsMixin,
):
    pass


def trino_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="TRINO")
    if cfg is None:
        settings = TrinoConnectorSettings()
    else:
        settings = TrinoConnectorSettings(  # type: ignore
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", False),
        )

    return dict(TRINO=settings)


class TrinoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = TrinoConnectorSettings
    fallback = trino_settings_fallback
