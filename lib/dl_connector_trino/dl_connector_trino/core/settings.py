import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DeprecatedTableDatasourceSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)

from dl_connector_trino.core.constants import CONNECTION_TYPE_TRINO


@attr.s(frozen=True)
class DeprecatedTrinoConnectorSettings(
    DeprecatedConnectorSettingsBase,
    DeprecatedTableDatasourceSettingsMixin,
):
    pass


def trino_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="TRINO")
    if cfg is None:
        settings = DeprecatedTrinoConnectorSettings()
    else:
        settings = DeprecatedTrinoConnectorSettings(  # type: ignore
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", True),
        )

    return dict(TRINO=settings)


class TrinoConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin):
    type: str = CONNECTION_TYPE_TRINO.value


class TrinoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedTrinoConnectorSettings
    fallback = trino_settings_fallback
    pydantic_settings_class = TrinoConnectorSettings
