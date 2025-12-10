import attr
import pydantic

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.mixins import DeprecatedTableDatasourceSettingsMixin
from dl_core.connectors.settings.primitives import (
    DeprecatedConnectorSettingsDefinition,
    get_connectors_settings_config,
)
from dl_core.connectors.settings.pydantic.base import ConnectorSettings
from dl_core.connectors.settings.pydantic.mixins import TableDatasourceSettingsMixin

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


class DeprecatedTrinoSettingDefinition(DeprecatedConnectorSettingsDefinition):
    settings_class = DeprecatedTrinoConnectorSettings
    fallback = trino_settings_fallback


class TrinoConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin):
    type: str = pydantic.Field(alias="conn_type", default=CONNECTION_TYPE_TRINO.value)
