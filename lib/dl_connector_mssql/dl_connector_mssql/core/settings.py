import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.mixins import DatasourceTemplateSettingsMixin
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class MSSQLConnectorSettings(ConnectorSettingsBase, DatasourceTemplateSettingsMixin):
    pass


def mssql_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="MSSQL")
    if cfg is None:
        settings = MSSQLConnectorSettings()
    else:
        settings = MSSQLConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", False),
        )
    return dict(MSSQL=settings)


class MSSQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MSSQLConnectorSettings
    fallback = mssql_settings_fallback
