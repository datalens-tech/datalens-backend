import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class MySQLConnectorSettings(
    ConnectorSettingsBase,
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
):
    pass


def mysql_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="MYSQL")
    if cfg is None:
        settings = MySQLConnectorSettings()
    else:
        settings = MySQLConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", True),
        )
    return dict(MYSQL=settings)


class MySQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MySQLConnectorSettings
    fallback = mysql_settings_fallback
