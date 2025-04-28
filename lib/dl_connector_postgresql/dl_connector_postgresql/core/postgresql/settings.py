import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.mixins import DatasourceTemplateSettingsMixin
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class PostgreSQLConnectorSettings(ConnectorSettingsBase, DatasourceTemplateSettingsMixin):
    pass


def postgresql_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="POSTGRESQL")
    if cfg is None:
        settings = PostgreSQLConnectorSettings()
    else:
        settings = PostgreSQLConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.ENABLE_DATASOURCE_TEMPLATE,
        )
    return dict(POSTGRESQL=settings)


class PostgreSQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = PostgreSQLConnectorSettings
    fallback = postgresql_settings_fallback
