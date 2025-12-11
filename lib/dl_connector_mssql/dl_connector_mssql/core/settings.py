import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.mixins import (
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import (
    DeprecatedConnectorSettingsDefinition,
    get_connectors_settings_config,
)
from dl_core.connectors.settings.pydantic.base import ConnectorSettings
from dl_core.connectors.settings.pydantic.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)

from dl_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL


@attr.s(frozen=True)
class DeprecatedMSSQLConnectorSettings(
    DeprecatedConnectorSettingsBase,
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
):
    pass


def mssql_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="MSSQL")
    if cfg is None:
        settings = DeprecatedMSSQLConnectorSettings()
    else:
        settings = DeprecatedMSSQLConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", True),
        )
    return dict(MSSQL=settings)


class DeprecatedMSSQLSettingDefinition(DeprecatedConnectorSettingsDefinition):
    settings_class = DeprecatedMSSQLConnectorSettings
    fallback = mssql_settings_fallback


class MSSQLConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_MSSQL.value
