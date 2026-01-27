from typing import ClassVar

import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
    TableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
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


class MSSQLConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_MSSQL.value

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__MSSQL__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_MSSQL_ENABLE_DATASOURCE_TEMPLATE",
        "CONNECTORS__MSSQL__ENABLE_TABLE_DATASOURCE_FORM": "CONNECTORS_MSSQL_ENABLE_TABLE_DATASOURCE_FORM",
    }


class MSSQLSettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedMSSQLConnectorSettings
    fallback = mssql_settings_fallback

    pydantic_settings_class = MSSQLConnectorSettings
