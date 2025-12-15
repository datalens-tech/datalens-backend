import attr

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_core.connectors.settings.mixins import (
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
)
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    TableDatasourceSettingsMixin,
)

from dl_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE


@attr.s(frozen=True)
class DeprecatedOracleConnectorSettings(
    DeprecatedConnectorSettingsBase,
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
):
    pass


def oracle_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="ORACLE")
    if cfg is None:
        settings = DeprecatedOracleConnectorSettings()
    else:
        settings = DeprecatedOracleConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", True),
        )
    return dict(ORACLE=settings)


class OracleSettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedOracleConnectorSettings
    fallback = oracle_settings_fallback


class OracleConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_ORACLE.value
