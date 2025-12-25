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

from dl_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM


@attr.s(frozen=True)
class DeprecatedGreenplumConnectorSettings(
    DeprecatedConnectorSettingsBase,
    DeprecatedDatasourceTemplateSettingsMixin,
    DeprecatedTableDatasourceSettingsMixin,
):
    pass


def greenplum_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="GREENPLUM")
    if cfg is None:
        settings = DeprecatedGreenplumConnectorSettings()
    else:
        settings = DeprecatedGreenplumConnectorSettings(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
            ENABLE_TABLE_DATASOURCE_FORM=cfg.get("ENABLE_TABLE_DATASOURCE_FORM", True),
        )
    return dict(GREENPLUM=settings)


class GreenplumConnectorSettings(ConnectorSettings, TableDatasourceSettingsMixin, DatasourceTemplateSettingsMixin):
    type: str = CONNECTION_TYPE_GREENPLUM.value


class GreenplumSettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedGreenplumConnectorSettings
    fallback = greenplum_settings_fallback
    pydantic_settings_class = GreenplumConnectorSettings
