from typing import (
    Annotated,
    ClassVar,
)

import pydantic
import pydantic_settings

from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import DatasourceTemplateSettingsMixin
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition
import dl_settings

from dl_connector_chyt.core.constants import CONNECTION_TYPE_CHYT


class CHYTConnectorSettings(ConnectorSettings, DatasourceTemplateSettingsMixin):
    """
    PUBLIC_CLIQUES:     cliques which usage is discouraged due to their high load by other users
    FORBIDDEN_CLIQUES:  cliques that cannot be used at all
    DEFAULT_CLIQUE:     clique that is set by default in the connection form
    """

    type: str = CONNECTION_TYPE_CHYT.value

    model_config = pydantic.ConfigDict(alias_generator=dl_settings.prefix_alias_generator("CONN_CHYT_"))

    PUBLIC_CLIQUES: Annotated[
        tuple[str, ...],
        dl_settings.split_validator(","),
        pydantic_settings.NoDecode,
    ] = pydantic.Field(default_factory=tuple)
    FORBIDDEN_CLIQUES: Annotated[
        tuple[str, ...],
        dl_settings.split_validator(","),
        pydantic_settings.NoDecode,
    ] = pydantic.Field(default_factory=tuple)
    DEFAULT_CLIQUE: str | None = None

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__CHYT__PUBLIC_CLIQUES": "CONNECTORS_CHYT_PUBLIC_CLIQUES",
        "CONNECTORS__CHYT__FORBIDDEN_CLIQUES": "CONNECTORS_CHYT_FORBIDDEN_CLIQUES",
        "CONNECTORS__CHYT__DEFAULT_CLIQUE": "CONNECTORS_CHYT_DEFAULT_CLIQUE",
        "CONNECTORS__CHYT__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_CHYT_ENABLE_DATASOURCE_TEMPLATE",
    }


class CHYTSettingDefinition(ConnectorSettingsDefinition):
    pydantic_settings_class = CHYTConnectorSettings
