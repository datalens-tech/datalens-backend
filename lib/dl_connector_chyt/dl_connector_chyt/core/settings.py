from typing import (
    Annotated,
    ClassVar,
    Optional,
)

import attr
import pydantic
import pydantic_settings

from dl_configs.connectors_settings import DeprecatedConnectorSettingsBase
from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.utils import split_by_comma
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core.connectors.settings.mixins import (
    DatasourceTemplateSettingsMixin,
    DeprecatedDatasourceTemplateSettingsMixin,
)
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)
import dl_settings

from dl_connector_chyt.core.constants import CONNECTION_TYPE_CHYT


@attr.s(frozen=True)
class DeprecatedCHYTConnectorSettings(DeprecatedConnectorSettingsBase, DeprecatedDatasourceTemplateSettingsMixin):
    """
    PUBLIC_CLIQUES:     cliques which usage is discouraged due to their high load by other users
    FORBIDDEN_CLIQUES:  cliques that cannot be used at all
    DEFAULT_CLIQUE:     clique that is set by default in the connection form
    """

    PUBLIC_CLIQUES: tuple[str] = s_attrib("PUBLIC_CLIQUES", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore
    FORBIDDEN_CLIQUES: tuple[str] = s_attrib("FORBIDDEN_CLIQUES", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore
    DEFAULT_CLIQUE: Optional[str] = s_attrib("DEFAULT_CLIQUE", missing=None)  # type: ignore


def chyt_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, DeprecatedConnectorSettingsBase]:
    cfg = get_connectors_settings_config(full_cfg, object_like_config_key="CHYT")
    if cfg is None:
        settings = DeprecatedCHYTConnectorSettings()
    else:
        settings = DeprecatedCHYTConnectorSettings(  # type: ignore
            PUBLIC_CLIQUES=tuple(cfg.CONN_CHYT_PUBLIC_CLIQUES),
            FORBIDDEN_CLIQUES=tuple(cfg.CONN_CHYT_FORBIDDEN_CLIQUES),
            DEFAULT_CLIQUE=cfg.CONN_CHYT_DEFAULT_CLIQUE,
            ENABLE_DATASOURCE_TEMPLATE=cfg.get("ENABLE_DATASOURCE_TEMPLATE", True),
        )
    return dict(CHYT=settings)


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
    ]
    FORBIDDEN_CLIQUES: Annotated[
        tuple[str, ...],
        dl_settings.split_validator(","),
        pydantic_settings.NoDecode,
    ]
    DEFAULT_CLIQUE: str | None = None

    root_fallback_env_keys: ClassVar[dict[str, str]] = {
        "CONNECTORS__CHYT__PUBLIC_CLIQUES": "CONNECTORS_CHYT_PUBLIC_CLIQUES",
        "CONNECTORS__CHYT__FORBIDDEN_CLIQUES": "CONNECTORS_CHYT_FORBIDDEN_CLIQUES",
        "CONNECTORS__CHYT__DEFAULT_CLIQUE": "CONNECTORS_CHYT_DEFAULT_CLIQUE",
        "CONNECTORS__CHYT__ENABLE_DATASOURCE_TEMPLATE": "CONNECTORS_CHYT_ENABLE_DATASOURCE_TEMPLATE",
    }


class CHYTSettingDefinition(ConnectorSettingsDefinition):
    settings_class = DeprecatedCHYTConnectorSettings
    fallback = chyt_settings_fallback

    pydantic_settings_class = CHYTConnectorSettings
