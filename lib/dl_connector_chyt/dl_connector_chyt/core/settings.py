from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
)
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.utils import split_by_comma
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class CHYTConnectorSettings(ConnectorSettingsBase):
    """
    PUBLIC_CLIQUES:     cliques which usage is discouraged due to their high load by other users
    FORBIDDEN_CLIQUES:  cliques that cannot be used at all
    DEFAULT_CLIQUE:     clique that is set by default in the connection form
    """

    PUBLIC_CLIQUES: tuple[str] = s_attrib("PUBLIC_CLIQUES", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore
    FORBIDDEN_CLIQUES: tuple[str] = s_attrib("FORBIDDEN_CLIQUES", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore
    DEFAULT_CLIQUE: Optional[str] = s_attrib("DEFAULT_CLIQUE", missing=None)  # type: ignore


class ConnectorsDataCHYTBase(ConnectorsDataBase):
    CONN_CHYT_PUBLIC_CLIQUES: ClassVar[Optional[tuple[str]]] = None
    CONN_CHYT_FORBIDDEN_CLIQUES: ClassVar[Optional[tuple[str]]] = None
    CONN_CHYT_DEFAULT_CLIQUE: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return "CHYT"


def chyt_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg,
        object_like_config_key="CHYT",
        connector_data_class=ConnectorsDataCHYTBase,
    )
    if cfg is None:
        return {}
    return dict(
        CHYT=CHYTConnectorSettings(  # type: ignore
            PUBLIC_CLIQUES=tuple(cfg.CONN_CHYT_PUBLIC_CLIQUES),  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "tuple" has incompatible type "Any | tuple[str] | None"; expected "Iterable[Any]"  [arg-type]
            FORBIDDEN_CLIQUES=tuple(cfg.CONN_CHYT_FORBIDDEN_CLIQUES),  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "tuple" has incompatible type "Any | tuple[str] | None"; expected "Iterable[Any]"  [arg-type]
            DEFAULT_CLIQUE=cfg.CONN_CHYT_DEFAULT_CLIQUE,
        )
    )


class CHYTSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYTConnectorSettings
    fallback = chyt_settings_fallback
