from typing import Optional

import attr

from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.utils import split_by_comma
import dl_settings


@attr.s(frozen=True)
class DeprecatedRQESettings:
    CORE_CONNECTOR_WHITELIST: Optional[list[str]] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "list[str] | None")  [assignment]
        "CORE_CONNECTOR_WHITELIST",
        env_var_converter=lambda s: list(split_by_comma(s)),
        missing=None,
        fallback_cfg_key="CORE_CONNECTOR_WHITELIST",
    )
    RQE_SECRET_KEY: Optional[tuple[str]] = s_attrib(  # type: ignore  # 2025-09-11 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "tuple[str] | None")  [assignment]
        "RQE_SECRET_KEY",
        missing=None,
        sensitive=True,
        env_var_converter=split_by_comma,
    )
    FORBID_PRIVATE_ADDRESSES: bool = s_attrib("FORBID_PRIVATE_ADDRESSES", missing=False)  # type: ignore


class RQESettings(dl_settings.BaseRootSettingsWithFallback):
    ...
