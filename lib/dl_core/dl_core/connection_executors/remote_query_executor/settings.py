from typing import Optional

import attr

from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.utils import split_by_comma


@attr.s(frozen=True)
class RQESettings:
    CORE_CONNECTOR_WHITELIST: Optional[list[str]] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "list[str] | None")  [assignment]
        "CORE_CONNECTOR_WHITELIST",
        env_var_converter=lambda s: list(split_by_comma(s)),
        missing=None,
        fallback_cfg_key="CORE_CONNECTOR_WHITELIST",
    )
    RQE_SECRET_KEY: Optional[str] = s_attrib("RQE_SECRET_KEY", missing=None, sensitive=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
