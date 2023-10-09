from typing import Optional

import attr

from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.utils import split_by_comma


@attr.s(frozen=True)
class RQESettings:
    CORE_CONNECTOR_WHITELIST: Optional[list[str]] = s_attrib(  # type: ignore
        "CORE_CONNECTOR_WHITELIST",
        env_var_converter=lambda s: list(split_by_comma(s)),
        missing=None,
    )
    RQE_SECRET_KEY: Optional[str] = s_attrib("RQE_SECRET_KEY", missing=None)  # type: ignore
