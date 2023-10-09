from typing import Optional

import attr

from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.utils import split_by_comma


@attr.s(frozen=True)
class RQESettings:
    CORE_CONNECTOR_WHITELIST: Optional[list[str]] = s_attrib(  # type: ignore
        "CORE_CONNECTOR_WHITELIST",
        env_var_converter=lambda s: list(split_by_comma(s)),
        fallback_cfg_key="RQE_CORE_CONNECTOR_WHITELIST",
        missing=None,
    )
    HMAC_KEY: Optional[str] = s_attrib(  # type: ignore
        "HMAC_KEY",
        fallback_cfg_key="RQE_SECRET_KEY",
        missing=None,
    )
    HMAC_KEY_LEGACY: Optional[str] = s_attrib(  # type: ignore  # TODO: migrate to HMAC_KEY; see remap_env
        "HMAC_KEY_LEGACY",
        fallback_cfg_key="EXT_QUERY_EXECUTER_SECRET_KEY",
        missing=None,
    )
