from __future__ import annotations

from typing import Optional

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_cache_engine.primitives import CacheTTLConfig


@attr.s(auto_attribs=True, frozen=True)
class SROptions:
    rci: RequestContextInfo
    with_caches: bool = False
    cache_save_background: Optional[bool] = False
    default_caches_ttl_config: CacheTTLConfig = CacheTTLConfig()
    with_compeng_pg: bool = False
