from typing import Optional

import attr

from dl_configs.settings_submodels import RedisSettings


@attr.s
class RQECachesSetting:
    redis_settings: Optional[RedisSettings] = attr.ib(default=None)
    caches_ttl: Optional[int] = attr.ib(default=None)
