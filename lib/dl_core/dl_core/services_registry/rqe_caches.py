import attr

from dl_configs.settings_submodels import RedisSettings


@attr.s
class RQECachesSetting:
    redis_settings: RedisSettings | None = attr.ib(default=None)
    caches_ttl: int | None = attr.ib(default=None)
