from dl_configs.enums import RedisMode
from dl_constants.enums import RedisInstanceKind
from dl_core.aio.web_app_services.redis import (
    RedisBaseService,
    RedisSentinelService,
    SingleHostSimpleRedisService,
)
from dl_core.utils import make_url
from dl_file_uploader_lib.settings import FileUploaderBaseSettings


def init_redis_service(settings: FileUploaderBaseSettings) -> RedisBaseService:
    redis_service: RedisBaseService
    if settings.REDIS_APP.MODE == RedisMode.single_host:
        assert len(settings.REDIS_APP.HOSTS) == 1
        redis_service = SingleHostSimpleRedisService(
            url=make_url(
                protocol="rediss" if settings.REDIS_APP.SSL else "redis",
                host=settings.REDIS_APP.HOSTS[0],
                port=settings.REDIS_APP.PORT,
                path=str(settings.REDIS_APP.DB),
            ),
            password=settings.REDIS_APP.PASSWORD,
            instance_kind=RedisInstanceKind.persistent,
            ssl=settings.REDIS_APP.SSL,
        )
    elif settings.REDIS_APP.MODE == RedisMode.sentinel:
        redis_service = RedisSentinelService(
            sentinel_hosts=settings.REDIS_APP.HOSTS,
            sentinel_port=settings.REDIS_APP.PORT,
            namespace=settings.REDIS_APP.CLUSTER_NAME,
            password=settings.REDIS_APP.PASSWORD,
            db=settings.REDIS_APP.DB,
            instance_kind=RedisInstanceKind.persistent,
            ssl=settings.REDIS_APP.SSL,
        )
    else:
        raise ValueError(f"Unknown redis mode {settings.REDIS_APP.MODE}")
    return redis_service
