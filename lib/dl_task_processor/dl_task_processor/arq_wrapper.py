import asyncio
import functools
import logging
from typing import (
    Any,
    Optional,
    Protocol,
    TypeAlias,
    runtime_checkable,
)

from arq import ArqRedis
from arq import Retry as ArqRetry
from arq import cron
from arq.connections import RedisSettings as ArqRedisSettings
from arq.constants import (
    default_queue_name,
    expires_extra_ms,
)
from arq.cron import CronJob as _CronJob
import attr
from redis import RedisError
from redis.asyncio import Sentinel

from dl_configs.enums import RedisMode
from dl_task_processor.executor import (
    Executor,
    TaskInstance,
)
from dl_task_processor.task import (
    BaseTaskMeta,
    InstanceID,
    Retry,
)


LOGGER = logging.getLogger(__name__)

EXECUTOR_KEY = "bi_executor"
SOCKET_TIMEOUT = 3  # timeout for a single Redis command


CronTask: TypeAlias = _CronJob


@attr.s
class ArqCronWrapper:
    _task: BaseTaskMeta = attr.ib()
    __qualname__ = "ArqCronWrapper"
    # special asyncio marker; because of asyncio.iscoroutinefunction in the arq core
    _is_coroutine = asyncio.coroutines._is_coroutine  # type: ignore  # 2024-01-24 # TODO: Module has no attribute "_is_coroutine"; maybe "iscoroutine" or "coroutine"?  [attr-defined]

    async def __call__(self, ctx: dict[Any, Any], *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        return await arq_base_task(
            ctx,
            # TODO: add common object for params
            params={
                "instance_id": InstanceID.make(),
                "name": self._task.name,
                "task_params": self._task.get_params(with_name=False),
            },
        )


async def create_redis_pool(settings: ArqRedisSettings) -> ArqRedis:
    """
    Note: mostly copy-n-paste from `arq.create_pool`
    The only meaningful change to the upstream version in passing additional timeouts to `Redis` & `Sentinel`
    """

    LOGGER.info("Creating redis pool for an arq worker on %s, db=%s", settings.host, settings.database)

    assert not (
        type(settings.host) is str and settings.sentinel
    ), "str provided for 'host' but 'sentinel' is true; list of sentinels expected"

    if settings.sentinel:
        # def pool_factory(*args: Any, **kwargs: Any) -> ArqRedis:
        def pool_factory(db: int, username: str | None, password: str | None, encoding: str) -> ArqRedis:
            client = Sentinel(
                sentinels=settings.host,
                ssl=settings.ssl,
                socket_connect_timeout=settings.conn_timeout,
                socket_timeout=SOCKET_TIMEOUT,
                db=db,
                username=username,
                password=password,
                encoding=encoding,
            )
            return client.master_for(settings.sentinel_master, redis_class=ArqRedis)

    else:
        pool_factory = functools.partial(
            ArqRedis,
            host=settings.host,
            port=settings.port,
            unix_socket_path=settings.unix_socket_path,
            socket_connect_timeout=settings.conn_timeout,
            socket_timeout=SOCKET_TIMEOUT,
            ssl=settings.ssl,
            ssl_keyfile=settings.ssl_keyfile,
            ssl_certfile=settings.ssl_certfile,
            ssl_cert_reqs=settings.ssl_cert_reqs,
            ssl_ca_certs=settings.ssl_ca_certs,
            ssl_ca_data=settings.ssl_ca_data,
            ssl_check_hostname=settings.ssl_check_hostname,
        )

    retry = 0
    while True:
        try:
            pool = pool_factory(
                db=settings.database,
                username=settings.username,
                password=settings.password,
                encoding="utf8",
            )
            pool.job_serializer = None
            pool.job_deserializer = None
            pool.default_queue_name = default_queue_name
            pool.expires_extra_ms = expires_extra_ms
            await pool.ping()

        except (ConnectionError, OSError, RedisError, asyncio.TimeoutError) as e:
            if retry < settings.conn_retries:
                LOGGER.warning(
                    "redis connection error %s:%s %s %s, %d retries remaining...",
                    settings.host,
                    settings.port,
                    e.__class__.__name__,
                    e,
                    settings.conn_retries - retry,
                )
                await asyncio.sleep(settings.conn_retry_delay)
                retry = retry + 1
            else:
                raise
        else:
            if retry > 0:
                LOGGER.info("redis connection successful")
            return pool


@runtime_checkable
class _BIRedisSettings(Protocol):
    """
    dl_configs.settings_submodels.RedisSettings-like object
    """

    MODE: RedisMode
    HOSTS: tuple[str, ...]
    PORT: int
    PASSWORD: str | None
    DB: int
    CLUSTER_NAME: str
    SSL: Optional[bool]


def create_arq_redis_settings(settings: _BIRedisSettings) -> ArqRedisSettings:
    if settings.MODE == RedisMode.single_host:
        assert len(settings.HOSTS) == 1, f"Multiple hosts value {settings.HOSTS} is restricted for single_host mode"
        return ArqRedisSettings(
            host=settings.HOSTS[0],
            port=settings.PORT,
            password=settings.PASSWORD,
            database=settings.DB,
            ssl=settings.SSL or False,
            conn_timeout=3,
        )
    elif settings.MODE == RedisMode.sentinel:
        redis_targets = [(host, settings.PORT) for host in settings.HOSTS]
        return ArqRedisSettings(
            host=redis_targets,
            password=settings.PASSWORD,
            sentinel=True,
            sentinel_master=settings.CLUSTER_NAME,
            database=settings.DB,
            ssl=settings.SSL or False,
            conn_timeout=3,
        )
    else:
        raise ValueError(f"Unknown redis mode {settings.MODE}")


@attr.s
class CronSchedule:
    """
    second: second(s) to run the job on, 0 - 59
    minute: minute(s) to run the job on, 0 - 59
    hour: hour(s) to run the job on, 0 - 23
    day: day(s) to run the job on, 1 - 31
    weekday: week day(s) to run the job on, 0 - 6 or mon - sun
    month: month(s) to run the job on, 1 - 12
    """

    second: Optional[set[int]] = attr.ib(default=None)
    minute: Optional[set[int]] = attr.ib(default=None)
    hour: Optional[set[int]] = attr.ib(default=None)
    day: Optional[set[int]] = attr.ib(default=None)
    weekday: Optional[set[int]] = attr.ib(default=None)
    month: Optional[set[int]] = attr.ib(default=None)


def make_cron_task(task: BaseTaskMeta, schedule: CronSchedule) -> CronTask:
    cron_task = cron(
        ArqCronWrapper(task=task),
        name=task.name,
        **attr.asdict(schedule),
    )
    return cron_task


async def arq_base_task(context: dict, params: dict) -> None:
    LOGGER.info("Run arq task with params %s", params)
    task_instance = TaskInstance(
        name=params["name"],
        params=params["task_params"],
        instance_id=params["instance_id"],
        request_id=params.get("request_id"),
        attempt=context["job_try"] - 1,  # it starts from 1 o_O
    )
    executor: Executor = context[EXECUTOR_KEY]
    job_result = await executor.run_job(task_instance)
    if isinstance(job_result, Retry):
        # https://arq-docs.helpmanual.io/#retrying-jobs-and-cancellation
        raise ArqRetry(
            defer=job_result.delay * context["job_try"] * job_result.backoff,
        )
