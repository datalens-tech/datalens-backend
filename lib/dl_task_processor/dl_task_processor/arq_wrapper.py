import asyncio
import logging
from typing import (
    Any,
    Dict,
    Optional,
    Protocol,
    TypeAlias,
    runtime_checkable,
)

from arq import (
    create_pool,
    cron,
)
from arq import ArqRedis
from arq import Retry as ArqRetry
from arq.connections import RedisSettings as ArqRedisSettings
from arq.cron import CronJob as _CronJob
import attr

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


EXECUTOR_KEY = "bi_executor"

LOGGER = logging.getLogger(__name__)


CronTask: TypeAlias = _CronJob


@attr.s
class ArqCronWrapper:
    _task: BaseTaskMeta = attr.ib()
    __qualname__ = "ArqCronWrapper"
    # special asyncio marker; because of asyncio.iscoroutinefunction in the arq core
    _is_coroutine = asyncio.coroutines._is_coroutine  # type: ignore  # 2024-01-24 # TODO: Module has no attribute "_is_coroutine"; maybe "iscoroutine" or "coroutine"?  [attr-defined]

    async def __call__(self, ctx: Dict[Any, Any], *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
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
    LOGGER.info("Creating redis pool for an arq worker on %s, db=%s", settings.host, settings.database)
    return await create_pool(settings)


@runtime_checkable
class _BIRedisSettings(Protocol):
    """
    dl_configs.settings_submodels.RedisSettings-like object
    """

    MODE: RedisMode
    HOSTS: tuple[str, ...]
    PORT: int
    PASSWORD: str
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


async def arq_base_task(context: Dict, params: Dict) -> None:
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
