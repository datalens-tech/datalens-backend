import logging

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List

from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from bi_core.logging_config import configure_logging
from bi_task_processor.arq_wrapper import create_redis_pool, create_arq_redis_settings
from bi_task_processor.controller import Cli

from bi_task_processor.arq_wrapper import make_cron_task, CronSchedule
from bi_task_processor.processor import ARQProcessorImpl, TaskProcessor
from bi_task_processor.state import TaskState, DummyStateImpl
from bi_task_processor.worker import ArqWorker, WorkerSettings, HealthChecker
from bi_task_processor.executor import ExecutorFabric
from bi_task_processor.context import BaseContextFabric
from bi_test_project_task_interface.context import Context

from bi_test_project_worker.tasks import REGISTRY
from bi_test_project_worker.settings import TestProjectWorkerSettings

from bi_test_project_task_interface.tasks import PeriodicTask


LOGGER = logging.getLogger(__name__)


class ContextFab(BaseContextFabric):
    async def make(self) -> Context:
        return Context(
            tpe=ThreadPoolExecutor(),
        )

    async def tear_down(self, inst: Context):
        inst.tpe.shutdown()


def create_worker(settings: TestProjectWorkerSettings, state: TaskState = None) -> ArqWorker:
    if state is None:
        state = TaskState(DummyStateImpl())
    worker = ArqWorker(
        redis_settings=create_arq_redis_settings(settings.REDIS),
        executor_fab=ExecutorFabric(
            registry=REGISTRY,
            state=state,
        ),
        context_fab=ContextFab(),
        worker_settings=WorkerSettings(),
        cron_tasks=[
            make_cron_task(
                # is_cron is a just param, not a special value
                PeriodicTask(is_cron=True),
                schedule=CronSchedule(minute={30}),
            )
        ],
    )
    return worker


def run_standalone_worker() -> None:
    loop = asyncio.get_event_loop()
    settings = load_settings_from_env_with_fallback(TestProjectWorkerSettings)
    worker = create_worker(settings)
    configure_logging(app_name='bi_test_project_worker', sentry_dsn=settings.SENTRY_DSN)
    worker_task = loop.create_task(worker.start())
    try:
        loop.run_forever()
    except asyncio.CancelledError:  # pragma: no cover
        # happens on shutdown, fine
        pass
    finally:
        worker_task.cancel()
        loop.run_until_complete(worker.stop())
        loop.close()


def run_health_check() -> None:
    loop = asyncio.get_event_loop()
    settings = load_settings_from_env_with_fallback(TestProjectWorkerSettings)
    configure_logging(app_name='bi_test_project_worker_health_check', sentry_dsn=settings.SENTRY_DSN)
    worker = create_worker(settings)
    health_checker = HealthChecker(worker)
    loop.run_until_complete(health_checker.check())


def run_cli(args: List) -> None:
    parsed_args = Cli.parse_params(args)
    loop = asyncio.get_event_loop()
    settings = load_settings_from_env_with_fallback(TestProjectWorkerSettings)
    configure_logging(app_name='bi_file_uploader_cli')
    redis_pool = loop.run_until_complete(
        create_redis_pool(
            create_arq_redis_settings(settings.REDIS),
        ),
    )
    impl = ARQProcessorImpl(redis_pool)
    state = TaskState(DummyStateImpl())
    processor = TaskProcessor(impl=impl, state=state)
    cli = Cli(processor=processor, registry=REGISTRY)
    loop.run_until_complete(cli.run(parsed_args))


if __name__ == '__main__':
    run_standalone_worker()
