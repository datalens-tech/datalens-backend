from __future__ import annotations


import logging

import asyncio
import redis.asyncio
import pytest

from bi_configs.enums import AppType, RedisMode
from bi_configs.settings_submodels import RedisSettings

from bi_task_processor.processor import TaskProcessor, ARQProcessorImpl, LocalProcessorImpl
from bi_task_processor.state import TaskState, BITaskStateImpl
from bi_task_processor.executor import Executor
from bi_task_processor.arq_wrapper import create_redis_pool, create_arq_redis_settings
from bi_task_processor.worker import ArqWorkerTestWrapper

from bi_test_project_worker.app import create_worker, ContextFab
from bi_test_project_worker.settings import TestProjectWorkerSettings
from bi_test_project_worker.tasks import REGISTRY


LOGGER = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def loop(event_loop):
    """
    Preventing creation of new loop by `aiohttp.pytest_plugin` loop fixture in favor of pytest-asyncio one
    And set loop pytest-asyncio created loop as default for thread
    """
    asyncio.set_event_loop(event_loop)
    return event_loop


@pytest.fixture(scope='function')
def redis_settings():
    return RedisSettings(
        MODE=RedisMode.single_host,
        CLUSTER_NAME='',
        HOSTS=('127.0.0.1',),
        PORT=51319,
        DB=10,
    )


@pytest.fixture(scope='function')
def redis_cli(redis_settings):
    return redis.asyncio.Redis(
        host=redis_settings.HOSTS[0],
        port=redis_settings.PORT,
        db=redis_settings.DB,
        password=redis_settings.PASSWORD,
    )


@pytest.fixture(scope='function')
def task_processor_settings(redis_settings):
    settings = TestProjectWorkerSettings(
        APP_TYPE=AppType.TESTS,
        REDIS=redis_settings,
        SENTRY_DSN=None,
    )
    yield settings


@pytest.fixture(scope='function')
async def redis_pool(task_processor_settings):
    pool = await create_redis_pool(
        create_arq_redis_settings(task_processor_settings.REDIS),
    )
    yield pool
    await pool.close()


@pytest.fixture(scope='function')
def task_state():
    return TaskState(BITaskStateImpl())


@pytest.fixture(scope='function')
async def task_processor_arq_worker(loop, task_processor_settings, task_state):
    settings = task_processor_settings

    LOGGER.info('Set up worker')
    worker = create_worker(settings, state=task_state)
    wrapper = ArqWorkerTestWrapper(loop=loop, worker=worker)
    yield await wrapper.start()
    await wrapper.stop()


@pytest.fixture(scope='function')
def task_processor_arq_client(loop, task_processor_arq_worker, redis_pool, task_state):
    impl = ARQProcessorImpl(redis_pool)
    p = TaskProcessor(impl=impl, state=task_state)
    LOGGER.info('Arq TP is ready')
    return p


@pytest.fixture(scope='function')
async def task_processor_local_client(loop, task_state):
    context = await ContextFab().make()
    executor = Executor(context=context, state=task_state, registry=REGISTRY)
    impl = LocalProcessorImpl(executor)
    processor = TaskProcessor(impl=impl, state=task_state)
    LOGGER.info('Local TP is ready')
    yield processor
    await ContextFab().tear_down(context)
