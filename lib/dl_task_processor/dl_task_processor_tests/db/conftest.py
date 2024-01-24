from __future__ import annotations

import logging

import pytest

from dl_configs.enums import RedisMode
from dl_configs.settings_submodels import RedisSettings
from dl_task_processor.arq_wrapper import (
    create_arq_redis_settings,
    create_redis_pool,
)
from dl_task_processor.executor import ExecutorFabric
from dl_task_processor.processor import (
    ARQProcessorImpl,
    TaskProcessor,
)
from dl_task_processor.state import (
    BITaskStateImpl,
    TaskState,
)
from dl_task_processor.worker import (
    ArqWorker,
    ArqWorkerTestWrapper,
    WorkerSettings,
)
from dl_task_processor_tests.utils import (
    REGISTRY,
    ARQContextFab,
)
from dl_testing.containers import get_test_container_hostport


LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def redis_settings(request):
    # try to redefine value
    if hasattr(request, "param") and request.param is not None:
        return request.param
    return RedisSettings(
        MODE=RedisMode.single_host,
        CLUSTER_NAME="",
        HOSTS=(get_test_container_hostport("redis", fallback_port=51219).host,),
        PORT=get_test_container_hostport("redis", fallback_port=51219).port,
        DB=10,
    )


@pytest.fixture(scope="function")
async def redis_pool(loop, redis_settings):
    pool = await create_redis_pool(
        create_arq_redis_settings(redis_settings),
    )
    yield pool
    await pool.close()


@pytest.fixture(scope="function")
def task_state():
    return TaskState(BITaskStateImpl())


@pytest.fixture(scope="function")
def worker_settings(request):
    return getattr(request, "param", WorkerSettings())


@pytest.fixture(scope="function")
async def init_task_processor_arq_worker(task_state, redis_settings, worker_settings):
    arq_redis = create_arq_redis_settings(redis_settings)
    worker = ArqWorker(
        redis_settings=arq_redis,
        executor_fab=ExecutorFabric(
            registry=REGISTRY,
            state=task_state,
        ),
        context_fab=ARQContextFab(arq_redis),
        worker_settings=worker_settings,
    )
    yield worker


@pytest.fixture(scope="function")
async def task_processor_arq_worker(loop, init_task_processor_arq_worker):
    wrapper = ArqWorkerTestWrapper(loop=loop, worker=init_task_processor_arq_worker)
    yield await wrapper.start()
    await wrapper.stop()


@pytest.fixture(scope="function")
def task_processor_arq_client(loop, task_processor_arq_worker, redis_pool, redis_settings, task_state):
    impl = ARQProcessorImpl(redis_pool)
    p = TaskProcessor(impl=impl, state=task_state)
    LOGGER.info("Arq TP is ready")
    return p


@pytest.fixture(scope="function", params=["arq", "local"])
def task_processor_client(request, task_processor_arq_client, task_processor_local_client):
    if request.param == "arq":
        yield task_processor_arq_client
    elif request.param == "local":
        yield task_processor_local_client
