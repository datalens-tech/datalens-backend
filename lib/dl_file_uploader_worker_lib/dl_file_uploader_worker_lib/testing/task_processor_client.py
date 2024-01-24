import asyncio
import contextlib
import logging
from typing import AsyncGenerator

import arq

from dl_file_uploader_worker_lib.app import FileUploaderContextFab
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings
from dl_file_uploader_worker_lib.tasks import REGISTRY
from dl_file_uploader_worker_lib.testing.app_factory import TestingFileUploaderWorkerFactory
from dl_task_processor.arq_wrapper import (
    create_arq_redis_settings,
    create_redis_pool,
)
from dl_task_processor.executor import Executor
from dl_task_processor.processor import (
    ARQProcessorImpl,
    LocalProcessorImpl,
    TaskProcessor,
)
from dl_task_processor.state import TaskState
from dl_task_processor.worker import (
    ArqWorker,
    ArqWorkerTestWrapper,
)


LOGGER = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def task_processor_arq_worker(
    loop: asyncio.AbstractEventLoop,
    task_state: TaskState,
    file_uploader_worker_settings: FileUploaderWorkerSettings,
    ca_data: bytes,
) -> AsyncGenerator[ArqWorker, None]:
    LOGGER.info("Set up worker")
    worker = TestingFileUploaderWorkerFactory(
        settings=file_uploader_worker_settings,
        ca_data=ca_data,
    ).create_worker(state=task_state)
    wrapper = ArqWorkerTestWrapper(loop=loop, worker=worker)
    try:
        yield await wrapper.start()
    finally:
        await wrapper.stop()


@contextlib.asynccontextmanager
async def task_processor_arq_client(
    loop: asyncio.AbstractEventLoop,
    redis_pool: arq.ArqRedis,
    task_state: TaskState,
    file_uploader_worker_settings: FileUploaderWorkerSettings,
    ca_data: bytes,
) -> AsyncGenerator[TaskProcessor, None]:
    async with task_processor_arq_worker(
        loop,
        task_state,
        file_uploader_worker_settings,
        ca_data=ca_data,
    ):
        impl = ARQProcessorImpl(redis_pool)
        p = TaskProcessor(impl=impl, state=task_state)
        LOGGER.info("Arq TP is ready")
        yield p


@contextlib.asynccontextmanager
async def task_processor_local_client(
    task_state: TaskState,
    file_uploader_worker_settings: FileUploaderWorkerSettings,
    ca_data: bytes,
) -> AsyncGenerator[TaskProcessor, None]:
    context_fab = FileUploaderContextFab(
        settings=file_uploader_worker_settings,
        ca_data=ca_data,
    )
    context = await context_fab.make()
    executor = Executor(context=context, state=task_state, registry=REGISTRY)
    impl = LocalProcessorImpl(executor)
    processor = TaskProcessor(impl=impl, state=task_state)
    LOGGER.info("Local TP is ready")
    try:
        yield processor
    finally:
        await context_fab.tear_down(context)


@contextlib.asynccontextmanager
async def get_task_processor_client(
    client_type: str,
    loop: asyncio.AbstractEventLoop,
    task_state: TaskState,
    file_uploader_worker_settings: FileUploaderWorkerSettings,
    ca_data: bytes,
) -> AsyncGenerator[TaskProcessor, None]:
    if client_type == "arq":
        arq_redis_settings = create_arq_redis_settings(file_uploader_worker_settings.REDIS_ARQ)
        pool = await create_redis_pool(arq_redis_settings)
        async with task_processor_arq_client(
            loop,
            pool,
            task_state,
            file_uploader_worker_settings,
            ca_data=ca_data,
        ) as tp:
            try:
                yield tp
            finally:
                await pool.close()
    elif client_type == "local":
        async with task_processor_local_client(
            task_state,
            file_uploader_worker_settings,
            ca_data=ca_data,
        ) as tp:
            yield tp
