from __future__ import annotations

import asyncio
import logging

import pytest

from dl_task_processor.executor import Executor
from dl_task_processor.processor import (
    LocalProcessorImpl,
    TaskProcessor,
)
from dl_task_processor.state import (
    BITaskStateImpl,
    TaskState,
)
from dl_task_processor_tests.utils import (
    REGISTRY,
    LocalContextFab,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def loop(event_loop):
    """
    Preventing creation of new loop by `aiohttp.pytest_plugin` loop fixture in favor of pytest-asyncio one
    And set loop pytest-asyncio created loop as default for thread
    """
    asyncio.set_event_loop(event_loop)
    return event_loop


@pytest.fixture(scope="function")
def task_state():
    return TaskState(BITaskStateImpl())


@pytest.fixture(scope="function")
async def task_processor_local_client(loop, task_state):
    context = await LocalContextFab().make()
    executor = Executor(context=context, state=task_state, registry=REGISTRY)
    impl = LocalProcessorImpl(executor)
    processor = TaskProcessor(impl=impl, state=task_state)
    LOGGER.info("Local TP is ready")
    yield processor
    await LocalContextFab().tear_down(context)
