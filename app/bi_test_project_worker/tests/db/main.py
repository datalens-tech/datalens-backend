import pytest
import logging

from bi_test_project_task_interface.tasks import SomeTask
from bi_task_processor.state import wait_task


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_arq(task_processor_arq_client, task_state):
    LOGGER.info('hello')
    task = await task_processor_arq_client.schedule(SomeTask(foo='123'))
    assert await wait_task(task, task_state) == ['scheduled', 'started', 'success']


@pytest.mark.asyncio
async def test_local(task_processor_local_client, task_state):
    LOGGER.info('hello')
    task = await task_processor_local_client.schedule(SomeTask(foo='123'))
    assert await wait_task(task, task_state) == ['scheduled', 'started', 'success']
