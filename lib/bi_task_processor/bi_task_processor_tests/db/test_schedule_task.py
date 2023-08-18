import pytest
import logging

from bi_task_processor.state import wait_task
from bi_task_processor_tests.utils import (
    SomeTaskInterface,
    BrokenTaskInterface,
    RetryTaskInterface,
    TestIdsTaskInterface,
    ScheduleFromTaskTaskInterface,
)


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('task_meta', 'result'),
    [
        (SomeTaskInterface(foo='value1'), ['scheduled', 'started', 'success']),
        (RetryTaskInterface(foobar='value2'), ['scheduled', 'started', 'retry', 'started', 'failed']),
        (BrokenTaskInterface(bar='value3'), ['scheduled', 'started', 'failed']),
    ],
)
async def test_schedule(task_processor_client, task_state, task_meta, result):
    task = await task_processor_client.schedule(task_meta)
    assert await wait_task(task, task_state) == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'request_id',
    [
        'SOME_REQUEST_ID',
        None,
    ],
)
async def test_task_ids(task_processor_client, task_state, request_id):
    # lets check defaults too
    if request_id:
        task_processor_client._request_id = request_id
    task = await task_processor_client.schedule(TestIdsTaskInterface(expected_request_id=request_id))
    # task should selfcheck internal ids
    # like req_id, instance_id and run_id
    assert await wait_task(task, task_state) == ['scheduled', 'started', 'success']


@pytest.mark.asyncio
async def test_schedule_task_by_task(task_processor_client, task_state):
    task = await task_processor_client.schedule(ScheduleFromTaskTaskInterface())
    assert await wait_task(task, task_state) == ['scheduled', 'started', 'success']
