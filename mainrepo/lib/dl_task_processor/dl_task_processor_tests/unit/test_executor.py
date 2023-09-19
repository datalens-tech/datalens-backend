import logging

import pytest

from dl_task_processor.executor import Executor
from dl_task_processor.task import (
    Fail,
    InstanceID,
    LoggerFields,
    Retry,
    Success,
    TaskInstance,
)
from dl_task_processor_tests.utils import (
    BROKEN_MARK,
    REGISTRY,
    BrokenTaskInterface,
    LocalContextFab,
    RetryTaskInterface,
    SomeTaskInterface,
)


LOGGER = logging.getLogger(__name__)
DEFAULT_REQ_ID = "reqid-123"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_meta", "result"),
    [
        (SomeTaskInterface(foo="as"), Success()),
        (RetryTaskInterface(foobar="asd"), Retry(delay=1, backoff=1, attempts=2)),
        (BrokenTaskInterface(bar="as"), Fail()),
    ],
)
async def test_run_task(task_state, task_meta, result):
    context = await LocalContextFab().make()
    executor = Executor(
        context=context,
        registry=REGISTRY,
        state=task_state,
    )
    task = TaskInstance(
        instance_id=InstanceID.make(),
        name=task_meta.name,
        params=task_meta.get_params(),
    )
    task_result = await executor.run_job(task)
    assert task_result == result


@pytest.mark.asyncio
async def test_run_task_logs(task_state, caplog):
    """
    test messages from internal task logger
    """
    caplog.set_level(logging.INFO)
    context = await LocalContextFab().make()
    executor = Executor(
        context=context,
        registry=REGISTRY,
        state=task_state,
    )
    task_meta = BrokenTaskInterface(bar="21")
    task = TaskInstance(
        instance_id=InstanceID.make(),
        name=task_meta.name,
        params=task_meta.get_params(),
        request_id=DEFAULT_REQ_ID,
    )
    caplog.clear()
    task_result = await executor.run_job(task)
    assert task_result == Fail()
    internal_task_message = next(rec for rec in caplog.records if rec.message == BROKEN_MARK)
    assert getattr(internal_task_message, LoggerFields.task_name.name) == task.name
    assert getattr(internal_task_message, LoggerFields.task_instance_id.name) == task.instance_id.to_str()
    assert getattr(internal_task_message, LoggerFields.task_params.name) == task.params
    assert getattr(internal_task_message, LoggerFields.task_run_id.name)
    assert getattr(internal_task_message, LoggerFields.request_id.name) == DEFAULT_REQ_ID
    assert isinstance(getattr(internal_task_message, LoggerFields.task_run_id.name), str)
