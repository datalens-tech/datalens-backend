import attr
import logging

from bi_task_processor.context import BaseContext
from bi_task_processor.task import (
    TaskRegistry,
    TaskName,
    TaskInstance,
    RunID,
    LoggerFields,
    TaskResult,
    Fail,
    Success,
    Retry,
)
from bi_task_processor.state import TaskState
from bi_app_tools.ylog.context import log_context
from bi_app_tools.profiling_base import GenericProfiler


LOGGER = logging.getLogger(__name__)


@attr.s
class Executor:
    _registry: TaskRegistry = attr.ib()
    _state: TaskState = attr.ib()
    _context: BaseContext = attr.ib()

    def _update_status(self, task: TaskInstance, task_result: TaskResult) -> None:
        if isinstance(task_result, Fail):
            LOGGER.info('Task has been failed')
            self._state.set_failed(task)
        elif isinstance(task_result, Success):
            LOGGER.info('Finish task')
            self._state.set_success(task)
        elif isinstance(task_result, Retry):
            LOGGER.info('Task needs to be retried')
            self._state.set_retry(task)
        else:
            LOGGER.warning('Unknown result type %s', task_result)

    async def run_job(self, task: TaskInstance) -> TaskResult:
        run_id = RunID.make()
        logger_extra_info = {
            LoggerFields.task_name.name: task.name,
            LoggerFields.task_params.name: task.params,
            LoggerFields.task_instance_id.name: task.instance_id.to_str(),
            LoggerFields.task_run_id.name: run_id.to_str(),
            LoggerFields.request_id.name: task.request_id,
        }
        with log_context(**logger_extra_info):
            LOGGER.info('Init task %s', task.instance_id.to_str())
            task_cls = self._registry.get_task(TaskName(task.name))
            executor_task = task_cls.from_params(
                ctx=self._context,
                params=task.params,
                instance_id=task.instance_id,
                run_id=run_id,
                request_id=task.request_id,
            )
            self._state.set_started(task)
            try:
                LOGGER.info('Run task %s', task.instance_id.to_str())
                with GenericProfiler('run-task-processor-task'):
                    task_result = await executor_task.run()
            except Exception:
                # let it be in sentry
                LOGGER.error('Task has raised an exception %s', task.instance_id.to_str(), exc_info=True)
                task_result = Fail()
            if isinstance(task_result, Retry):
                LOGGER.info(
                    'Task %s are going to be rescheduled %s from %s',
                    task.instance_id.to_str(),
                    task.attempt,
                    task_result.attempts,
                )
                if task.attempt >= task_result.attempts - 1:  # the 1st attempt must be number 0 because we are programmers
                    LOGGER.info('It was the last chance for task %s', task.instance_id.to_str())
                    task_result = Fail()
            self._update_status(
                task_result=task_result,
                task=task,
            )
            return task_result


@attr.s
class ExecutorFabric:
    _registry: TaskRegistry = attr.ib()
    _state: TaskState = attr.ib()

    async def make(self, context: BaseContext) -> Executor:
        return Executor(
            registry=self._registry,
            state=self._state,
            context=context,
        )
