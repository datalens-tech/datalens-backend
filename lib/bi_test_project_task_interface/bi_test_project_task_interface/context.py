import attr

from bi_utils.aio import ContextVarExecutor

from bi_task_processor.context import BaseContext


@attr.s
class Context(BaseContext):
    tpe: ContextVarExecutor = attr.ib()
