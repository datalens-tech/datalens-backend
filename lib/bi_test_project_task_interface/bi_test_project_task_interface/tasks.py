import attr

from bi_task_processor.task import TaskName, BaseTaskMeta


@attr.s
class SomeTask(BaseTaskMeta):
    name = TaskName('some_task')
    foo: str = attr.ib()


@attr.s
class AnotherTask(BaseTaskMeta):
    name = TaskName('another_task')
    bar: str = attr.ib()


@attr.s
class BrokenTask(BaseTaskMeta):
    name = TaskName('broken_task')
    foobar: str = attr.ib()


@attr.s
class PeriodicTask(BaseTaskMeta):
    name = TaskName('cron_task')
    is_cron: bool = attr.ib()
