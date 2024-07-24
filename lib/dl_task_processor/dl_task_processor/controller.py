import argparse
import json
from typing import (
    Iterable,
    Optional,
    Type,
)

import attr

from dl_task_processor.processor import TaskProcessor
from dl_task_processor.task import (
    BaseTaskMeta,
    TaskInstance,
    TaskName,
    TaskRegistry,
)


@attr.s
class Controller:
    """
    Some user interface example

    ==As CLI==
    cli task info // whole list
    cli task info --name foo
    cli task blacklist --name foo // why not?
    cli instance info --name bla --id bla
    cli instance schedule --name foo --params a=1 b=2 c=3 // what about dicts or lists?
    cli instance stop --id asdasda

    ==As IPython
    c = Controller(tp, r)
    for task in c.tasks.filter():
        print(task)

    task_meta = c.tasks.meta(name='foo')
    task = task_meta(foo='bar')
    instance = await c.instances.schedule(task, start_time=datetime.datetime(...))
    scheduled_instance = await c.instances.get(name='foo')
    async for instance in c.instances.filter():
        await instance.instances.stop(scheduled_instance)

    ===As API===
    GET (and filters)
    /tasks
    GET
    /tasks/<>/info
    GET (and filters)
    POST (schedule new)
    /instances
    GET
    /instances/<>/state
    GET
    POST (change state) -- stop, reschedule
    /instances/<>/actions
    """

    _processor: TaskProcessor = attr.ib()
    _registry: TaskRegistry = attr.ib()

    @property
    def tasks(self) -> "TaskController":
        return TaskController(registry=self._registry)

    @property
    def instances(self) -> "InstanceController":
        return InstanceController(processor=self._processor)


@attr.s
class TaskController:
    _registry: TaskRegistry = attr.ib()

    def filter(self, name: TaskName) -> Iterable[Type[BaseTaskMeta]]:
        return self._registry.filter_task_meta(name)

    def meta(self, name: TaskName) -> Type[BaseTaskMeta]:
        return self._registry.get_task_meta(name)


@attr.s
class InstanceController:
    _processor: TaskProcessor = attr.ib()

    async def schedule(self, task: BaseTaskMeta) -> TaskInstance:
        scheduled_task = await self._processor.schedule(task)
        return scheduled_task

    async def get(self, name: TaskName) -> TaskInstance:
        raise NotImplementedError()

    async def stop(self, name: TaskName) -> TaskInstance:
        raise NotImplementedError()

    async def filter(self) -> Iterable[TaskInstance]:
        raise NotImplementedError()


@attr.s
class Cli:
    _processor: TaskProcessor = attr.ib()
    _registry: TaskRegistry = attr.ib()

    @classmethod
    def parse_params(cls, args: Optional[list[str]] = None) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Task Processor controller")
        subparsers = parser.add_subparsers(dest="subparser")

        # task parsers
        parser_tasks = subparsers.add_parser("task")
        subparsers_tasks = parser_tasks.add_subparsers(dest="action")
        # task filter parsers
        task_info_parser = subparsers_tasks.add_parser("info", help="Show tasks with name and params")
        task_info_parser.add_argument("--name", default=None, help="Task name")
        # only defaults below
        task_info_parser.add_argument("--format", choices=["json"], default="json", help="Output format")
        task_info_parser.add_argument(
            "--fields", choices=["all"], nargs="*", default=["all"], help="List of fields or `all`"
        )

        # instance parsers
        parser_instances = subparsers.add_parser("instance")
        subparsers_instances = parser_instances.add_subparsers(dest="action")
        # instance schedule parsers
        instances_schedule_parser = subparsers_instances.add_parser("schedule", help="Schedule task")
        instances_schedule_parser.add_argument("--name", help="Task name", required=True)
        instances_schedule_parser.add_argument("--params", default=[], help="Task params", nargs="+")

        return parser.parse_args(args)

    async def run(self, args: argparse.Namespace) -> None:
        def _parse_task_params(params: Iterable) -> dict:
            return {item.split("=")[0]: item.split("=")[1] for item in params}

        controller = Controller(registry=self._registry, processor=self._processor)
        if args.subparser == "instance":
            if args.action == "schedule":
                task = controller.tasks.meta(TaskName(args.name))
                params = _parse_task_params(args.params)
                await controller.instances.schedule(task(**params))
            else:
                raise ValueError(f"Unknown action {args.action}")
        elif args.subparser == "task":
            if args.action == "info":
                tasks = controller.tasks.filter(name=TaskName(args.name))
                for task in tasks:
                    print(
                        json.dumps(
                            {
                                "name": task.name,
                                "params": task.get_schema(),
                            },
                            sort_keys=True,
                            indent=4,
                        ),
                    )
            else:
                raise ValueError(f"Unknown action {args.action}")
        else:
            raise ValueError(f"Unknown subparser {args.subparser}")
