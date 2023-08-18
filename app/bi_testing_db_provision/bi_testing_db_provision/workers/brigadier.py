import asyncio
import contextlib
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Type, Dict, cast, AsyncGenerator

import attr
import shortuuid

from bi_testing_db_provision.db_connection import DBConnFactory
from bi_testing_db_provision.workers.worker_base import BaseWorker

LOGGER = logging.getLogger(__name__)


@attr.s
class WorkerDescriptor:
    worker: BaseWorker = attr.ib()
    main_cycle_task: asyncio.Task = attr.ib(default=None)


@attr.s
class Brigadier:
    _conn_factory: DBConnFactory = attr.ib()
    _worker_id_prefix: str = attr.ib()
    _initial_worker_config: Dict[Type[BaseWorker], int] = attr.ib()
    _tpe: ThreadPoolExecutor = attr.ib(factory=ThreadPoolExecutor)
    # internals
    _worker_map: Dict[str, WorkerDescriptor] = attr.ib(init=False, factory=dict)
    _worker_operation_lock: asyncio.Lock = attr.ib(factory=asyncio.Lock)

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        self._create_workers()

    @contextlib.asynccontextmanager
    async def worker_operation_lock(self, operation_name: str) -> AsyncGenerator[None, None]:
        LOGGER.info("Waiting for worker operation lock to perform %s", operation_name)
        async with self._worker_operation_lock:
            LOGGER.info("Got worker action lock to perform %s", operation_name)
            yield

    def _create_workers(self) -> None:
        for worker_type, worker_count in self._initial_worker_config.items():
            worker_type = cast(Type[BaseWorker], worker_type)

            for i in range(worker_count):
                worker_id = self.generate_worker_id()
                self._worker_map[worker_id] = WorkerDescriptor(
                    worker=worker_type.create(
                        the_id=worker_id,
                        conn_factory=self._conn_factory,
                        tpe=self._tpe,
                    )
                )

    def generate_worker_id(self) -> str:
        return f"{self._worker_id_prefix}_{shortuuid.uuid()}"

    async def _worker_control_task(self, worker_descriptor: WorkerDescriptor):  # type: ignore  # TODO: fix
        LOGGER.info("Control task was launched for worker %s", worker_descriptor.worker.worker_label)
        while True:
            try:
                LOGGER.info("Control task starting main cycle for worker %s", worker_descriptor.worker.worker_label)
                await worker_descriptor.worker.main_cycle()
            except asyncio.CancelledError:
                LOGGER.info(
                    "Control task detects cancellation of main cycle for %s",
                    worker_descriptor.worker.worker_label
                )
                raise
            except Exception:  # noqa
                LOGGER.exception("Worker %s was failed, going to relaunch", worker_descriptor.worker.worker_label)
                # TODO FIX: Do not relaunch worker. Create new one
                await asyncio.sleep(1.)

    async def launch_workers(self) -> None:
        LOGGER.info("Workers launch was requested")
        async with self.worker_operation_lock("launch workers"):
            for worker_descriptor in self._worker_map.values():
                control_task = asyncio.create_task(self._worker_control_task(worker_descriptor))
                worker_descriptor.main_cycle_task = control_task

    async def stop_workers(self) -> None:
        LOGGER.info("Workers stop was requested")
        async with self.worker_operation_lock("stop workers"):
            all_worker_descriptors = self._worker_map.values()

            async def worker_finalizer(worker_descriptor: WorkerDescriptor) -> None:
                task = worker_descriptor.main_cycle_task

                if task is None:
                    return

                worker_label = worker_descriptor.worker.worker_label

                LOGGER.info("Canceling worker %s task %s", worker_label, task)
                task.cancel()

                try:
                    # TODO FIX: It looks like there is a bug in async_timeout
                    #  looks like if task will be cancelled while it is in async_timeout
                    #  cancellation will be ignored if task will be awaited immediately after cancellation
                    while True:
                        try:
                            LOGGER.info("Waiting for worker %s task %s", worker_label, task)
                            await asyncio.wait_for(task, timeout=0.1)
                        except asyncio.TimeoutError:
                            LOGGER.info("Worker %s cancellation timeout, going to try again...", worker_label)
                        else:
                            break
                except asyncio.CancelledError:
                    LOGGER.info("Worker %s was cancelled OK", worker_label)
                except Exception:  # noqa
                    LOGGER.info("Worker %s was cancelled, but task finished with exception", worker_label)

            await asyncio.gather(*[worker_finalizer(wd) for wd in all_worker_descriptors])
            LOGGER.info("All worker tasks cancellation done")
