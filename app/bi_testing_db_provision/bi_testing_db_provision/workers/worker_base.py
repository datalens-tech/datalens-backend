import abc
import asyncio
import contextlib
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import AsyncGenerator, Optional, TypeVar, Generic, Type

import attr

from bi_testing_db_provision.db_connection import DBConn, DBConnFactory

LOGGER = logging.getLogger(__name__)

_WORKER_TARGET_TV = TypeVar('_WORKER_TARGET_TV')
_WORKER_TV = TypeVar('_WORKER_TV', bound='BaseWorker')


@attr.s
class BaseWorker(Generic[_WORKER_TARGET_TV], metaclass=abc.ABCMeta):
    _id: str = attr.ib()
    _connection_factory: DBConnFactory = attr.ib()
    _connect_attempt_limit: Optional[int] = attr.ib(default=None)
    _tpe: Optional[ThreadPoolExecutor] = attr.ib(default=None)

    _connection: DBConn = attr.ib(init=False, default=None)
    _transaction_lock: asyncio.Lock = attr.ib(init=False, factory=asyncio.Lock)

    @property
    def id(self) -> str:
        return self._id

    @property
    def worker_label(self) -> str:
        worker_type_name = type(self).__qualname__
        worker_id = self.id
        return f"{worker_type_name}/{worker_id}"

    @property
    def tpe(self) -> ThreadPoolExecutor:
        if self._tpe is None:
            raise ValueError(f"TPE was not provided for worker: {self}")
        return self._tpe

    @contextlib.asynccontextmanager
    async def transaction_cm(self) -> AsyncGenerator[None, None]:
        # TODO FIX: Resolve potential deadlock
        async with self._transaction_lock:
            async with self._connection.begin():
                yield

    async def _ensure_connection(self):  # type: ignore  # TODO: fix
        connect_attempts = 0
        while True:
            if self._connection is not None:
                try:
                    await self._connection.scalar("SELECT 1")
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa
                    LOGGER.exception("Connection checkup was failed, releasing connection")
                    bad_connection, self._connection = self._connection, None  # type: ignore  # TODO: fix
                    self._connection_factory.release(bad_connection)
                    continue
                else:
                    return

            if self._connect_attempt_limit is not None and connect_attempts > self._connect_attempt_limit:
                raise Exception("Connection attempt limit reached")
            connect_attempts += 1

            try:
                LOGGER.info("Going to connect to PG")
                connection = await self._connection_factory.get()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa
                LOGGER.exception("Connection was failed")
                # TODO FIX: Make exponential back-off
                await asyncio.sleep(1)
                continue
            else:
                self._connection = connection
                await self._on_reconnect()

    async def _on_reconnect(self) -> None:
        """
        Here all resources depending on connection must be recreated
        """
        pass

    async def wait_for_target(self) -> _WORKER_TARGET_TV:
        raise NotImplementedError()

    async def handle_target(self, target: _WORKER_TARGET_TV) -> _WORKER_TARGET_TV:
        raise NotImplementedError()

    async def save_target_transactional(self, target: _WORKER_TARGET_TV) -> _WORKER_TARGET_TV:
        raise NotImplementedError()

    async def main_cycle(self):  # type: ignore  # TODO: fix
        while True:
            LOGGER.info("Going to ensure worker connection to PG: %s", self.worker_label)
            await self._ensure_connection()
            LOGGER.info("Worker connection to PG is ok: %s", self.worker_label)

            try:
                LOGGER.info("Waiting for target resource: %s", self.worker_label)
                resource = await self.wait_for_target()

                LOGGER.info("Going to handle resource: %s", self.worker_label)
                resource = await self.handle_target(resource)

                LOGGER.info("Resource handling done. Going to save with new status: %s", self.worker_label)
                async with self.transaction_cm():
                    await self.save_target_transactional(resource)

                LOGGER.info("Resource saving done: %s", self.worker_label)
            except asyncio.CancelledError:
                LOGGER.info("Worker was cancelled: %s", self.worker_label)
                raise
            except Exception:  # noqa
                cool_down_interval = 2
                await asyncio.sleep(cool_down_interval)
                LOGGER.exception("Got exception during handling item")

    @classmethod
    def create(
            cls: Type[_WORKER_TV],
            the_id: str,
            conn_factory: DBConnFactory,
            tpe: Optional[ThreadPoolExecutor] = None,
    ) -> _WORKER_TV:
        return cls(id=the_id, connection_factory=conn_factory, tpe=tpe)
