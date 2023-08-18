from __future__ import annotations

import asyncio
import json
import logging
from asyncio import Future
from typing import Dict, Any, Optional, Set, AsyncContextManager

import attr
import sqlalchemy as sa
from aiopg.sa import SAConnection, Engine, create_engine

from bi_configs.utils import ROOT_CERTIFICATES_FILENAME
from bi_testing_db_provision.common_config_models import PGConfig

LOGGER = logging.getLogger(__name__)

_EventPayload = Dict[str, Any]


@attr.s(hash=False)
class DBConn:
    _sa_connection: SAConnection = attr.ib()
    _listen_task: Optional[asyncio.Task] = attr.ib(init=False, default=None)
    _event_waiter_future_map: Dict[str, Set[Future[_EventPayload]]] = attr.ib(init=False, factory=dict)
    _active_subscriptions: Set[str] = attr.ib(init=False, factory=set)

    @staticmethod
    def quote_identifier(indent: str) -> str:
        # TODO FIX: Really quote
        return indent

    # TODO FIX: More accurate typing
    async def execute(self, query: Any, *multiparams: Any, **params: Any) -> Any:
        return await self._sa_connection.execute(query, *multiparams, **params)

    async def scalar(self, query: Any, *multiparams: Any, **params: Any) -> Any:
        return await self._sa_connection.scalar(query, *multiparams, **params)

    def begin(self) -> AsyncContextManager[None]:
        return self._sa_connection.begin()

    @property
    def in_transaction(self) -> bool:
        return self._sa_connection.in_transaction

    async def subscribe(self, channel: str) -> None:
        if channel in self._active_subscriptions:
            return

        await self._sa_connection.execute(f"LISTEN {self.quote_identifier(channel)}")
        self._active_subscriptions.add(channel)

        if self._listen_task is None:
            self._listen_task = asyncio.create_task(self._handle_event_queue())

    async def publish(self, channel: str, payload: _EventPayload) -> None:
        notification_body = json.dumps(payload, ensure_ascii=True)
        await self._sa_connection.execute(
            sa.text(f"NOTIFY {self.quote_identifier(channel)}, :body"""),
            dict(
                channel=channel,
                body=notification_body,
            )
        )

    async def wait_for_single_event(self, channel: str, timeout: Optional[float] = None) -> _EventPayload:
        fut_set = self._event_waiter_future_map.setdefault(channel, set())
        fut = asyncio.Future()  # type: ignore  # TODO: fix
        fut_set.add(fut)

        try:
            if timeout is None:
                return await fut
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            fut_set.remove(fut)

    async def _handle_event_queue(self):  # type: ignore  # TODO: fix
        low_level_conn = self._sa_connection.connection

        while True:
            msg = await low_level_conn.notifies.get()
            try:
                payload = json.loads(msg.payload)
            except Exception:  # noqa
                LOGGER.exception("Exception during parsing event payload")
            else:
                fut_set = self._event_waiter_future_map.setdefault(msg.channel, set())
                for fut in fut_set:
                    if not fut.done():
                        fut.set_result(payload)

    async def close(self):  # type: ignore  # TODO: fix
        listen_task = self._listen_task
        if listen_task is not None and not listen_task.cancelled():
            listen_task.cancel()
        await self._sa_connection.close()


@attr.s
class DBConnFactory:
    _engine: Engine = attr.ib()
    _close_engine: bool = attr.ib(default=False)
    _connections: Set[DBConn] = attr.ib(init=False, factory=set)
    _scheduled_close_tasks: Set[asyncio.Task] = attr.ib(init=False, factory=set)

    async def get(self) -> DBConn:
        sa_conn = await self._engine.acquire()
        conn = DBConn(sa_conn)
        self._connections.add(conn)
        return conn

    def release(self, conn: DBConn) -> None:
        async def close_conn():  # type: ignore  # TODO: fix
            return await conn.close()

        self._scheduled_close_tasks.add(asyncio.create_task(close_conn))  # type: ignore  # TODO: fix
        self._connections.remove(conn)

    async def close(self) -> None:
        for conn in self._connections:
            try:
                await conn.close()
            except Exception:  # noqa
                pass
        for task in self._scheduled_close_tasks:
            try:
                await task
            except Exception:  # noqa
                pass

        if self._close_engine:
            self._engine.close()
            await self._engine.wait_closed()

    @classmethod
    async def from_pg_config(cls, pg_config: PGConfig) -> DBConnFactory:
        engine: Engine = await create_engine(
            host=','.join(pg_config.host_list),
            port=pg_config.port,
            database=pg_config.database,
            user=pg_config.user,
            password=pg_config.password,
            connect_timeout=pg_config.connect_timeout,
            sslmode=pg_config.ssl_mode,
            sslrootcert=ROOT_CERTIFICATES_FILENAME,
        )
        return DBConnFactory(engine=engine, close_engine=True)
