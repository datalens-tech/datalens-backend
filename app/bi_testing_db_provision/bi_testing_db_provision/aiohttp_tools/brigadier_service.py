import asyncio
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional, ClassVar, Dict, Type

import attr
from aiohttp import web

from bi_testing_db_provision.common_config_models import PGConfig
from bi_testing_db_provision.db_connection import DBConnFactory
from bi_testing_db_provision.workers.brigadier import Brigadier
from bi_testing_db_provision.workers.worker_base import BaseWorker

LOGGER = logging.getLogger(__name__)


@attr.s()
class _BrigadierServiceData:
    brigadier: Optional[Brigadier] = attr.ib(default=None)
    conn_factory: Optional[DBConnFactory] = attr.ib(default=None)
    tpe: Optional[ThreadPoolExecutor] = attr.ib(default=None)
    startup_lock: asyncio.Lock = attr.ib(factory=asyncio.Lock)


@attr.s
class BrigadierService:
    _APP_KEY_SERVICE_DATA: ClassVar[str] = '_brigadier_service_data'
    _APP_KEY_BRIGADIER: ClassVar[str] = 'brigadier'
    _APP_KEY_BRIGADIER_PG_ENG: ClassVar[str] = 'brigadier_pg_eng'
    _APP_KEY_BRIGADIER_CONN_FACTORY: ClassVar[str] = 'brigadier_factory'
    _APP_KEY_BRIGADIER_TPE: ClassVar[str] = 'brigadier_tpe'

    pg_config: PGConfig = attr.ib()
    workers_config: Dict[Type[BaseWorker], int] = attr.ib()
    worker_id_prefix: str = attr.ib()

    async def on_startup(self, app: web.Application) -> None:
        LOGGER.info("Executing brigadier service startup")
        service_data = _BrigadierServiceData()
        app[self._APP_KEY_SERVICE_DATA] = service_data

        async with service_data.startup_lock:
            service_data.conn_factory = await DBConnFactory.from_pg_config(self.pg_config)

            # TODO FIX: Do not create TPE here
            service_data.tpe = ThreadPoolExecutor()

            service_data.brigadier = Brigadier(
                conn_factory=service_data.conn_factory,
                initial_worker_config=self.workers_config,
                worker_id_prefix=self.worker_id_prefix,
                tpe=service_data.tpe,
            )

            await service_data.brigadier.launch_workers()

    async def on_shutdown(self, app: web.Application) -> None:
        LOGGER.info("Executing brigadier service shutdown")
        service_data: _BrigadierServiceData = app[self._APP_KEY_SERVICE_DATA]

        async with service_data.startup_lock:
            brigadier: Optional[Brigadier] = service_data.brigadier
            conn_factory: Optional[DBConnFactory] = service_data.conn_factory
            tpe: Optional[ThreadPoolExecutor] = service_data.tpe

            if brigadier is not None:
                LOGGER.info("Waiting for workers to stop")
                await brigadier.stop_workers()
            if conn_factory is not None:
                LOGGER.info("Closing connection factory")
                await conn_factory.close()
            if tpe is not None:
                LOGGER.info("Terminating TPE")
                tpe.shutdown()
            LOGGER.info("Brigadier service was fully terminated")

    @classmethod
    def get_for_app(cls, app: web.Application) -> Optional[Brigadier]:
        service_data = app.get(cls._APP_KEY_SERVICE_DATA)
        if isinstance(service_data, _BrigadierServiceData):
            return service_data.brigadier
        else:
            return None
