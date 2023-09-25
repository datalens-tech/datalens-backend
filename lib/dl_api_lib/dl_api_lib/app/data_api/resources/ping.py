# TODO FIX: Move to bi-common

from __future__ import annotations

import logging
from typing import (
    ClassVar,
    Optional,
)

import aiohttp
from aiohttp import (
    ClientTimeout,
    TCPConnector,
    web,
)

from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgService
from dl_configs.enums import (
    RQE_SERVICES,
    RequiredService,
)
from dl_configs.rqe import RQEBaseURL
from dl_core.connection_executors.models.constants import HEADER_REQUEST_ID
from dl_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from dl_utils.aio import timeout

from .base import (
    BaseView,
    requires,
)


LOGGER = logging.getLogger(__name__)


@requires(RequiredResourceCommon.SKIP_AUTH)
class PingView(BaseView):
    async def get(self) -> web.Response:
        return web.json_response(
            {
                "request_id": self.dl_request.rci.request_id,
            }
        )


@requires(RequiredResourceCommon.SKIP_AUTH)
class PingReadyView(BaseView):
    default_timeout: ClassVar[float] = 10.0

    async def is_pg_ready(self) -> bool:
        compeng: CompEngPgService = CompEngPgService.get_app_instance(self.request.app)
        processor = compeng.get_data_processor()

        try:
            async with timeout(self.default_timeout):
                await processor.start()
                result = await processor.ping()
                assert result == 1, result  # TODO: encapsulate this logic
        except Exception as e:
            LOGGER.info("Got an exception trying to check PG readiness: %s", e)
            return False
        finally:
            await processor.end()

        return True

    async def is_rqe_ready(self, base_url: RQEBaseURL) -> Optional[int]:
        session = aiohttp.ClientSession(
            connector=TCPConnector(force_close=True),
            timeout=ClientTimeout(
                total=self.default_timeout * 2,
                connect=5,
                sock_read=self.default_timeout * 2,
                sock_connect=5,
            ),
        )

        async with session as rqe_session:
            try:
                resp = await rqe_session.get(
                    url=f"{base_url}/ping",
                    headers={
                        HEADER_REQUEST_ID: self.dl_request.rci.request_id,
                    },
                )
            except Exception as e:
                LOGGER.info("Got an exception trying to check RQE readiness: %s", e)
                return None

        return resp.status

    async def get(self) -> web.Response:
        response_details: dict[str, Optional[int]] = dict()
        required_services = self.dl_request.services_registry.get_required_services()

        if RequiredService.POSTGRES in required_services:
            LOGGER.info("Going to check PG readiness")
            response_details[RequiredService.POSTGRES.name] = await self.is_pg_ready()

        if required_rqe_services := required_services & RQE_SERVICES:
            conn_exec_factory = self.dl_request.services_registry.get_conn_executor_factory()
            assert isinstance(conn_exec_factory, DefaultConnExecutorFactory)
            rqe_config = conn_exec_factory.rqe_config
            assert rqe_config is not None, "RQE readiness check requested, but no rqe_config provided"

            rqe_base_url_map: dict[RequiredService, RQEBaseURL] = {
                RequiredService.RQE_INT_SYNC: rqe_config.int_sync_rqe,
                RequiredService.RQE_EXT_SYNC: rqe_config.ext_sync_rqe,
                RequiredService.RQE_EXT_ASYNC: rqe_config.ext_async_rqe,
            }

            for rqe_service in required_rqe_services:
                rqe_base_url = rqe_base_url_map[rqe_service]
                LOGGER.info(f"Going to check RQE readiness ({rqe_service.name} at {rqe_base_url})")
                response_details[rqe_service.name] = await self.is_rqe_ready(rqe_base_url)

        status = (
            200 if all(ping_result == 200 or ping_result is True for ping_result in response_details.values()) else 503
        )

        return web.json_response(
            status=status,
            data={
                "request_id": self.dl_request.rci.request_id,
                "details": {
                    **response_details,
                },
            },
        )
