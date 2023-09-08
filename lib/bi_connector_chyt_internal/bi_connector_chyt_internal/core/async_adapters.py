from __future__ import annotations

import asyncio
import logging
import weakref
from typing import Optional

import attr
from aiohttp import ClientResponse, BasicAuth

from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from bi_core.connectors.clickhouse_base.ch_commons import get_chyt_user_auth_headers
from bi_connector_chyt.core.async_adapters import AsyncCHYTAdapter

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)
from bi_connector_chyt_internal.core.target_dto import (
    BaseCHYTInternalConnTargetDTO,
    CHYTInternalConnTargetDTO,
    CHYTUserAuthConnTargetDTO,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class BaseAsyncCHYTInternalAdapter(AsyncCHYTAdapter):
    _mirror_dba: Optional['BaseAsyncCHYTInternalAdapter'] = attr.ib(default=None, init=False)
    _mirrored_queries_tasks: set[asyncio.Future] = attr.ib(factory=weakref.WeakSet)

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # TODO FIX: Fix typing
        if isinstance(self._target_dto, BaseCHYTInternalConnTargetDTO) and self._target_dto.mirroring_conn_target_dto:  # type: ignore  # TODO: fix
            mirror_target_dto = self._target_dto.mirroring_conn_target_dto  # type: ignore  # TODO: fix
            assert mirror_target_dto.mirroring_conn_target_dto is None, "Cascade mirroring for CHYT detected"
            self._mirror_dba = self.create(
                target_dto=mirror_target_dto,
                req_ctx_info=self._req_ctx_info,
                default_chunk_size=self._default_chunk_size,
            )

    async def _make_mirrored_query(self, dba_q: DBAdapterQuery) -> None:
        mirror_dba = self._mirror_dba
        log_msg_prefix = "Mirrored query result"
        try:
            assert mirror_dba, "._make_mirrored_query() called with self._mirror_dba=None"
            resp = await mirror_dba._make_query(dba_q, mirroring_mode=True)
        except AssertionError as err:
            LOGGER.exception(err)
        except asyncio.TimeoutError:
            LOGGER.info(f"{log_msg_prefix}: timeout")
        except Exception as err:  # noqa
            LOGGER.info(f"{log_msg_prefix}: exception %s", type(err), exc_info=True)
        else:
            LOGGER.info(f"{log_msg_prefix}: response from '%s' status=%s", mirror_dba._target_dto.db_name, resp.status)

    async def _make_query(self, dba_q: DBAdapterQuery, mirroring_mode: bool = False) -> ClientResponse:
        if self._mirror_dba is not None:
            LOGGER.debug("Going to send query copy of query to clique '%s'", self._mirror_dba._target_dto.db_name)
            task = asyncio.ensure_future(asyncio.shield(self._make_mirrored_query(dba_q)))
            self._mirrored_queries_tasks.add(task)
        else:
            LOGGER.debug("Mirroring DBA not set. Copy of query will not be sent.")

        return await super()._make_query(dba_q, mirroring_mode=mirroring_mode)

    async def _wait_mirrored_queries_close_mirrored_dba(self) -> None:
        assert self._mirror_dba is not None
        try:
            LOGGER.info("Waiting for %s mirrored queries to become done/timeout", len(self._mirrored_queries_tasks))
            if self._mirrored_queries_tasks:
                await asyncio.gather(*list(self._mirrored_queries_tasks), return_exceptions=True)
            await self._mirror_dba.close()
        except Exception:  # noqa
            LOGGER.warning("Error during closing mirror CHYT DBA", exc_info=True)

    async def close(self) -> None:
        if self._mirror_dba:
            asyncio.ensure_future(self._wait_mirrored_queries_close_mirrored_dba())

        return await super().close()


class AsyncCHYTInternalAdapter(BaseAsyncCHYTInternalAdapter):
    conn_type = CONNECTION_TYPE_CH_OVER_YT

    _target_dto: CHYTInternalConnTargetDTO = attr.ib()


class AsyncCHYTUserAuthAdapter(BaseAsyncCHYTInternalAdapter):
    conn_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH

    _target_dto: CHYTUserAuthConnTargetDTO = attr.ib()

    def get_session_auth(self) -> Optional[BasicAuth]:
        return None

    def get_session_headers(self) -> dict[str, str]:
        auth_headers = get_chyt_user_auth_headers(
            self._target_dto.header_authorization,
            self._target_dto.header_cookie,
            self._target_dto.header_csrf_token,
        )

        return {
            **super().get_session_headers(),
            **auth_headers,
        }
