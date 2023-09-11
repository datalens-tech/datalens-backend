from __future__ import annotations

import abc
import asyncio
import random
import logging

import aiohttp
import attr

from bi_core import exc
from bi_core.utils import raise_for_status_and_hide_secret_headers

from bi_connector_chyt.core.connection_executors import BaseCHYTAdapterConnExecutor
from bi_connector_chyt_internal.core.async_adapters import AsyncCHYTInternalAdapter, AsyncCHYTUserAuthAdapter
from bi_connector_chyt_internal.core.conn_options import CHYTInternalConnectOptions
from bi_connector_chyt_internal.core.dto import CHYTInternalBaseDTO, CHYTInternalDTO, CHYTUserAuthDTO
from bi_connector_chyt_internal.core.target_dto import (
    BaseCHYTInternalConnTargetDTO,
    CHYTInternalConnTargetDTO,
    CHYTUserAuthConnTargetDTO,
)
from bi_connector_chyt_internal.core.adapters import CHYTInternalAdapter, CHYTUserAuthAdapter
from bi_connector_chyt_internal.core.utils import get_chyt_user_auth_headers


LOGGER = logging.getLogger(__name__)


@attr.s(cmp=False, hash=False)
class BaseCHYTInternalSyncAdapterConnExecutor(BaseCHYTAdapterConnExecutor):
    _conn_dto: CHYTInternalBaseDTO = attr.ib()
    _conn_options: CHYTInternalConnectOptions = attr.ib()

    @abc.abstractmethod
    async def _get_target_conn_dto(self) -> BaseCHYTInternalConnTargetDTO:
        raise NotImplementedError

    async def _make_target_conn_dto_pool(self) -> list[BaseCHYTInternalConnTargetDTO]:  # type: ignore  # TODO: fix
        base_conn_target_dto = await self._get_target_conn_dto()

        target_dto = base_conn_target_dto

        do_mirroring = False
        mirroring_frac = self._conn_options.mirroring_frac
        mirroring_clique_alias = self._conn_options.mirroring_clique_alias
        if mirroring_frac and mirroring_clique_alias:
            do_mirroring = True if mirroring_frac >= 1.0 else random.random() < mirroring_frac
        if do_mirroring:
            LOGGER.info("Queries will be mirrored to clique %r", mirroring_clique_alias)
            target_dto = target_dto.clone(
                mirroring_conn_target_dto=base_conn_target_dto.clone(
                    db_name=mirroring_clique_alias,
                    total_timeout=self._conn_options.mirroring_clique_req_timeout_sec,
                ),
            )

        return [target_dto]

    async def _get_real_hosts(self, session: aiohttp.ClientSession) -> str:
        LOGGER.info("Requesting host for CHYT")

        cluster = self._conn_dto.cluster
        url = f'http://{cluster}.yt.yandex.net/hosts'

        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                hosts = await resp.json()
                if not hosts:
                    raise ValueError((f'Received no heavy-proxy hosts for YT {cluster}. '
                                      'Check cluster availability and current infra events.'), )
        except Exception as err:
            raise exc.DatabaseUnavailable(message=repr(err), orig=err, details={}, ) from err

        host = random.choice(hosts)
        LOGGER.info("Got host for CHYT: %s", host)
        return host


@attr.s(cmp=False, hash=False)
class CHYTInternalSyncAdapterConnExecutor(BaseCHYTInternalSyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = CHYTInternalAdapter

    _conn_dto: CHYTInternalDTO = attr.ib()

    async def _get_target_conn_dto(self) -> CHYTInternalConnTargetDTO:
        # TODO: at least a per-incoming-request session (or even longer) would be preferable.
        async with aiohttp.ClientSession() as session:
            real_host = await self._get_real_hosts(session)

        return CHYTInternalConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            host=real_host,
            port=80,
            yt_cluster=self._conn_dto.cluster,
            username='default',
            password=self._conn_dto.token,
            db_name=self._conn_dto.clique_alias,
            cluster_name=None,
            endpoint='query',
            protocol='http',
            max_execution_time=self._conn_options.max_execution_time,
            total_timeout=self._conn_options.total_timeout,
            connect_timeout=self._conn_options.connect_timeout,
            mirroring_conn_target_dto=None,
            insert_quorum=self._conn_options.insert_quorum,
            insert_quorum_timeout=self._conn_options.insert_quorum_timeout,
            disable_value_processing=self._conn_options.disable_value_processing,
        )


@attr.s(cmp=False, hash=False)
class CHYTUserAuthSyncAdapterConnExecutor(BaseCHYTInternalSyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = CHYTUserAuthAdapter

    _conn_dto: CHYTUserAuthDTO = attr.ib()

    async def _get_csrf_token(self, session: aiohttp.ClientSession) -> str:
        LOGGER.info("Requesting csrf token from YT")

        cluster = self._conn_dto.cluster
        url = f'http://{cluster}.yt.yandex.net/auth/whoami'

        auth_headers = get_chyt_user_auth_headers(
            self._conn_dto.header_authorization, self._conn_dto.header_cookie, None)

        try:
            async with session.get(url, headers=auth_headers) as resp:
                raise_for_status_and_hide_secret_headers(resp)
                res_data = await resp.json()
                csrf_token = res_data['csrf_token']
        except Exception as err:
            raise exc.DatabaseUnavailable(
                message=repr(err),
                orig=err,
                details={},
            ) from err

        LOGGER.info("Got csrf token for CHYT")

        return csrf_token

    async def _get_target_conn_dto(self) -> CHYTUserAuthConnTargetDTO:
        # TODO: at least a per-incoming-request session (or even longer) would be preferable.
        async with aiohttp.ClientSession() as session:
            real_host, csrf_token = await asyncio.gather(self._get_real_hosts(session), self._get_csrf_token(session))

        return CHYTUserAuthConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            host=real_host,
            port=80,
            yt_cluster=self._conn_dto.cluster,
            username=None,  # type: ignore  # TODO: fix
            password=None,  # type: ignore  # TODO: fix
            header_authorization=self._conn_dto.header_authorization,
            header_cookie=self._conn_dto.header_cookie,
            header_csrf_token=csrf_token,
            db_name=self._conn_dto.clique_alias,
            cluster_name=None,
            endpoint='query',
            protocol='http',
            max_execution_time=self._conn_options.max_execution_time,
            total_timeout=self._conn_options.total_timeout,
            connect_timeout=self._conn_options.connect_timeout,
            mirroring_conn_target_dto=None,
            insert_quorum=self._conn_options.insert_quorum,
            insert_quorum_timeout=self._conn_options.insert_quorum_timeout,
            disable_value_processing=self._conn_options.disable_value_processing,
        )


@attr.s(cmp=False, hash=False)
class CHYTInternalAsyncAdapterConnExecutor(CHYTInternalSyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncCHYTInternalAdapter  # type: ignore  # TODO: fix


@attr.s(cmp=False, hash=False)
class CHYTUserAuthAsyncAdapterConnExecutor(CHYTUserAuthSyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncCHYTUserAuthAdapter  # type: ignore  # TODO: fix
