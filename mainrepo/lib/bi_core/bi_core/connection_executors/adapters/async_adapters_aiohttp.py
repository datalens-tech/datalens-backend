from __future__ import annotations

import abc
import asyncio
import contextlib
import ssl
from typing import TYPE_CHECKING, Dict, Generator, Optional, Sequence, Tuple, Type, TypeVar

import aiohttp.client_exceptions
import attr
from aiohttp.client import ClientSession, ClientTimeout

from bi_configs.utils import get_root_certificates_path

from bi_core.connection_executors.adapters.async_adapters_base import AsyncDirectDBAdapter
from bi_core import exc

if TYPE_CHECKING:
    from aiohttp import BasicAuth

    from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
    from bi_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
    from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery


_DBA_TV = TypeVar('_DBA_TV', bound='AiohttpDBAdapter')


@attr.s
class AiohttpDBAdapter(AsyncDirectDBAdapter, metaclass=abc.ABCMeta):
    """ Common base for adapters that primarily use an aiohttp client """

    # TODO?: commonize some of the CTDTO attributes such as connect_timeout?
    _target_dto: ConnTargetDTO = attr.ib()
    _req_ctx_info: DBAdapterScopedRCI = attr.ib()

    _http_read_chunk_size: int = attr.ib(init=False, default=(1024 * 64))
    _default_chunk_size: int = attr.ib()

    _session: ClientSession = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        self._session = ClientSession(
            timeout=self.get_session_timeout(),
            auth=self.get_session_auth(),
            headers=self.get_session_headers(),
            connector=self.create_aiohttp_connector(
                ssl_context=ssl.create_default_context(cafile=get_root_certificates_path())
            )
        )

    def create_aiohttp_connector(self, ssl_context: Optional[ssl.SSLContext]) -> aiohttp.TCPConnector:
        return aiohttp.TCPConnector(
            ssl_context=ssl_context,
        )

    @classmethod
    def create(
            cls: Type[_DBA_TV],
            target_dto: ConnTargetDTO,
            req_ctx_info: DBAdapterScopedRCI,
            default_chunk_size: int,
    ) -> _DBA_TV:
        return cls(
            target_dto=target_dto,
            req_ctx_info=req_ctx_info,
            default_chunk_size=default_chunk_size,
        )

    def get_session_timeout(self) -> ClientTimeout:
        # Sime default large-ish timeout.
        return ClientTimeout(total=3600.0)

    def get_session_auth(self) -> Optional[BasicAuth]:
        return None

    def get_session_headers(self) -> Dict[str, str]:
        return {
            # TODO: bi_constants / bi_configs.constants
            'user-agent': 'DataLens',
        }

    async def close(self) -> None:
        await self._session.close()

    execute_err_map: Sequence[Tuple[Type[Exception], Type[exc.DatabaseQueryError]]] = (
        (aiohttp.client_exceptions.ClientConnectorError, exc.SourceConnectError),
        (exc.AIOHttpConnTimeoutError, exc.SourceConnectError),
        (aiohttp.client_exceptions.ServerTimeoutError, exc.SourceTimeout),
        (asyncio.exceptions.TimeoutError, exc.SourceTimeout),
        (aiohttp.client_exceptions.ServerDisconnectedError, exc.SourceClosedPrematurely),
        (exc.RSTError, exc.SourceClosedPrematurely),
        (aiohttp.client_exceptions.ClientPayloadError, exc.SourceProtocolError),
    )

    @contextlib.contextmanager
    def wrap_execute_excs(self, query: DBAdapterQuery, stage: Optional[str]) -> Generator[None, None, None]:
        try:
            try:
                yield None
            except aiohttp.client_exceptions.ClientOSError as pre_err:
                # need to check errno, so hard to plug into the main exc handler nicely.
                if pre_err.errno == 104:  # 104 == errno.ECONNRESET
                    raise exc.RSTError() from pre_err
                raise
            except aiohttp.client_exceptions.ServerTimeoutError as aiohttp_timeout_err:
                if aiohttp_timeout_err.args[0].startswith('Connection timeout'):
                    # https://github.com/aio-libs/aiohttp/blob/3.8/aiohttp/client.py#L541
                    raise exc.AIOHttpConnTimeoutError() from aiohttp_timeout_err
                raise
        except Exception as err:
            for orig_err_type, mapped_err_type in self.execute_err_map:
                if isinstance(err, orig_err_type):
                    # TODO CONSIDER: Add messages for users' DBs (not internal materialization CH)
                    debug_compiled_query = None
                    if self._target_dto.pass_db_query_to_user:
                        debug_compiled_query = query.debug_compiled_query

                    new_err = mapped_err_type(
                        db_message=None,
                        query=debug_compiled_query,
                        orig=err,
                        details={},
                    )
                    new_err.debug_info.update(stage=stage)
                    raise new_err from err
            raise
