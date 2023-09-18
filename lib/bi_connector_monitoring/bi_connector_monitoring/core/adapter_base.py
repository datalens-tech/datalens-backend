from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

import logging

import attr
from aiohttp.client import ClientResponse

from dl_core.connection_executors.adapters.async_adapters_aiohttp import AiohttpDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawExecutionResult
from dl_core.db.native_type import GenericNativeType
from dl_core.exc import DatabaseQueryError
from dl_app_tools.profiling_base import generic_profiler_async

if TYPE_CHECKING:
    from dl_core.connection_executors.models.db_adapter_data import RawSchemaInfo, DBAdapterQuery
    from dl_core.connection_models import TableIdent, TableDefinition, SchemaIdent, DBIdent
    from dl_constants.types import TBIChunksGen


LOGGER = logging.getLogger(__name__)


class AsyncBaseSolomonAdapter(AiohttpDBAdapter):
    _url: str = attr.ib(init=False)

    async def run_query(self, dba_query: DBAdapterQuery) -> ClientResponse:
        resp = await self._session.post(
            url=self._url,
            json={},
        )
        return resp

    def parse_response_body(self, response: dict[str, Any], dba_query: DBAdapterQuery) -> dict:
        return dict(rows=[], schema=[])

    @staticmethod
    def make_exc(  # TODO:  Move to ErrorTransformer
        status_code: int,  # noqa
        err_body: str,
        debug_compiled_query: Optional[str] = None,
    ) -> DatabaseQueryError:
        exc_cls = DatabaseQueryError
        return exc_cls(
            db_message=err_body,
            query=debug_compiled_query,
            orig=None,
            details={},
        )

    @generic_profiler_async("db-full")  # type: ignore  # TODO: fix
    async def execute(self, dba_query: DBAdapterQuery) -> AsyncRawExecutionResult:
        with self.wrap_execute_excs(query=dba_query, stage='request'):
            resp = await self.run_query(dba_query)

        if resp.status != 200:
            data = await resp.json()
            db_exc = self.make_exc(
                status_code=resp.status,
                err_body=data['message'],
                debug_compiled_query=dba_query.debug_compiled_query,
            )
            raise db_exc

        response = await resp.json()
        rd = self.parse_response_body(response, dba_query=dba_query)

        async def chunk_gen(
            chunk_size: int = dba_query.chunk_size or self._default_chunk_size,
        ) -> TBIChunksGen:
            data = rd['rows']
            while data:
                chunk = data[:chunk_size]
                data = data[chunk_size:]
                yield chunk

        return AsyncRawExecutionResult(
            raw_cursor_info=dict(
                schema=rd['schema'],
                names=[name for name, _ in rd['schema']],
                driver_types=[driver_type for _, driver_type in rd['schema']],
                db_types=[
                    self._type_name_to_native_type(driver_type)
                    for _, driver_type in rd['schema']
                ],
            ),
            raw_chunk_generator=chunk_gen(),
        )

    def _type_name_to_native_type(self, type_name: str) -> GenericNativeType:
        return GenericNativeType.normalize_name_and_create(
            conn_type=self.conn_type,
            name=type_name,
        )

    async def test(self) -> None:
        pass

    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        raise NotImplementedError()

    async def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        raise NotImplementedError()

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError()

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError()

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError()
