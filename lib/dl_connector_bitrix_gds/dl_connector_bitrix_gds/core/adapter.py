from __future__ import annotations

import datetime
import json
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
)
import uuid

import attr
from redis_cache_lock.types import TClientACM
from redis_cache_lock.utils import wrap_generate_func
import sqlalchemy as sa
from sqlalchemy.sql.elements import (
    ClauseElement,
    TypeCoerce,
)

from dl_app_tools.profiling_base import generic_profiler_async
from dl_cache_engine.engine import RedisCacheLockWrapped
from dl_constants.enums import ConnectionType
from dl_core.aio.web_app_services.redis import RedisConnParams
from dl_core.connection_executors.adapters.adapter_actions.async_base import AsyncDBVersionAdapterAction
from dl_core.connection_executors.adapters.adapter_actions.db_version import AsyncDBVersionAdapterActionNone
from dl_core.connection_executors.adapters.async_adapters_aiohttp import AiohttpDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawExecutionResult
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawColumnInfo,
    RawSchemaInfo,
)
from dl_core.connection_models import (
    SchemaIdent,
    TableDefinition,
    TableIdent,
)
from dl_core.connectors.base.error_handling import ETBasedExceptionMaker
from dl_core.exc import DatabaseQueryError
from dl_type_transformer.native_type import CommonNativeType

from dl_connector_bitrix_gds.core.caches import (
    bitrix_cache_deserializer,
    bitrix_cache_serializer,
    build_local_key_rep,
    get_redis_cli_acm_from_params,
)
from dl_connector_bitrix_gds.core.constants import (
    CONNECTION_TYPE_BITRIX24,
    DEFAULT_DB,
)
from dl_connector_bitrix_gds.core.error_transformer import bitrix_error_transformer
from dl_connector_bitrix_gds.core.tables import (
    BITRIX_TABLES_MAP,
    CRM_DYNAMIC_ITEMS_TABLE,
    SMART_PROCESS_TABLE_PREFIX,
)


if TYPE_CHECKING:
    from sqlalchemy.sql.elements import Label

    from dl_connector_bitrix_gds.core.tables import BitrixGDSTable
    from dl_connector_bitrix_gds.core.target_dto import BitrixGDSConnTargetDTO

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True, kw_only=True)
class BitrixRequestPayload:
    portal: str = attr.ib()
    table: str = attr.ib()
    json_body: dict = attr.ib()
    flatten_body: dict = attr.ib()


def extract_select_column_name(column: Label) -> str:
    element = column.element
    if isinstance(element, TypeCoerce):
        element = element.typed_expression
    return element.name


class BitrixGDSDefaultAdapter(AiohttpDBAdapter, ETBasedExceptionMaker):
    conn_type: ClassVar[ConnectionType] = CONNECTION_TYPE_BITRIX24
    _target_dto: BitrixGDSConnTargetDTO = attr.ib()
    _redis_cli_acm: Optional[TClientACM] = attr.ib(init=False)

    table: Optional[BitrixGDSTable] = None

    _error_transformer = bitrix_error_transformer

    EXTRA_EXC_CLS = (json.JSONDecodeError,)

    def _make_async_db_version_action(self) -> AsyncDBVersionAdapterAction:
        return AsyncDBVersionAdapterActionNone()

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        redis_conn_params: Optional[RedisConnParams]
        if self._target_dto.redis_conn_params is not None:
            redis_conn_params = RedisConnParams(**self._target_dto.redis_conn_params)
        else:
            redis_conn_params = None
        self._redis_cli_acm = get_redis_cli_acm_from_params(redis_conn_params)

    @generic_profiler_async("db-query-cached")  # type: ignore  # TODO: fix
    async def _run_query_cached(self, dba_query: DBAdapterQuery) -> Any:
        async def wrap_run_query() -> Any:
            result = await self._run_query(dba_query)
            return result

        payload = self._build_request_payload(dba_query)
        local_key_rep = build_local_key_rep(payload.portal, payload.table, payload.flatten_body)

        assert self._redis_cli_acm is not None
        rcl = RedisCacheLockWrapped(
            key=local_key_rep.key_parts_hash,
            client_acm=self._redis_cli_acm,
            resource_tag="bic_conn_bitrix_query_cache",
            lock_ttl_sec=60,
            data_ttl_sec=self._target_dto.redis_caches_ttl or 600,
        )

        result_b, result = await rcl.generate_with_lock(
            generate_func=wrap_generate_func(
                func=wrap_run_query,
                serialize=bitrix_cache_serializer,
            ),
        )
        if result is None:
            LOGGER.info("Result found in cache: %s", local_key_rep.key_parts_hash)
            result = bitrix_cache_deserializer(result_b)

        return result

    @generic_profiler_async("db-query")  # type: ignore  # TODO: fix
    async def _run_query(self, dba_query: DBAdapterQuery) -> Any:
        query_text = self.compile_query_for_execution(dba_query.query)
        payload = self._build_request_payload(dba_query)

        api_url = f"https://{self._target_dto.portal}/bitrix/tools/biconnector/pbi.php"
        request_id = self._req_ctx_info.request_id or str(uuid.uuid4())
        LOGGER.info(
            "Sending query to Bitrix:\nrequest_id: %s\nurl: %s\nquery: %s\nparams: %s",
            request_id,
            api_url,
            query_text,
            json.dumps({k: (v if k != "key" else "...") for k, v in payload.json_body.items()}),
        )

        request_params: dict[str, Any] = dict(
            table=payload.table,
            consumer="datalens",
            request_id=request_id,
        )

        limit = self._extract_limit(dba_query.query)
        if limit is not None:
            request_params["limit"] = limit

        with self.handle_execution_error(query_text):
            resp = await self._session.post(
                url=api_url,
                params=request_params,
                json=payload.json_body,
            )

            if resp.status != 200:
                body = await resp.text()
                raise DatabaseQueryError(db_message=body, query=query_text)

            resp_body = await resp.json()

        return resp_body

    def _parse_response_body_data(self, body: list, selected_columns: Optional[list] = None) -> dict:
        if not len(body):
            raise ValueError("empty response")
        cols = body[0]
        rows = body[1:]

        assert self.table is not None
        columns_type = self.table.get_columns_type()
        if selected_columns is None:
            selected_columns = cols

        try:
            normalized_data = dict(
                cols=[dict(id=col, label=col, type=columns_type.get(col, "string")) for col in selected_columns],
                rows=[[dict(zip(cols, row, strict=True))[col] for col in selected_columns] for row in rows],
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError("unexpected data structure") from e

        return normalized_data

    def _table_schema(self, table: str) -> BitrixGDSTable:
        if table.startswith(SMART_PROCESS_TABLE_PREFIX):
            return CRM_DYNAMIC_ITEMS_TABLE
        else:
            return BITRIX_TABLES_MAP[table]

    def _build_request_payload(self, dba_query: DBAdapterQuery) -> BitrixRequestPayload:
        table = self._extract_table_name(dba_query.query)
        if self.table is None:
            self.table = self._table_schema(table)
        json_body, flatten_body = self.generate_body(dba_query)
        payload = BitrixRequestPayload(
            table=table,
            portal=self._target_dto.portal,
            json_body=json_body,
            flatten_body=flatten_body,
        )
        return payload

    def _extract_table_name(self, query: ClauseElement | str) -> str:
        assert isinstance(query, sa.sql.Select)
        froms = query.froms[0]
        if isinstance(froms, sa.sql.Subquery) and hasattr(froms, "element"):
            froms = froms.element.froms[0]
        assert isinstance(froms, sa.sql.TableClause)
        return froms.name

    def _extract_limit(self, query: ClauseElement | str) -> int | None:
        assert isinstance(query, sa.sql.Select)
        return query._limit  # type: ignore  # TODO: "Select" has no attribute "_limit"  [attr-defined]

    def _parse_response_body(self, body: Any, dba_query: DBAdapterQuery) -> dict:
        assert isinstance(dba_query.query, sa.sql.Select)
        selected_columns_values = dba_query.query.selected_columns.values()
        selected_columns: Optional[list[str]] = None
        if "*" not in set(column.name for column in selected_columns_values):
            selected_columns = [extract_select_column_name(column) for column in selected_columns_values]
            # 'table."COLUMN_NAME"' -> 'COLUMN_NAME'
            selected_columns = [col.split(".")[-1].replace('"', "").replace("`", "") for col in selected_columns]

        try:
            if not isinstance(body, list):
                raise TypeError("Unexpected response format")
            return self._parse_response_body_data(body, selected_columns=selected_columns)
        except (ValueError, TypeError) as err:
            LOGGER.debug("Unexpected API response")
            raise DatabaseQueryError(
                message=f"Unexpected API response body: {err.args[0]}",
                db_message="",
                query=dba_query.debug_compiled_query,
                orig=None,
                details={},
            ) from err

    @generic_profiler_async("db-full")  # type: ignore  # TODO: fix
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        with self.wrap_execute_excs(query=query, stage="request"):
            if self._redis_cli_acm is not None:
                resp_body = await self._run_query_cached(query)
            else:
                resp_body = await self._run_query(query)

        rd = self._parse_response_body(resp_body, query)

        async def chunk_gen(chunk_size=query.chunk_size or self._default_chunk_size):  # type: ignore  # TODO: fix
            data = rd["rows"]
            while data:
                chunk = data[:chunk_size]
                data = data[chunk_size:]
                yield chunk

        return AsyncRawExecutionResult(
            raw_cursor_info=dict(cols=rd["cols"]),
            raw_chunk_generator=chunk_gen(),
        )

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        known_general_tables = BITRIX_TABLES_MAP.keys()
        user_tables = await self._get_user_tables(schema_ident)
        tables = [
            table_name
            for table_name in user_tables
            if table_name in known_general_tables or table_name.startswith(SMART_PROCESS_TABLE_PREFIX)
        ]
        return [
            TableIdent(
                db_name=schema_ident.db_name,
                schema_name=schema_ident.schema_name,
                table_name=table_name,
            )
            for table_name in tables
        ]

    async def _get_user_tables(self, schema_ident: SchemaIdent) -> list[str]:
        body: dict[str, Any] = {
            "key": self._target_dto.token,
        }
        api_url: str = f"https://{self._target_dto.portal}/bitrix/tools/biconnector/gds.php?show_tables"

        with self.handle_execution_error(api_url):
            resp = await self._session.post(
                url=api_url,
                json=body,
            )

            if resp.status != 200:
                message = await resp.text()
                raise DatabaseQueryError(db_message=message)

        tables: list[str] = [table[0] for table in await resp.json()]
        return tables

    @generic_profiler_async("db-table-info")  # type: ignore  # TODO: fix
    async def get_table_info(
        self, table_def: Optional[TableDefinition] = None, fetch_idx_info: bool = False
    ) -> RawSchemaInfo:
        assert isinstance(table_def, TableIdent)
        table_name = table_def.table_name

        assert table_name in [table.table_name for table in await self.get_tables(SchemaIdent(DEFAULT_DB, None))]
        bitrix_table = self._table_schema(table_name)
        columns_type = bitrix_table.get_columns_type()
        query = sa.select(["*"]).select_from(sa.table(table_name))
        if bitrix_table.daterange_col_name is not None:
            query = query.where(
                sa.column(bitrix_table.daterange_col_name) == "2000-01-01",
            )
        query_obj = DBAdapterQuery(
            query=query,
        )
        res = await self.execute(query_obj)
        res_cols = res.raw_cursor_info["cols"]

        return RawSchemaInfo(
            columns=tuple(
                RawColumnInfo(
                    name=col["id"],
                    title=col["id"],
                    nullable=True,
                    native_type=CommonNativeType(
                        name=columns_type.get(col["id"], "string"),
                        nullable=True,
                    ),
                )
                for col in res_cols
            )
        )

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        return table_ident in await self.get_tables(SchemaIdent(DEFAULT_DB, None))
        # db_name and schema_name from SchemaIdent is used in method get_tables.
        # Later method get_tables is used in get_parameter_combination, which sets
        # db_name as 'default' and doesn't actually use schema_name

    async def test(self) -> None:
        tables = await self.get_tables(SchemaIdent(DEFAULT_DB, None))
        table_name = tables[0].table_name
        table = self._table_schema(table_name)
        query_obj = DBAdapterQuery(
            query=sa.select(["*"])
            .select_from(sa.table(table_name))
            .where(
                sa.column(table.daterange_col_name) == "2000-01-01",
            ),
        )
        await self.execute(query_obj)

    def generate_body(self, dba_query: DBAdapterQuery) -> tuple[dict[str, Any], dict[str, str]]:
        assert self.table is not None
        body: dict[str, Any] = {
            "key": self._target_dto.token,
            "dateRange": {},
        }

        def date_converter(value: Any) -> Any:
            if isinstance(value, (datetime.datetime, datetime.date)):
                return value.isoformat()
            return value

        def build_date_range(body: dict[str, Any], clause: sa.sql.expression.BinaryExpression) -> dict[str, Any]:
            label: str = ""
            if isinstance(clause.left, sa.sql.elements.ColumnClause):
                label = clause.left.name
            elif isinstance(clause.left, sa.sql.elements.Cast):
                label = str(clause.left.anon_label)
            col_name = label.split(".")[-1].replace('"', "").replace("`", "")
            body["configParams"] = {"timeFilterColumn": col_name}
            op = clause.operator.__name__
            if op == "eq":
                body["dateRange"]["startDate"] = date_converter(clause.right.effective_value)
                body["dateRange"]["endDate"] = date_converter(clause.right.effective_value)
            elif op == "between_op":
                body["dateRange"]["startDate"] = date_converter(clause.right.clauses[0].effective_value)
                body["dateRange"]["endDate"] = date_converter(clause.right.clauses[1].effective_value)
            # a bold assumption
            elif op in ("gt", "ge"):
                body["dateRange"]["startDate"] = date_converter(clause.right.effective_value)
            elif op in ("lt", "le"):
                body["dateRange"]["endDate"] = date_converter(clause.right.effective_value)
            return body

        assert isinstance(dba_query.query, sa.sql.Select)
        sa_whereclause = dba_query.query.whereclause
        if sa_whereclause is not None:
            if isinstance(sa_whereclause, sa.sql.expression.BooleanClauseList):
                for clause in sa_whereclause.clauses:
                    body = build_date_range(body, clause)
                    # getting just first datetime filtration
                    break
            elif isinstance(sa_whereclause, sa.sql.expression.BinaryExpression):
                clause = sa_whereclause
                body = build_date_range(body, clause)

        flatten_body = {
            "key": body["key"],
            "startDate": body["dateRange"].get("startDate"),
            "endDate": body["dateRange"].get("endDate"),
            "timeFilterColumn": body.get("configParams", dict()).get("timeFilterColumn"),
            "limit": self._extract_limit(dba_query.query),
        }

        return body, flatten_body
