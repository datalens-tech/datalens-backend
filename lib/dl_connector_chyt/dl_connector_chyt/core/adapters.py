from __future__ import annotations

import abc
from typing import (
    Any,
    ClassVar,
    Optional,
    Type,
)

from aiochclient.http_clients import aiohttp
import attr

from dl_constants.enums import IndexKind
from dl_core import exc
from dl_core.connection_executors.models.db_adapter_data import RawIndexInfo
from dl_core.connection_models import TableIdent
from dl_core.connectors.base.error_transformer import DBExcKWArgs
from dl_core.utils import get_current_w3c_tracing_headers
from dl_utils.aio import await_sync
from dl_utils.utils import make_url

from dl_connector_chyt.core.constants import CONNECTION_TYPE_CHYT
from dl_connector_chyt.core.target_dto import (
    BaseCHYTConnTargetDTO,
    CHYTConnTargetDTO,
)
from dl_connector_chyt.core.utils import CHYTUtils
from dl_connector_clickhouse.core.clickhouse_base.adapters import (
    BaseClickHouseAdapter,
    BaseClickHouseConnLineConstructor,
)
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import (
    ensure_db_message,
    get_ch_settings,
)


class CHYTConnLineConstructor(BaseClickHouseConnLineConstructor):
    def _get_dsn_params(
        self,
        safe_db_symbols: tuple[str, ...] = (),
        db_name: Optional[str] = None,
        standard_auth: Optional[bool] = True,
    ) -> dict:
        new_safe_symbols = safe_db_symbols + ("*",)
        return super()._get_dsn_params(safe_db_symbols=new_safe_symbols, db_name=db_name, standard_auth=standard_auth)


class BaseCHYTAdapter(BaseClickHouseAdapter, abc.ABC):
    ch_utils = CHYTUtils
    conn_line_constructor_type: ClassVar[Type[CHYTConnLineConstructor]] = CHYTConnLineConstructor

    _target_dto: BaseCHYTConnTargetDTO = attr.ib()

    def _get_dsn_params_from_headers(self) -> dict[str, str]:
        params = {
            "statuses_to_retry": "500,502,504",
            **super()._get_dsn_params_from_headers(),
        }
        params.update(
            **self._convert_headers_to_dsn_params(
                get_current_w3c_tracing_headers(
                    override_sampled=self.ch_utils.get_tracing_sample_flag_override(self._req_ctx_info),
                    req_id=self._req_ctx_info.request_id,
                ),
            )
        )
        return params

    def get_ch_settings(self) -> dict:
        return get_ch_settings(
            max_execution_time=self._target_dto.max_execution_time,
            read_only_level=2,
            output_format_json_quote_denormals=1,
        )

    # Fat boilerplate of an override.
    @classmethod
    def make_exc(  # TODO:  Move to ErrorTransformer
        cls,
        wrapper_exc: Exception,
        orig_exc: Optional[Exception],
        debug_compiled_query: Optional[str],
        **kwargs: Any,
    ) -> tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        exc_cls, kw = super().make_exc(
            wrapper_exc=wrapper_exc,
            orig_exc=orig_exc,
            debug_compiled_query=debug_compiled_query,
            **kwargs,  # type: ignore
        )
        return ensure_db_message(exc_cls, kw)

    # noinspection PyMethodMayBeStatic
    async def _fetch_yt_sorting_columns(
        self,
        host: str,
        table_path: str,
        secret_auth_headers: dict[str, str],
    ) -> Optional[RawIndexInfo]:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                make_url(
                    protocol="https",
                    host=host,
                    port=443,
                    path="/api/v3/get",
                ),
                params=dict(path=f"{table_path}/@schema"),
                headers={
                    **secret_auth_headers,
                    "accept": "application/json",
                },
            ) as resp:
                resp.raise_for_status()
                resp_js = await resp.json()

        sorting_columns = tuple(
            col_js["name"] for col_js in resp_js["$value"] if isinstance(col_js.get("sort_order"), str)
        )

        if sorting_columns:
            return RawIndexInfo(
                columns=sorting_columns,
                unique=resp_js["$attributes"]["unique_keys"],
                kind=IndexKind.table_sorting,
            )
        else:
            return None

    @abc.abstractmethod
    async def _get_yt_table_index_info(
        self,
        table_path: str,
        secret_auth_headers: dict[str, str],  # Strange name to prevent leaking to Sentry
    ) -> Optional[RawIndexInfo]:
        raise NotImplementedError()

    @abc.abstractmethod
    def _get_yt_proxy_auth_headers(self) -> dict[str, str]:
        raise NotImplementedError()

    def _get_table_indexes(self, table_ident: TableIdent) -> tuple[RawIndexInfo, ...]:
        """Even does not try to use SA to fetch indexes"""

        may_be_sorting_idx = await_sync(
            self._get_yt_table_index_info(
                table_path=table_ident.table_name, secret_auth_headers=self._get_yt_proxy_auth_headers()
            )
        )
        return (may_be_sorting_idx,) if may_be_sorting_idx is not None else ()


class CHYTAdapter(BaseCHYTAdapter):
    conn_type = CONNECTION_TYPE_CHYT

    _target_dto: CHYTConnTargetDTO = attr.ib()

    def _get_yt_proxy_auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"OAuth {self._target_dto.password}"}

    async def _get_yt_table_index_info(
        self,
        table_path: str,
        secret_auth_headers: dict[str, str],
    ) -> Optional[RawIndexInfo]:
        assert isinstance(table_path, str) and table_path.startswith("//"), "Incorrect YT table path"
        return await self._fetch_yt_sorting_columns(self._target_dto.host, table_path, secret_auth_headers)
