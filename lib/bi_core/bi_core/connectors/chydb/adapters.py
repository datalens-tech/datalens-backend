from __future__ import annotations

from typing import Tuple, Optional, Type, ClassVar

from bi_constants.enums import ConnectionType

from bi_core import exc
from bi_core.connectors.clickhouse_base.adapters import (
    ClickHouseAdapter, BaseClickHouseConnLineConstructor,
)
from bi_core.connectors.clickhouse_base.ch_commons import (
    CHYDBUtils, get_ch_settings, ensure_db_message,
)
from bi_core.connectors.base.error_transformer import DBExcKWArgs


class CHYDBAdapterConnLineConstructor(BaseClickHouseConnLineConstructor):
    def _get_dsn_query_params(self) -> dict:
        return {
            **super()._get_dsn_query_params(),
            'statuses_to_retry': '500,502,504',
        }


class CHYDBAdapter(ClickHouseAdapter):
    conn_type = ConnectionType.chydb
    ch_utils = CHYDBUtils  # type: ignore  # TODO: fix
    conn_line_constructor_type: ClassVar[Type[CHYDBAdapterConnLineConstructor]] = CHYDBAdapterConnLineConstructor

    def get_ch_settings(self) -> dict:
        return get_ch_settings(
            max_execution_time=self._target_dto.max_execution_time,
            read_only_level=2,
            output_format_json_quote_denormals=1,
        )

    def get_default_db_name(self) -> Optional[str]:
        return 'default'

    def _test(self) -> None:
        super()._test()
        # Synopsis:
        # https://st.yandex-team.ru/BI-1338#5e690a670c1b9028f531c6d5
        # CHYDB doesn't check authentication (token) on `select 1`;
        # as a temporary fix, poke blackbox,
        # to at least catch cases like "username in the token field".
        token = self._target_dto.password
        from bi_blackbox_client.authenticate import authenticate
        auth_res = authenticate(
            authorization_header='OAuth {}'.format(token),
            # TODO: might need actual context or better mockup-values.
            userip='::1',
            host='localhost',
            # NOTE: Not currently required, but might be in the future, but this hack might be removed by then:
            # scopes=('ydb:api',),
            scopes=(),
            force_tvm=True,  # mostly because this happens in tests.
        )
        if not auth_res['user_id']:
            raise exc.CHYDBQueryError(
                db_message='Token is invalid: {!r}'.format(auth_res),
                query='select 1',
            )

    # Copy of fat boilerplate of an override.
    @classmethod
    def make_exc(  # type: ignore  # TODO: fix   # TODO:  Move to ErrorTransformer
            cls,
            wrapper_exc: Exception,
            orig_exc: Optional[Exception],
            debug_compiled_query: Optional[str],
            **kwargs,
    ) -> Tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        exc_cls, kw = super().make_exc(  # type: ignore  # TODO: fix
            wrapper_exc=wrapper_exc,
            orig_exc=orig_exc,
            debug_compiled_query=debug_compiled_query,
            **kwargs)
        return ensure_db_message(exc_cls, kw)
