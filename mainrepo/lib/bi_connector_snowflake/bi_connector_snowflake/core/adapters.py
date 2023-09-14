from __future__ import annotations

from typing import (
    Any,
    Callable,
    Optional,
)

import attr
from snowflake import sqlalchemy as ssa
from snowflake.connector import connect as sf_connect
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.sql.type_api import TypeEngine

from bi_core.connection_executors.adapters.adapters_base_sa import BaseSAAdapter
from bi_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from bi_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawSchemaInfo,
)
from bi_core.connection_models.common_models import (
    DBIdent,
    SATextTableDefinition,
)
from bi_core.db.native_type import SATypeSpec

from bi_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from bi_connector_snowflake.core.error_transformer import snowflake_error_transformer
from bi_connector_snowflake.core.target_dto import SnowFlakeConnTargetDTO


def construct_creator_func(target_dto: SnowFlakeConnTargetDTO) -> Callable:
    def get_connection() -> Any:
        params = dict(
            user=target_dto.user_name,
            account=target_dto.account_name,
            authenticator="oauth",
            token=target_dto.access_token,
            cache_column_metadata=True,
            database=target_dto.db_name,
            schema=target_dto.schema,
            warehouse=target_dto.warehouse,
        )
        if target_dto.user_role:
            params["role"] = target_dto.user_role

        conn = sf_connect(**params)

        # note: let's keep following lines commented for some time,
        #   at one point SF adapter did not respect db/schema/wh passed to connect
        #   in the recent runs it seems fixed
        # conn.cursor().execute(f"use database {target_dto.db_name}")
        # conn.cursor().execute(f"use schema {target_dto.schema}")
        # conn.cursor().execute(f"use warehouse {target_dto.warehouse}")
        return conn

    return get_connection


@attr.s(kw_only=True)
class SnowFlakeDefaultAdapter(BaseClassicAdapter, BaseSAAdapter[SnowFlakeConnTargetDTO]):
    conn_type = CONNECTION_TYPE_SNOWFLAKE
    _error_transformer = snowflake_error_transformer

    # https://docs.snowflake.com/en/user-guide/python-connector-api#type-codes
    _type_code_to_sa = {
        0: ssa.DECIMAL,
        1: ssa.REAL,
        2: ssa.STRING,
        3: ssa.DATE,
        4: ssa.TIMESTAMP,
        5: ssa.VARIANT,
        6: ssa.TIMESTAMP_LTZ,
        7: ssa.TIMESTAMP_TZ,
        8: ssa.TIMESTAMP_TZ,
        9: ssa.OBJECT,
        10: ssa.ARRAY,
        11: ssa.BINARY,
        12: ssa.TIME,
        13: ssa.BOOLEAN,
    }

    def _cursor_column_to_sa(self, cursor_col, require: bool = True) -> Optional[SATypeSpec]:  # type: ignore  # TODO: fix
        """
        cursor_col:

            name, type_code, display_size, internal_size, precision, scale, is_nullable

            https://docs.snowflake.com/en/user-guide/python-connector-api#id4
        """
        type_code = cursor_col[1]
        if type_code is None:  # shouldn't really happen
            if require:
                raise ValueError(f"Cursor column has no type: {cursor_col!r}")
            return None
        sa_cls = self._type_code_to_sa.get(type_code)
        if sa_cls is None:
            if require:
                raise ValueError(f"Unknown cursor type: {type_code!r}")
            return None

        if sa_cls is ssa.NUMBER:
            # See also: `self.normalize_sa_col_type`
            precision = cursor_col[4]
            scale = cursor_col[5]

            # taken from Oracle adapter, not sure if applicapble for Snowflake
            # # Going by the comparison with the 'create view' -> SA logic.
            # if scale == -127:
            #     scale = 0
            sa_type = sa_cls(precision, scale)
        else:
            sa_type = sa_cls

        return sa_type

    def get_default_db_name(self) -> Optional[str]:
        return None

    def get_db_name_for_query(self, db_name_from_query: Optional[str]) -> str:
        return self._target_dto.db_name

    def _get_db_engine(self, db_name: str, disable_streaming: bool = False) -> Engine:
        if disable_streaming:
            raise Exception("`disable_streaming` is not applicable here")
        return sa.create_engine(
            "snowflake://not@used/db",
            creator=construct_creator_func(self._target_dto),
        ).execution_options(compiled_cache=None)

    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return self.execute(DBAdapterQuery("SELECT CURRENT_VERSION()", db_name=db_ident.db_name)).get_all()[0][0]

    def normalize_sa_col_type(self, sa_col_type: TypeEngine) -> TypeEngine:
        if isinstance(sa_col_type, ssa.DECIMAL) and sa_col_type.scale == 0:
            return sa.Integer()

        return super().normalize_sa_col_type(sa_col_type)

    def _get_subselect_table_info(self, subquery: SATextTableDefinition) -> RawSchemaInfo:
        raw_cursor_info, _ = self._get_subselect_raw_cursor_info_and_data(subquery.text)
        return self._raw_cursor_info_to_schema(raw_cursor_info)
