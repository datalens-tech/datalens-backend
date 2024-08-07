from __future__ import annotations

import logging
import re
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Pattern,
    Type,
)

import attr
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import ddl as sa_ddl
from sqlalchemy.sql import schema as sa_schema

from dl_core import exc
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connectors.base.error_transformer import DBExcKWArgs
from dl_core.db import SchemaColumn
from dl_type_transformer.sa_types import make_sa_type

from dl_connector_clickhouse.core.clickhouse_base.constants import BACKEND_TYPE_CLICKHOUSE
from dl_connector_clickhouse.core.clickhouse_base.exc import (
    CannotInsertNullInOrdinaryColumn,
    CHIncorrectData,
    CHReadonlyUser,
    ClickHouseSourceDoesNotExistError,
    EstimatedExecutionTooLong,
    InvalidSplitSeparator,
    TooManyColumns,
)


if TYPE_CHECKING:
    from dl_type_transformer.type_transformer import TypeTransformer

LOGGER = logging.getLogger(__name__)


def get_ch_settings(
    read_only_level: Optional[int] = None,
    max_execution_time: Optional[int] = None,
    insert_quorum: Optional[int] = None,
    insert_quorum_timeout: Optional[int] = None,
    output_format_json_quote_denormals: Optional[int] = None,
) -> dict:
    settings = {
        # https://clickhouse.com/docs/en/operations/settings/settings#settings-join_use_nulls
        # 1 — JOIN behaves the same way as in standard SQL.
        # The type of the corresponding field is converted to Nullable, and empty cells are filled with NULL.
        "join_use_nulls": 1,
        # Limits the time to wait for a response from the servers in the cluster.
        # If a ddl request has not been performed on all hosts, a response will contain
        # a timeout error and a request will be executed in an async mode.
        "distributed_ddl_task_timeout": 280,
        # https://clickhouse.com/docs/en/operations/settings/query-complexity#max-execution-time
        # Maximum query execution time in seconds.
        # By default, specify a large value to ensure there are no
        # forever-running queries (which is also known to break old-version CH
        # hosts at around 100_000 second long queries).
        # Note that in CH the value is rounded down to integer, and 0 seems to mean 'no limit'.
        # TODO: get rid of this parameter or figure out a proper way to use it, bc CH's estimates seem to be way off
        "max_execution_time": max_execution_time if max_execution_time is None else 3600 * 4,
        "readonly": read_only_level,
        # https://clickhouse.com/docs/en/operations/settings/settings#settings-insert_quorum
        # INSERT succeeds only when ClickHouse manages to correctly write data to the insert_quorum
        # of replicas during the insert_quorum_timeout
        "insert_quorum": insert_quorum,
        "insert_quorum_timeout": insert_quorum_timeout,
        # request clickhouse stat in response headers
        # otherwise clickhouse sends nulls in X-ClickHouse-Summary
        "send_progress_in_http_headers": 0,
        # https://clickhouse.com/docs/en/operations/settings/formats#output_format_json_quote_denormals
        # Enables +nan, -nan, +inf, -inf outputs in JSON output format.
        # After support from frontend should be enabled by default in all cases.
        "output_format_json_quote_denormals": output_format_json_quote_denormals,
    }

    return {k: v for k, v in settings.items() if v is not None}


@attr.s(frozen=True, auto_attribs=True)
class ParsedErrorMsg:
    full_msg: str
    code: int


class ClickHouseBaseUtils:
    add_real_user_header: ClassVar[bool] = False

    ch_err_msg_re: ClassVar[Pattern] = re.compile(r"^(std::exception\.\s+)?Code:\s+(?P<code>\d+)")

    # ClickHouse error codes list: https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
    # TODO: verify codes meaning and pick some other useful codes from CH list.
    map_err_code_exc_cls: ClassVar[dict[int, Type[exc.DatabaseQueryError]]] = {
        10: exc.ColumnDoesNotExist,
        36: InvalidSplitSeparator,
        41: exc.CannotParseDateTime,
        43: exc.InvalidArgumentType,
        46: exc.UnknownFunction,
        47: exc.ColumnDoesNotExist,
        53: exc.JoinColumnTypeMismatch,
        60: ClickHouseSourceDoesNotExistError,
        62: exc.InvalidQuery,
        70: exc.UnexpectedInfOrNan,
        72: exc.CannotParseNumber,
        81: exc.DatabaseDoesNotExist,
        117: CHIncorrectData,
        153: exc.DivisionByZero,
        159: exc.SourceTimeout,
        160: EstimatedExecutionTooLong,
        161: TooManyColumns,
        164: CHReadonlyUser,
        241: exc.DbMemoryLimitExceeded,
        277: exc.DBIndexNotUsed,
        349: CannotInsertNullInOrdinaryColumn,
        516: exc.DbAuthenticationFailed,
        1000: exc.NoSpaceLeft,
    }

    @classmethod
    def parse_message(cls, err_msg: str) -> Optional[ParsedErrorMsg]:
        match = cls.ch_err_msg_re.match(err_msg)

        if match is None:
            return None

        code = int(match.group("code"))
        LOGGER.info("Got CH error code %s", code)

        return ParsedErrorMsg(
            code=code,
            full_msg=err_msg,
        )

    @classmethod
    def get_exc_class_by_parsed_message(
        cls, msg: ParsedErrorMsg
    ) -> Optional[tuple[Type[exc.DatabaseQueryError], dict[str, str]]]:
        if msg.code in cls.map_err_code_exc_cls:
            return cls.map_err_code_exc_cls[msg.code], {}
        return None

    @classmethod
    def get_exc_class(cls, err_msg: str) -> Optional[tuple[Type[exc.DatabaseQueryError], dict[str, str]]]:
        parse_msg = cls.parse_message(err_msg)
        if parse_msg:
            return cls.get_exc_class_by_parsed_message(parse_msg)
        raise ValueError("Can not parse error message", err_msg)

    @classmethod
    def get_context_headers(cls, rci: DBAdapterScopedRCI) -> dict[str, str]:
        headers: dict[str, str] = {
            # TODO: bi_constants / bi_configs.constants
            "user-agent": "DataLens",
        }

        if rci.user_name and cls.add_real_user_header:
            headers["X-DataLens-Real-User"] = rci.user_name

        if rci.request_id:
            headers["x-request-id"] = rci.request_id

        if rci.client_ip:
            headers["X-Forwarded-For-Y"] = rci.client_ip

        return headers

    @classmethod
    def get_tracing_sample_flag_override(cls, rci: DBAdapterScopedRCI) -> Optional[bool]:
        """
        Return overridden sample flag value. If value should not be overridden `None` will be returned.
        """
        return None


class ClickHouseUtils(ClickHouseBaseUtils):
    pass


def create_column_sql(
    sa_dialect: DefaultDialect,
    col: SchemaColumn,
    tt: TypeTransformer,
    partition_fields: Optional[list[str]] = None,
) -> str:
    native_type = tt.type_user_to_native(user_t=col.user_type, native_t=col.native_type)

    nullable: Optional[bool]
    if partition_fields and col.name in partition_fields:
        # Partition column cannot be nullable. Enforcing it in here.
        nullable = False
    else:
        nullable = None

    if nullable is not None:
        native_type = native_type.as_common().clone(nullable=nullable)

    sa_type = make_sa_type(backend_type=BACKEND_TYPE_CLICKHOUSE, native_type=native_type)

    sa_column_obj = sa_schema.Column(col.name, sa_type)
    sa_cc_obj = sa_ddl.CreateColumn(sa_column_obj)
    sa_ddl_obj = sa_cc_obj.compile(dialect=sa_dialect)
    return sa_ddl_obj.string


def ensure_db_message(
    exc_cls: Type[exc.DatabaseQueryError],
    kw: DBExcKWArgs,
) -> tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
    db_message = kw.get("db_message")
    details = kw.get("details")
    if db_message and details is not None and not details.get("db_message"):
        details.update(db_message=db_message)
    return exc_cls, kw
