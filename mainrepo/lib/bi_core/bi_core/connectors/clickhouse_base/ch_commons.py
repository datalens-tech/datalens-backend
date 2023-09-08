from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Pattern, Type

import attr
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import schema as sa_schema, ddl as sa_ddl

from bi_core import exc
from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from bi_core.connectors.clickhouse_base.exc import ClickHouseSourceDoesNotExistError
from bi_core.db import SchemaColumn, make_sa_type

if TYPE_CHECKING:
    from bi_core.db.conversion_base import TypeTransformer

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
        'join_use_nulls': 1,

        # Limits the time to wait for a response from the servers in the cluster.
        # If a ddl request has not been performed on all hosts, a response will contain
        # a timeout error and a request will be executed in an async mode.
        'distributed_ddl_task_timeout': 280,

        # https://clickhouse.com/docs/en/operations/settings/query-complexity#max-execution-time
        # Maximum query execution time in seconds.
        # By default, specify a large value to ensure there are no
        # forever-running queries (which is also known to break old-version CH
        # hosts at around 100_000 second long queries).
        # Note that in CH the value is rounded down to integer, and 0 seems to mean 'no limit'.
        'max_execution_time': max_execution_time if max_execution_time is None else 3600 * 4,

        'readonly': read_only_level,

        # https://clickhouse.com/docs/en/operations/settings/settings#settings-insert_quorum
        # INSERT succeeds only when ClickHouse manages to correctly write data to the insert_quorum
        # of replicas during the insert_quorum_timeout
        'insert_quorum': insert_quorum,
        'insert_quorum_timeout': insert_quorum_timeout,

        # request clickhouse stat in response headers for BI-1775
        # otherwise clickhouse sends nulls in X-ClickHouse-Summary
        'send_progress_in_http_headers': 0,  # temporary disabling for DLHELP-1730 investigation

        # https://clickhouse.com/docs/en/operations/settings/formats#output_format_json_quote_denormals
        # Enables +nan, -nan, +inf, -inf outputs in JSON output format.
        # Currently enabling only in sync wrapper (for materializer).
        # After CHARTS-3488 should be enabled by default in all cases.
        'output_format_json_quote_denormals': output_format_json_quote_denormals,
    }

    return {
        k: v for k, v in settings.items() if v is not None
    }


@attr.s(frozen=True, auto_attribs=True)
class ParsedErrorMsg:
    full_msg: str
    code: int


class ClickHouseBaseUtils:
    add_real_user_header: ClassVar[bool] = False

    ch_err_msg_re: ClassVar[Pattern] = re.compile(r'^(std::exception\.\s+)?Code:\s+(?P<code>\d+)')

    # ClickHouse error codes list: https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
    # TODO: verify codes meaning and pick some other useful codes from CH list.
    map_err_code_exc_cls: ClassVar[Dict[int, Type[exc.DatabaseQueryError]]] = {
        10: exc.ColumnDoesNotExist,
        36: exc.InvalidSplitSeparator,
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
        117: exc.CHIncorrectData,
        153: exc.DivisionByZero,
        159: exc.SourceTimeout,
        160: exc.EstimatedExecutionTooLong,
        161: exc.TooManyColumns,
        164: exc.CHReadonlyUser,
        241: exc.DbMemoryLimitExceeded,
        277: exc.DBIndexNotUsed,
        349: exc.CannotInsertNullInOrdinaryColumn,
        516: exc.DbAuthenticationFailed,
        1000: exc.NoSpaceLeft,
    }

    expr_exc = {
        r'User "(?P<user>[-0-9a-zA-Z]+)" has no access to clique (?P<clique>\*\S+)': exc.CHYTCliqueAccessDenied,
        r'Clique (?P<clique>\*\S+) is not running': exc.CHYTCliqueIsNotRunning,
        r'Clique (?P<clique>\*\S+) is suspended': exc.CHYTCliqueIsSuspended,
        r'Invalid clique specification': exc.CHYTCliqueNotExists,
        r'Authentication failed': exc.CHYTAuthError,
    }

    @classmethod
    def parse_message(cls, err_msg: str) -> Optional[ParsedErrorMsg]:
        match = cls.ch_err_msg_re.match(err_msg)

        if match is None:
            return None

        code = int(match.group('code'))
        LOGGER.info('Got CH error code %s', code)

        return ParsedErrorMsg(
            code=code,
            full_msg=err_msg,
        )

    @classmethod
    def parse_clique_message(cls, err_msg: str) -> Optional[tuple[Type[exc.DatabaseQueryError], Dict[str, str]]]:
        for err_re, chyt_exc_cls in cls.expr_exc.items():
            match = re.search(err_re, err_msg)
            if match:
                LOGGER.info('Recognized as CHYT error without code')
                return chyt_exc_cls, match.groupdict()
        return None

    @classmethod
    def get_exc_class_by_parsed_message(
        cls, msg: ParsedErrorMsg
    ) -> Optional[tuple[Type[exc.DatabaseQueryError], Dict[str, str]]]:
        if msg.code in cls.map_err_code_exc_cls:
            return cls.map_err_code_exc_cls[msg.code], {}
        return None

    @classmethod
    def get_exc_class(
        cls, err_msg: str
    ) -> Optional[tuple[Type[exc.DatabaseQueryError], Dict[str, str]]]:
        # ClickHouse exception (contains "Code:")
        parse_msg = cls.parse_message(err_msg)
        if parse_msg:
            return cls.get_exc_class_by_parsed_message(parse_msg)
        # Clique exception
        not_ch_exc = cls.parse_clique_message(err_msg)
        if not_ch_exc:
            return not_ch_exc
        # Not CH/CHYT exception
        raise ValueError("Can not parse error message", err_msg)

    @classmethod
    def get_context_headers(cls, rci: DBAdapterScopedRCI) -> Dict[str, str]:
        headers: Dict[str, str] = {
            # TODO: bi_constants / bi_configs.constants
            'user-agent': 'DataLens',
        }

        if rci.user_name and cls.add_real_user_header:
            headers['X-DataLens-Real-User'] = rci.user_name

        if rci.request_id:
            headers['x-request-id'] = rci.request_id

        if rci.client_ip:
            headers['X-Forwarded-For-Y'] = rci.client_ip

        return headers

    @classmethod
    def get_tracing_sample_flag_override(cls, rci: DBAdapterScopedRCI) -> Optional[bool]:
        """
        Return overridden sample flag value. If value should not be overridden `None` will be returned.
        """
        return None


class ClickHouseUtils(ClickHouseBaseUtils):
    pass


class CHYTUtils(ClickHouseBaseUtils):
    add_real_user_header = True

    chyt_expr_exc = {
        r'Invalid sorted JOIN: (?P<col>.*) is not a key column': exc.CHYTISJNotAKeyColumn,
        r'Invalid sorted JOIN: joined columns should form prefix of joined table key columns':
            exc.CHYTISJNotKeyPrefixColumn,
        r'Invalid sorted JOIN: only single table may currently be joined': exc.CHYTISJMoreThanOneTable,
        r'Invalid sorted JOIN: table (?P<table>.*) is not sorted': exc.CHYTISJTableNotSorted,
        r'Invalid sorted JOIN: CHYT does not support compound expressions in ON/USING clause':
            exc.CHYTISJCompoundExpressionsNotSupported,
        r'Invalid sorted JOIN: key is empty': exc.CHYTISJKeyIsEmpty,
        r'Invalid sorted JOIN: joining concatenation of multiple tables is not supported':
            exc.CHYTISJConcatNotSupported,
        r'Invalid sorted JOIN: joined columns (?P<col1>.*) and (?P<col2>.*)'
            r' do not occupy same positions in key columns of joined tables': exc.CHYTISJNotSameKeyPosition,
        r'Invalid sorted JOIN': exc.CHYTInvalidSortedJoin,
        r'Access denied': exc.CHYTTableAccessDenied,
        r'Error validating permissions for user': exc.CHYTTableAccessDenied,
        r'CHYT does not support tables without schema': exc.CHYTTableHasNoSchema,
        r'NYT::TErrorException: Memory limit \(total\) exceeded': exc.DbMemoryLimitExceeded,
        r'Error resolving path': exc.SourceDoesNotExist,
        r'No tables to read from': exc.SourceDoesNotExist,
        r'Reading multiple dynamic tables or dynamic table together with static table is not supported':
            exc.CHYTMultipleDynamicTablesNotSupported,
        r'Subquery exceeds data weight limit': exc.CHYTSubqueryWeightLimitExceeded,
    }
    chyt_fallback_exc_cls = exc.CHYTQueryError

    @classmethod
    def get_exc_class_by_parsed_message(
        cls, msg: ParsedErrorMsg
    ) -> Optional[tuple[Type[exc.DatabaseQueryError], Dict[str, str]]]:
        if msg.code == 1001:
            LOGGER.info('Recognized as CHYT error code')
            for err_re, chyt_exc_cls in cls.chyt_expr_exc.items():
                match = re.search(err_re, msg.full_msg)
                if match:
                    return chyt_exc_cls, match.groupdict()
            return cls.chyt_fallback_exc_cls, {}

        return super().get_exc_class_by_parsed_message(msg)

    @classmethod
    def get_tracing_sample_flag_override(cls, rci: DBAdapterScopedRCI) -> Optional[bool]:
        # We should set sample flag only if x_dl_debug_mode is presented and is True
        return rci.x_dl_debug_mode is True


def get_chyt_user_auth_headers(
    authorization: Optional[str],
    cookie: Optional[str],
    csrf_token: Optional[str]
) -> Dict[str, str]:
    auth_headers = {}
    if authorization is not None:
        auth_headers['Authorization'] = authorization
    if cookie is not None:
        auth_headers['Cookie'] = cookie

    assert auth_headers, "need non-empty context.auth_headers for the conn_line"

    if csrf_token is not None:
        auth_headers['X-CSRF-Token'] = csrf_token

    return auth_headers


def get_clickhouse_on_cluster_expr(cluster_name: str) -> str:
    if cluster_name is not None:
        return 'ON CLUSTER `{cluster_name}`'.format(cluster_name=cluster_name)
    return ''


def get_clickhouse_format_string() -> str:
    return 'FORMAT JSONCompact'


def create_column_sql(
    sa_dialect: DefaultDialect,
    col: SchemaColumn,
    tt: TypeTransformer,
    partition_fields: Optional[List[str]] = None,
) -> str:
    native_type = tt.type_user_to_native(
        user_t=col.user_type, native_t=col.native_type)

    if partition_fields and col.name in partition_fields:
        # Partition column cannot be nullable. Enforcing it in here.
        nullable = False
    else:
        nullable = None  # type: ignore  # TODO: fix

    if nullable is not None:
        native_type = native_type.as_common().clone(nullable=nullable)

    sa_type = make_sa_type(native_type)

    sa_column_obj = sa_schema.Column(col.name, sa_type)
    sa_cc_obj = sa_ddl.CreateColumn(sa_column_obj)
    sa_ddl_obj = sa_cc_obj.compile(dialect=sa_dialect)
    return sa_ddl_obj.string


def ensure_db_message(exc_cls, kw):  # type: ignore  # TODO: fix
    db_message = kw.get('db_message')
    details = kw.get('details')
    if db_message and details is not None and not details.get('db_message'):
        details.update(db_message=db_message)
    return exc_cls, kw
