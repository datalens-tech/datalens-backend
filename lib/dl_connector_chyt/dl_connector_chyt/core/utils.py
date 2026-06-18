import logging
import re

from frozendict import frozendict

from dl_core import exc
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI

from dl_connector_chyt.core import exc as chyt_exc
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import (
    ClickHouseBaseUtils,
    ParsedErrorMsg,
)

LOGGER = logging.getLogger(__name__)


class CHYTUtils(ClickHouseBaseUtils):
    add_real_user_header = True

    chyt_expr_exc = frozendict(
        {
            r"Invalid sorted JOIN: (?P<col>.*) is not a key column": chyt_exc.CHYTInvalidSortedJoinNotAKeyColumnError,
            r"Invalid sorted JOIN: joined columns should form prefix of joined table key columns": chyt_exc.CHYTInvalidSortedJoinNotKeyPrefixColumnError,
            r"Invalid sorted JOIN: only single table may currently be joined": chyt_exc.CHYTInvalidSortedJoinMoreThanOneTableError,
            r"Invalid sorted JOIN: table (?P<table>.*) is not sorted": chyt_exc.CHYTInvalidSortedJoinTableNotSortedError,
            r"Invalid sorted JOIN: CHYT does not support compound expressions in ON/USING clause": chyt_exc.CHYTInvalidSortedJoinCompoundExpressionsNotSupportedError,
            r"Invalid sorted JOIN: key is empty": chyt_exc.CHYTInvalidSortedJoinKeyIsEmptyError,
            r"Invalid sorted JOIN: joining concatenation of multiple tables is not supported": chyt_exc.CHYTInvalidSortedJoinConcatNotSupportedError,
            r"Invalid sorted JOIN: joined columns (?P<col1>.*) and (?P<col2>.*)"
            r" do not occupy same positions in key columns of joined tables": chyt_exc.CHYTInvalidSortedJoinNotSameKeyPositionError,
            r"Invalid sorted JOIN": chyt_exc.CHYTInvalidSortedJoinError,
            r"Access denied": chyt_exc.CHYTTableAccessDeniedError,
            r"Error validating permissions for user": chyt_exc.CHYTTableAccessDeniedError,
            r"CHYT does not support tables without schema": chyt_exc.CHYTTableHasNoSchemaError,
            r"NYT::TErrorException: Memory limit \(total\) exceeded": exc.DbMemoryLimitExceededError,
            r"Error resolving path": exc.SourceDoesNotExistError,
            r"No tables to read from": exc.SourceDoesNotExistError,
            r"Reading multiple dynamic tables or dynamic table together with static table is not supported": chyt_exc.CHYTMultipleDynamicTablesNotSupportedError,
            r"Subquery exceeds data weight limit": chyt_exc.CHYTSubqueryWeightLimitExceededError,
        }
    )
    chyt_fallback_exc_cls = chyt_exc.CHYTQueryError
    clique_expr_exc = frozendict(
        {
            r'User "(?P<user>[-0-9a-zA-Z]+)" has no access to clique \*?(?P<clique>\S+)': chyt_exc.CHYTCliqueAccessDeniedError,
            r"Clique (?P<clique>\*\S+) is not running": chyt_exc.CHYTCliqueIsNotRunningError,
            r"Clique (?P<clique>\*\S+) is suspended": chyt_exc.CHYTCliqueIsSuspendedError,
            r"Invalid clique specification": chyt_exc.CHYTCliqueNotExistsError,
            r"Error parsing GUID \"(?P<clique>\S+)\"": chyt_exc.CHYTCliqueGuidParsingError,
            r"Authentication failed": chyt_exc.CHYTAuthError,
        }
    )

    @classmethod
    def parse_clique_message(cls, err_msg: str) -> tuple[type[exc.DatabaseQueryError], dict[str, str]] | None:
        for err_re, chyt_exc_cls in cls.clique_expr_exc.items():
            match = re.search(err_re, err_msg)
            if match:
                LOGGER.info("Recognized as CHYT error without code")
                return chyt_exc_cls, match.groupdict()
        return None

    @classmethod
    def get_exc_class_by_parsed_message(
        cls, msg: ParsedErrorMsg
    ) -> tuple[type[exc.DatabaseQueryError], dict[str, str]] | None:
        if msg.code == 1001:
            LOGGER.info("Recognized as CHYT error code")
            for err_re, chyt_exc_cls in cls.chyt_expr_exc.items():
                match = re.search(err_re, msg.full_msg)
                if match:
                    return chyt_exc_cls, match.groupdict()
            return cls.chyt_fallback_exc_cls, {}

        return super().get_exc_class_by_parsed_message(msg)

    @classmethod
    def get_exc_class(cls, err_msg: str) -> tuple[type[exc.DatabaseQueryError], dict[str, str]] | None:
        # Clique exception
        not_ch_exc = cls.parse_clique_message(err_msg)
        if not_ch_exc:
            return not_ch_exc
        return super().get_exc_class(err_msg)

    @classmethod
    def get_tracing_sample_flag_override(cls, rci: DBAdapterScopedRCI) -> bool | None:
        # We should set sample flag only if x_dl_debug_mode is presented and is True
        return rci.x_dl_debug_mode is True
