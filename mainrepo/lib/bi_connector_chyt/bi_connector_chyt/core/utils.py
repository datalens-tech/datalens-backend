import logging
import re
from typing import Optional, Type

from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from bi_core import exc

from bi_connector_clickhouse.core.clickhouse_base.ch_commons import ClickHouseBaseUtils, ParsedErrorMsg


LOGGER = logging.getLogger(__name__)


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
    clique_expr_exc = {
        r'User "(?P<user>[-0-9a-zA-Z]+)" has no access to clique (?P<clique>\*\S+)': exc.CHYTCliqueAccessDenied,
        r'Clique (?P<clique>\*\S+) is not running': exc.CHYTCliqueIsNotRunning,
        r'Clique (?P<clique>\*\S+) is suspended': exc.CHYTCliqueIsSuspended,
        r'Invalid clique specification': exc.CHYTCliqueNotExists,
        r'Authentication failed': exc.CHYTAuthError,
    }

    @classmethod
    def parse_clique_message(cls, err_msg: str) -> Optional[tuple[Type[exc.DatabaseQueryError], dict[str, str]]]:
        for err_re, chyt_exc_cls in cls.clique_expr_exc.items():
            match = re.search(err_re, err_msg)
            if match:
                LOGGER.info('Recognized as CHYT error without code')
                return chyt_exc_cls, match.groupdict()
        return None

    @classmethod
    def get_exc_class_by_parsed_message(
        cls, msg: ParsedErrorMsg
    ) -> Optional[tuple[Type[exc.DatabaseQueryError], dict[str, str]]]:
        if msg.code == 1001:
            LOGGER.info('Recognized as CHYT error code')
            for err_re, chyt_exc_cls in cls.chyt_expr_exc.items():
                match = re.search(err_re, msg.full_msg)
                if match:
                    return chyt_exc_cls, match.groupdict()
            return cls.chyt_fallback_exc_cls, {}

        return super().get_exc_class_by_parsed_message(msg)

    @classmethod
    def get_exc_class(
        cls, err_msg: str
    ) -> Optional[tuple[Type[exc.DatabaseQueryError], dict[str, str]]]:
        # Clique exception
        not_ch_exc = cls.parse_clique_message(err_msg)
        if not_ch_exc:
            return not_ch_exc
        return super().get_exc_class(err_msg)

    @classmethod
    def get_tracing_sample_flag_override(cls, rci: DBAdapterScopedRCI) -> Optional[bool]:
        # We should set sample flag only if x_dl_debug_mode is presented and is True
        return rci.x_dl_debug_mode is True
