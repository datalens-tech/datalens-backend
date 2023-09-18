import os

from dl_api_lib.query.registry import register_multi_query_mutator_factory_cls
from dl_connector_clickhouse.core.clickhouse_base.constants import BACKEND_TYPE_CLICKHOUSE
from dl_connector_clickhouse.formula.constants import ClickHouseDialect

# FIXME: Remove connectors
from bi_connector_mysql.core.constants import BACKEND_TYPE_MYSQL
from bi_connector_mysql.formula.constants import MySQLDialect
from dl_connector_postgresql.core.postgresql.constants import BACKEND_TYPE_POSTGRES
from dl_connector_postgresql.formula.constants import PostgreSQLDialect
from dl_query_processing.multi_query.factory import DefaultNativeWFMultiQueryMutatorFactory


def register_for_connectors_with_native_wf() -> None:
    """
    Register factories for connector/dialect combinations
    that support native window functions

    TODO: Connectorize and get rid of os.environ
    """

    if os.environ.get('NATIVE_WF_POSTGRESQL', '0') == '1':
        register_multi_query_mutator_factory_cls(
            backend_type=BACKEND_TYPE_POSTGRES,
            dialects=PostgreSQLDialect.and_above(PostgreSQLDialect.POSTGRESQL_9_4).to_list(),
            factory_cls=DefaultNativeWFMultiQueryMutatorFactory,
        )
    if os.environ.get('NATIVE_WF_CLICKHOUSE', '0') == '1':
        register_multi_query_mutator_factory_cls(
            backend_type=BACKEND_TYPE_CLICKHOUSE,
            dialects=ClickHouseDialect.and_above(ClickHouseDialect.CLICKHOUSE_22_10).to_list(),
            factory_cls=DefaultNativeWFMultiQueryMutatorFactory,
        )
    if os.environ.get('NATIVE_WF_MYSQL', '0') == '1':
        register_multi_query_mutator_factory_cls(
            backend_type=BACKEND_TYPE_MYSQL,
            dialects=MySQLDialect.and_above(MySQLDialect.MYSQL_8_0_12).to_list(),
            factory_cls=DefaultNativeWFMultiQueryMutatorFactory,
        )


# Native window functions
register_for_connectors_with_native_wf()
