from clickhouse_sqlalchemy.orm.query import Query as CHQuery

from dl_core.connections_security.base import (
    ConnSecuritySettings,
    NonUserInputConnectionSafetyChecker,
)
from dl_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)

from dl_connector_bundle_chs3.chs3_base.core.constants import BACKEND_TYPE_CHS3
from dl_connector_bundle_chs3.chs3_base.core.dto import BaseFileS3ConnDTO
from dl_connector_bundle_chs3.chs3_base.core.type_transformer import FileTypeTransformer
from dl_connector_clickhouse.core.clickhouse_base.query_compiler import ClickHouseQueryCompiler


class BaseFileS3CoreConnectionDefinition(CoreConnectionDefinition):
    type_transformer_cls = FileTypeTransformer
    dialect_string = "bi_clickhouse"


class BaseFileS3TableCoreSourceDefinition(CoreSourceDefinition):
    pass


class BaseFileS3CoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_CHS3
    conn_security = frozenset(
        {ConnSecuritySettings(NonUserInputConnectionSafetyChecker, frozenset({BaseFileS3ConnDTO}))}
    )
    query_cls = CHQuery
    compiler_cls = ClickHouseQueryCompiler
