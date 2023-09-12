from clickhouse_sqlalchemy.orm.query import Query as CHQuery

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreConnector, CoreSourceDefinition,
)

from bi_connector_bundle_chs3.chs3_base.core.constants import BACKEND_TYPE_CHS3
from bi_connector_bundle_chs3.chs3_base.core.dto import BaseFileS3ConnDTO
from bi_connector_bundle_chs3.chs3_base.core.type_transformer import FileTypeTransformer


class BaseFileS3CoreConnectionDefinition(CoreConnectionDefinition):
    type_transformer_cls = FileTypeTransformer
    dialect_string = 'bi_clickhouse'


class BaseFileS3TableCoreSourceDefinition(CoreSourceDefinition):
    pass


class BaseFileS3CoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_CHS3
    safe_dto_classes = frozenset({BaseFileS3ConnDTO})
    query_cls = CHQuery
