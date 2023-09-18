from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    List,
    Optional,
    Type,
)

import attr

from bi_cloud_integration.exc import YCUnauthenticated
from bi_sqlalchemy_yq.errors import YQError
from dl_constants.enums import ConnectionType
from dl_core import exc
from dl_core.connection_executors.adapters.adapters_base_sa_classic import ClassicSQLConnLineConstructor
from dl_core.connection_models import TableIdent

from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ
from bi_connector_yql.core.yq.target_dto import YQConnTargetDTO
from bi_connector_yql.core.yql_base.adapter import YQLAdapterBase

if TYPE_CHECKING:
    from dl_core.connection_models import SchemaIdent
    from dl_core.connectors.base.error_transformer import DBExcKWArgs


LOGGER = logging.getLogger(__name__)


class YQConnLineConstructor(ClassicSQLConnLineConstructor[YQConnTargetDTO]):
    def _get_dsn_query_params(self) -> dict:
        return dict(
            cloud_id=self._target_dto.cloud_id,
            folder_id=self._target_dto.folder_id,
        )


@attr.s
class YQAdapter(YQLAdapterBase[YQConnTargetDTO]):
    conn_type: ClassVar[ConnectionType] = CONNECTION_TYPE_YQ
    conn_line_constructor_type: ClassVar[Type[YQConnLineConstructor]] = YQConnLineConstructor

    EXTRA_EXC_CLS = (YQError, YCUnauthenticated)

    def _get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        raise Exception("TODO")

    @classmethod
    def make_exc(
        cls, wrapper_exc: Exception, orig_exc: Optional[Exception], debug_compiled_query: Optional[str]
    ) -> tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        if isinstance(wrapper_exc, YCUnauthenticated):
            orig_exc = wrapper_exc
        return super().make_exc(wrapper_exc, orig_exc, debug_compiled_query)
