from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import (
    TYPE_CHECKING,
    Generator,
    Sequence,
)

import sqlalchemy as sa

from dl_constants.enums import (
    DataSourceRole,
    ReportingQueryType,
)
import dl_core.exc as exc
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import ExpressionCtx
from dl_core.us_connection_base import ClassicConnectionSQL


if TYPE_CHECKING:
    from dl_core.connections_security.base import ConnectionSecurityManager
    from dl_core.us_connection_base import ConnectionBase


LOGGER = logging.getLogger(__name__)


@contextmanager
def select_data_context(role: DataSourceRole) -> Generator[None, None, None]:
    """
    Re-raise database-related errors taking dataset settings into account
    """
    try:
        yield
    except exc.SourceDoesNotExist as err:
        if role == DataSourceRole.materialization:
            raise exc.MaterializationNotFinished(db_message=err.db_message, query=err.query)
        raise


def get_value_range_query(expression: ExpressionCtx, dimension_filters: Sequence[ExpressionCtx] = ()) -> BIQuery:
    return BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=sa.func.min(expression.expression),
                alias="min_val",
                user_type=expression.user_type,
                avatar_ids=expression.avatar_ids,
            ),
            ExpressionCtx(
                expression=sa.func.max(expression.expression),
                alias="max_val",
                user_type=expression.user_type,
                avatar_ids=expression.avatar_ids,
            ),
        ],
        dimension_filters=dimension_filters,
    )


def get_query_type(connection: ConnectionBase, conn_sec_mgr: ConnectionSecurityManager) -> ReportingQueryType:
    if connection.is_always_internal_source:
        return ReportingQueryType.internal

    if isinstance(connection, ClassicConnectionSQL):
        if conn_sec_mgr.is_internal_connection(connection.get_conn_dto()):
            return ReportingQueryType.internal

    return ReportingQueryType.external
