from typing import (
    Mapping,
    Sequence,
)

import attr

from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_constants.types import TBIDataRow
from dl_dashsql.types import IncomingDSQLParamTypeExt


@attr.s(frozen=True, kw_only=True)
class TypedQueryParameter:
    name: str = attr.ib()
    user_type: UserDataType = attr.ib()
    value: IncomingDSQLParamTypeExt = attr.ib()


@attr.s(frozen=True, kw_only=True)
class TypedQuery:
    query_type: DashSQLQueryType = attr.ib()
    parameters: tuple[TypedQueryParameter, ...] = attr.ib()


@attr.s(frozen=True, kw_only=True)
class PlainTypedQuery(TypedQuery):
    query: str = attr.ib()


@attr.s(frozen=True, kw_only=True)
class TypedQueryResult:
    query_type: DashSQLQueryType = attr.ib()


@attr.s(frozen=True, kw_only=True)
class DataRowsTypedQueryResult(TypedQueryResult):
    data_rows: Sequence[TBIDataRow] = attr.ib()


@attr.s(frozen=True, kw_only=True)
class PreparedDBQuery:
    """
    A representation of a Python Database API-compliant query
    consisting of:
    - `paramstyle` specification
    - the query itself with parameter placeholders conforming to the specified paramstyle
    - parameters in the form (sequence or mapping) accepted the specified paramstyle
    """

    paramstyle: str = attr.ib()
    query: str = attr.ib()
    parameters: Sequence | Mapping = attr.ib()
