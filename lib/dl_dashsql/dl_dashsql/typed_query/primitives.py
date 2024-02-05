from typing import (
    Mapping,
    Sequence,
    Type,
    TypeVar,
)

import attr

from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_constants.types import TBIDataRow
import dl_dashsql.exc as exc
from dl_dashsql.types import IncomingDSQLParamTypeExt


@attr.s(frozen=True, kw_only=True)
class TypedQueryParameter:
    name: str = attr.ib()
    user_type: UserDataType = attr.ib()
    value: IncomingDSQLParamTypeExt = attr.ib()


_PARAM_VALUE_TV = TypeVar("_PARAM_VALUE_TV")


@attr.s(frozen=True, kw_only=True)
class TypedQueryParamGetter:
    parameters: tuple[TypedQueryParameter, ...] = attr.ib()
    # internal
    _params_by_name: Mapping[str, TypedQueryParameter] = attr.ib(init=False)

    @_params_by_name.default
    def _default_params_by_name(self) -> Mapping[str, TypedQueryParameter]:
        return {param.name: param for param in self.parameters}

    def get_strict(self, name: str) -> TypedQueryParameter:
        return self._params_by_name[name]

    # typed methods
    def get_typed_value(self, name: str, value_type: Type[_PARAM_VALUE_TV]) -> _PARAM_VALUE_TV:
        try:
            value = self.get_strict(name).value
            if not isinstance(value, value_type):
                raise exc.DashSQLParameterError(f"Parameter {name!r} has invalid type")
            return value

        except KeyError as e:
            raise exc.DashSQLParameterError(f"Required parameter {name!r} is missing") from e


@attr.s(frozen=True, kw_only=True)
class TypedQuery:
    query_type: DashSQLQueryType = attr.ib()
    parameters: tuple[TypedQueryParameter, ...] = attr.ib()
    # internal
    param_getter: TypedQueryParamGetter = attr.ib(init=False)

    @param_getter.default
    def _default_params_by_name(self) -> TypedQueryParamGetter:
        return TypedQueryParamGetter(parameters=self.parameters)


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
