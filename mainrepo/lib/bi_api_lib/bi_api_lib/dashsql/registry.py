from typing import Type

from bi_constants.enums import SourceBackendType

from bi_api_connector.dashsql import DashSQLParamLiteralizer


_LITERALIZER_CLASSES: dict[SourceBackendType, Type[DashSQLParamLiteralizer]] = {}


def get_dash_sql_param_literalizer(backend_type: SourceBackendType) -> DashSQLParamLiteralizer:
    literalizer_cls = _LITERALIZER_CLASSES[backend_type]
    return literalizer_cls()


def register_dash_sql_param_literalizer_cls(
        backend_type: SourceBackendType,
        literalizer_cls: Type[DashSQLParamLiteralizer],
) -> None:
    if (registered_literalizer_cls := _LITERALIZER_CLASSES.get(backend_type)) is not None:
        assert registered_literalizer_cls is literalizer_cls
    else:
        _LITERALIZER_CLASSES[backend_type] = literalizer_cls
