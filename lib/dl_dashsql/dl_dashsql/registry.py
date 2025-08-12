from dl_constants.enums import SourceBackendType
from dl_dashsql.literalizer import DashSQLParamLiteralizer


_LITERALIZER_CLASSES: dict[SourceBackendType, type[DashSQLParamLiteralizer]] = {}


def get_dash_sql_param_literalizer(backend_type: SourceBackendType) -> DashSQLParamLiteralizer:
    literalizer_cls = _LITERALIZER_CLASSES[backend_type]
    return literalizer_cls()


def register_dash_sql_param_literalizer_cls(
    backend_type: SourceBackendType,
    literalizer_cls: type[DashSQLParamLiteralizer],
) -> None:
    if (registered_literalizer_cls := _LITERALIZER_CLASSES.get(backend_type)) is not None:
        assert registered_literalizer_cls is literalizer_cls
    else:
        _LITERALIZER_CLASSES[backend_type] = literalizer_cls
