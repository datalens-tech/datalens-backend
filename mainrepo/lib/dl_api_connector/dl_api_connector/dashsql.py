import abc

import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_constants.enums import BIType
from dl_core.exc import DashSQLError


TValueBase = str | list[str] | tuple[str, ...]

BI_TYPE_TO_SA_TYPE: dict[BIType, TypeEngine] = {
    BIType.string: sa.TEXT(),
    BIType.integer: sa.BIGINT(),
    BIType.float: sa.FLOAT(),
    BIType.date: sa.DATE(),
    BIType.datetime: sa.DATETIME(),
    BIType.boolean: sa.BOOLEAN(),
    BIType.datetimetz: sa.DATETIME(timezone=True),
    BIType.genericdatetime: sa.DATETIME(),
}


class DashSQLParamLiteralizer(abc.ABC):
    @abc.abstractmethod
    def get_sa_type(self, bi_type: BIType, value_base: TValueBase) -> TypeEngine:
        raise NotImplementedError


class DefaultDashSQLParamLiteralizer(DashSQLParamLiteralizer):
    def get_sa_type(self, bi_type: BIType, value_base: TValueBase) -> TypeEngine:
        try:
            sa_type = BI_TYPE_TO_SA_TYPE[bi_type]
            return sa_type
        except (KeyError, ValueError):
            raise DashSQLError(f"Unsupported type {bi_type.name!r}")
