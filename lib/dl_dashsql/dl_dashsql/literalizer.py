import abc

import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_constants.enums import UserDataType
from dl_dashsql.exc import DashSQLError
from dl_dashsql.types import IncomingDSQLParamTypeExt


BI_TYPE_TO_SA_TYPE: dict[UserDataType, TypeEngine] = {
    UserDataType.string: sa.TEXT(),
    UserDataType.integer: sa.BIGINT(),
    UserDataType.float: sa.FLOAT(),
    UserDataType.date: sa.DATE(),
    UserDataType.datetime: sa.DATETIME(),
    UserDataType.boolean: sa.BOOLEAN(),
    UserDataType.datetimetz: sa.DATETIME(timezone=True),
    UserDataType.genericdatetime: sa.DATETIME(),
}


class DashSQLParamLiteralizer(abc.ABC):
    @abc.abstractmethod
    def get_sa_type(self, bi_type: UserDataType, value_base: IncomingDSQLParamTypeExt) -> TypeEngine:
        raise NotImplementedError


class DefaultDashSQLParamLiteralizer(DashSQLParamLiteralizer):
    def get_sa_type(self, bi_type: UserDataType, value_base: IncomingDSQLParamTypeExt) -> TypeEngine:
        try:
            sa_type = BI_TYPE_TO_SA_TYPE[bi_type]
            return sa_type
        except (KeyError, ValueError):
            raise DashSQLError(f"Unsupported type {bi_type.name!r}")
