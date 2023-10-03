import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_api_connector.dashsql import (
    DefaultDashSQLParamLiteralizer,
    TValueBase,
)
from dl_constants.enums import UserDataType


class OracleDashSQLParamLiteralizer(DefaultDashSQLParamLiteralizer):
    def get_sa_type(self, bi_type: UserDataType, value_base: TValueBase) -> TypeEngine:
        if bi_type == UserDataType.string:
            # See also: dl_formula/definitions/literals.py
            value_lst = [value_base] if isinstance(value_base, str) else value_base
            max_len = max(len(val) for val in value_lst)
            try:
                for val in value_lst:
                    val.encode("ascii")
            except UnicodeEncodeError:
                return sa.NCHAR(max_len)
            return sa.CHAR(max_len)

        return super().get_sa_type(bi_type=bi_type, value_base=value_base)
