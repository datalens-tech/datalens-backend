import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_constants.enums import UserDataType
from dl_dashsql.literalizer import DefaultDashSQLParamLiteralizer
from dl_dashsql.types import IncomingDSQLParamTypeExt


class OracleDashSQLParamLiteralizer(DefaultDashSQLParamLiteralizer):
    def get_sa_type(self, bi_type: UserDataType, value_base: IncomingDSQLParamTypeExt) -> TypeEngine:
        if bi_type == UserDataType.string:
            # See also: dl_formula/definitions/literals.py
            value_lst = [value_base] if isinstance(value_base, str) else value_base
            assert isinstance(value_lst, (list, tuple))
            max_len = max(len(val) for val in value_lst)  # type: ignore  # FIXME: Some kind of per-element checker
            try:
                for val in value_lst:
                    assert isinstance(val, str)
                    val.encode("ascii")
            except UnicodeEncodeError:
                return sa.NCHAR(max_len)
            return sa.CHAR(max_len)

        return super().get_sa_type(bi_type=bi_type, value_base=value_base)
