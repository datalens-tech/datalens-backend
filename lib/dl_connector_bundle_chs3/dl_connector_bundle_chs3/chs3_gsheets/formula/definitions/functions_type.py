from dl_formula.definitions.functions_type import (
    FuncDbCast2,
    FuncDbCast3,
    FuncDbCast4,
    FuncDbCastBase,
)

from dl_connector_bundle_chs3.chs3_gsheets.formula.constants import GSheetsFileS3Dialect as D
from dl_connector_bundle_chs3.chs3_gsheets.formula.utils import clickhouse_funcs_for_gsheets_v2_dialect
from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_connector_clickhouse.formula.definitions.functions_type import DEFINITIONS_TYPE as CH_DEFINITIONS_TYPE
from dl_connector_clickhouse.formula.definitions.functions_type import FuncDbCastClickHouseBase


CH_DEFINITIONS_TYPE = [func for func in CH_DEFINITIONS_TYPE if not issubclass(func.__class__, FuncDbCastBase)]


class FuncDbCastGSheetsBase(FuncDbCastClickHouseBase):
    WHITELISTS = {D.GSHEETS_V2: FuncDbCastClickHouseBase.WHITELISTS[ClickHouseDialect.CLICKHOUSE]}


class FuncDbCastGSheets2(FuncDbCastGSheetsBase, FuncDbCast2):
    pass


class FuncDbCastGSheets3(FuncDbCastGSheetsBase, FuncDbCast3):
    pass


class FuncDbCastGSheets4(FuncDbCastGSheetsBase, FuncDbCast4):
    pass


DEFINITIONS_TYPE = [
    *clickhouse_funcs_for_gsheets_v2_dialect(CH_DEFINITIONS_TYPE),
    FuncDbCastGSheets2(),
    FuncDbCastGSheets3(),
    FuncDbCastGSheets4(),
]
