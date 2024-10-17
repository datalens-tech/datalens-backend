from dl_formula.definitions.functions_type import (
    FuncDbCast2,
    FuncDbCast3,
    FuncDbCast4,
    FuncDbCastBase,
)

from dl_connector_bundle_chs3.file.formula.constants import FileS3Dialect as D
from dl_connector_bundle_chs3.file.formula.utils import clickhouse_funcs_for_file_dialect
from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_connector_clickhouse.formula.definitions.functions_type import DEFINITIONS_TYPE as CH_DEFINITIONS_TYPE
from dl_connector_clickhouse.formula.definitions.functions_type import FuncDbCastClickHouseBase


CH_DEFINITIONS_TYPE = [func for func in CH_DEFINITIONS_TYPE if not issubclass(func.__class__, FuncDbCastBase)]


class FuncDbCastFileBase(FuncDbCastClickHouseBase):
    WHITELISTS = {D.FILE: FuncDbCastClickHouseBase.WHITELISTS[ClickHouseDialect.CLICKHOUSE]}


class FuncDbCastFile2(FuncDbCastFileBase, FuncDbCast2):
    pass


class FuncDbCastFile3(FuncDbCastFileBase, FuncDbCast3):
    pass


class FuncDbCastFile4(FuncDbCastFileBase, FuncDbCast4):
    pass


DEFINITIONS_TYPE = [
    *clickhouse_funcs_for_file_dialect(CH_DEFINITIONS_TYPE),
    FuncDbCastFile2(),
    FuncDbCastFile3(),
    FuncDbCastFile4(),
]
