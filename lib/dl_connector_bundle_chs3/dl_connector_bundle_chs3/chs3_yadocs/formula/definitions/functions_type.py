from dl_formula.definitions.functions_type import (
    FuncDbCast2,
    FuncDbCast3,
    FuncDbCast4,
    FuncDbCastBase,
)

from dl_connector_bundle_chs3.chs3_yadocs.formula.constants import YaDocsFileS3Dialect as D
from dl_connector_bundle_chs3.chs3_yadocs.formula.utils import clickhouse_funcs_for_yadocs_dialect
from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_connector_clickhouse.formula.definitions.functions_type import DEFINITIONS_TYPE as CH_DEFINITIONS_TYPE
from dl_connector_clickhouse.formula.definitions.functions_type import FuncDbCastClickHouseBase


CH_DEFINITIONS_TYPE = [func for func in CH_DEFINITIONS_TYPE if not issubclass(func.__class__, FuncDbCastBase)]


class FuncDbCastYaDocsBase(FuncDbCastClickHouseBase):
    WHITELISTS = {D.YADOCS: FuncDbCastClickHouseBase.WHITELISTS[ClickHouseDialect.CLICKHOUSE]}


class FuncDbCastYaDocs2(FuncDbCastYaDocsBase, FuncDbCast2):
    pass


class FuncDbCastYaDocs3(FuncDbCastYaDocsBase, FuncDbCast3):
    pass


class FuncDbCastYaDocs4(FuncDbCastYaDocsBase, FuncDbCast4):
    pass


DEFINITIONS_TYPE = [
    *clickhouse_funcs_for_yadocs_dialect(CH_DEFINITIONS_TYPE),
    FuncDbCastYaDocs2(),
    FuncDbCastYaDocs3(),
    FuncDbCastYaDocs4(),
]
