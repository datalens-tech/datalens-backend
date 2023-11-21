from dl_connector_ydb.formula.definitions.conditional_blocks import DEFINITIONS_COND_BLOCKS
from dl_connector_ydb.formula.definitions.functions_aggregation import DEFINITIONS_AGG
from dl_connector_ydb.formula.definitions.functions_datetime import DEFINITIONS_DATETIME
from dl_connector_ydb.formula.definitions.functions_logical import DEFINITIONS_LOGICAL
from dl_connector_ydb.formula.definitions.functions_markup import DEFINITIONS_MARKUP
from dl_connector_ydb.formula.definitions.functions_math import DEFINITIONS_MATH
from dl_connector_ydb.formula.definitions.functions_string import DEFINITIONS_STRING
from dl_connector_ydb.formula.definitions.functions_type import DEFINITIONS_TYPE
from dl_connector_ydb.formula.definitions.operators_binary import DEFINITIONS_BINARY
from dl_connector_ydb.formula.definitions.operators_ternary import DEFINITIONS_TERNARY
from dl_connector_ydb.formula.definitions.operators_unary import DEFINITIONS_UNARY


DEFINITIONS = [
    *DEFINITIONS_COND_BLOCKS,
    *DEFINITIONS_AGG,
    *DEFINITIONS_DATETIME,
    *DEFINITIONS_LOGICAL,
    *DEFINITIONS_MARKUP,
    *DEFINITIONS_MATH,
    *DEFINITIONS_STRING,
    *DEFINITIONS_TYPE,
    *DEFINITIONS_UNARY,
    *DEFINITIONS_BINARY,
    *DEFINITIONS_TERNARY,
]
