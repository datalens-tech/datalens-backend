from dl_connector_starrocks.formula.definitions.conditional_blocks import DEFINITIONS_COND_BLOCKS
from dl_connector_starrocks.formula.definitions.functions_aggregation import DEFINITIONS_AGG
from dl_connector_starrocks.formula.definitions.functions_logical import DEFINITIONS_LOGICAL
from dl_connector_starrocks.formula.definitions.functions_math import DEFINITIONS_MATH
from dl_connector_starrocks.formula.definitions.functions_type import DEFINITIONS_TYPE
from dl_connector_starrocks.formula.definitions.operators_binary import DEFINITIONS_BINARY
from dl_connector_starrocks.formula.definitions.operators_ternary import DEFINITIONS_TERNARY
from dl_connector_starrocks.formula.definitions.operators_unary import DEFINITIONS_UNARY


DEFINITIONS = [
    *DEFINITIONS_BINARY,
    *DEFINITIONS_UNARY,
    *DEFINITIONS_TERNARY,
    *DEFINITIONS_AGG,
    *DEFINITIONS_TYPE,
    *DEFINITIONS_LOGICAL,
    *DEFINITIONS_MATH,
    *DEFINITIONS_COND_BLOCKS,
]
