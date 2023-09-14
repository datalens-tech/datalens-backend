from bi_connector_postgresql.formula.definitions.conditional_blocks import DEFINITIONS_COND_BLOCKS
from bi_connector_postgresql.formula.definitions.functions_aggregation import DEFINITIONS_AGG
from bi_connector_postgresql.formula.definitions.functions_array import DEFINITIONS_ARRAY
from bi_connector_postgresql.formula.definitions.functions_datetime import DEFINITIONS_DATETIME
from bi_connector_postgresql.formula.definitions.functions_logical import DEFINITIONS_LOGICAL
from bi_connector_postgresql.formula.definitions.functions_markup import DEFINITIONS_MARKUP
from bi_connector_postgresql.formula.definitions.functions_math import DEFINITIONS_MATH
from bi_connector_postgresql.formula.definitions.functions_special import DEFINITIONS_SPECIAL
from bi_connector_postgresql.formula.definitions.functions_string import DEFINITIONS_STRING
from bi_connector_postgresql.formula.definitions.functions_type import DEFINITIONS_TYPE
from bi_connector_postgresql.formula.definitions.functions_window import DEFINITIONS_WINDOW
from bi_connector_postgresql.formula.definitions.operators_binary import DEFINITIONS_BINARY
from bi_connector_postgresql.formula.definitions.operators_ternary import DEFINITIONS_TERNARY
from bi_connector_postgresql.formula.definitions.operators_unary import DEFINITIONS_UNARY

DEFINITIONS = [
    *DEFINITIONS_COND_BLOCKS,
    *DEFINITIONS_AGG,
    *DEFINITIONS_DATETIME,
    *DEFINITIONS_LOGICAL,
    *DEFINITIONS_MARKUP,
    *DEFINITIONS_MATH,
    *DEFINITIONS_SPECIAL,
    *DEFINITIONS_STRING,
    *DEFINITIONS_TYPE,
    *DEFINITIONS_WINDOW,
    *DEFINITIONS_UNARY,
    *DEFINITIONS_BINARY,
    *DEFINITIONS_TERNARY,
    *DEFINITIONS_ARRAY,
]
