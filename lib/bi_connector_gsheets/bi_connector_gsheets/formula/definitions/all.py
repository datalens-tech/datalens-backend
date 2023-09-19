from bi_connector_gsheets.formula.definitions.functions_datetime import DEFINITIONS_DATETIME
from bi_connector_gsheets.formula.definitions.functions_markup import DEFINITIONS_MARKUP
from bi_connector_gsheets.formula.definitions.functions_string import DEFINITIONS_STRING
from bi_connector_gsheets.formula.definitions.functions_type import DEFINITIONS_TYPE
from bi_connector_gsheets.formula.definitions.operators_binary import DEFINITIONS_BINARY


DEFINITIONS = [
    *DEFINITIONS_DATETIME,
    *DEFINITIONS_MARKUP,
    *DEFINITIONS_STRING,
    *DEFINITIONS_TYPE,
    *DEFINITIONS_BINARY,
]
