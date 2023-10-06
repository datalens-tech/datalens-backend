from dl_connector_bitrix_gds.formula.definitions.functions_datetime import DEFINITIONS_DATETIME
from dl_connector_bitrix_gds.formula.definitions.functions_markup import DEFINITIONS_MARKUP
from dl_connector_bitrix_gds.formula.definitions.functions_string import DEFINITIONS_STRING
from dl_connector_bitrix_gds.formula.definitions.functions_type import DEFINITIONS_TYPE
from dl_connector_bitrix_gds.formula.definitions.operators_binary import DEFINITIONS_BINARY
from dl_connector_bitrix_gds.formula.definitions.operators_ternary import DEFINITIONS_TERNARY


DEFINITIONS = [
    *DEFINITIONS_DATETIME,
    *DEFINITIONS_MARKUP,
    *DEFINITIONS_STRING,
    *DEFINITIONS_TYPE,
    *DEFINITIONS_BINARY,
    *DEFINITIONS_TERNARY,
]
