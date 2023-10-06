from dl_connector_metrica.formula.definitions.functions_datetime import DEFINITIONS_DATETIME
from dl_connector_metrica.formula.definitions.functions_markup import DEFINITIONS_MARKUP
from dl_connector_metrica.formula.definitions.functions_string import DEFINITIONS_STRING
from dl_connector_metrica.formula.definitions.functions_type import DEFINITIONS_TYPE
from dl_connector_metrica.formula.definitions.operators_binary import DEFINITIONS_BINARY
from dl_connector_metrica.formula.definitions.operators_ternary import DEFINITIONS_TERNARY
from dl_connector_metrica.formula.definitions.operators_unary import DEFINITIONS_UNARY


DEFINITIONS = [
    *DEFINITIONS_DATETIME,
    *DEFINITIONS_MARKUP,
    *DEFINITIONS_STRING,
    *DEFINITIONS_TYPE,
    *DEFINITIONS_UNARY,
    *DEFINITIONS_BINARY,
    *DEFINITIONS_TERNARY,
]
