from __future__ import annotations

from dl_formula_ref.functions.aggregation import FUNCTIONS_AGGREGATION
from dl_formula_ref.functions.array import FUNCTIONS_ARRAY
from dl_formula_ref.functions.date import FUNCTIONS_DATE
from dl_formula_ref.functions.logical import FUNCTIONS_LOGICAL
from dl_formula_ref.functions.markup import FUNCTIONS_MARKUP
from dl_formula_ref.functions.mathematical import FUNCTIONS_MATHEMATICAL
from dl_formula_ref.functions.operator import FUNCTIONS_OPERATOR
from dl_formula_ref.functions.string import FUNCTIONS_STRING
from dl_formula_ref.functions.time_series import FUNCTIONS_TIME_SERIES
from dl_formula_ref.functions.type_conversion import FUNCTIONS_TYPE_CONVERSION
from dl_formula_ref.functions.window import FUNCTIONS_WINDOW
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.registry import FUNC_REFERENCE_REGISTRY


EXPLICITLY_DEFINED_FUNCTIONS: list[FunctionDocRegistryItem] = [
    *FUNCTIONS_AGGREGATION,
    *FUNCTIONS_ARRAY,
    *FUNCTIONS_DATE,
    *FUNCTIONS_LOGICAL,
    *FUNCTIONS_MARKUP,
    *FUNCTIONS_MATHEMATICAL,
    *FUNCTIONS_OPERATOR,
    *FUNCTIONS_STRING,
    *FUNCTIONS_TIME_SERIES,
    *FUNCTIONS_TYPE_CONVERSION,
    *FUNCTIONS_WINDOW,
]


def populate_registry_from_definitions() -> None:
    for expl_item in EXPLICITLY_DEFINED_FUNCTIONS:
        FUNC_REFERENCE_REGISTRY.add_item(expl_item)
