from __future__ import annotations

from typing import List

from bi_formula_ref.registry.base import FunctionDocRegistryItem
from bi_formula_ref.registry.registry import FUNC_REFERENCE_REGISTRY
from bi_formula_ref.functions.aggregation import FUNCTIONS_AGGREGATION
from bi_formula_ref.functions.array import FUNCTIONS_ARRAY
from bi_formula_ref.functions.date import FUNCTIONS_DATE
from bi_formula_ref.functions.logical import FUNCTIONS_LOGICAL
from bi_formula_ref.functions.markup import FUNCTIONS_MARKUP
from bi_formula_ref.functions.mathematical import FUNCTIONS_MATHEMATICAL
from bi_formula_ref.functions.operator import FUNCTIONS_OPERATOR
from bi_formula_ref.functions.string import FUNCTIONS_STRING
from bi_formula_ref.functions.time_series import FUNCTIONS_TIME_SERIES
from bi_formula_ref.functions.type_conversion import FUNCTIONS_TYPE_CONVERSION
from bi_formula_ref.functions.window import FUNCTIONS_WINDOW


EXPLICITLY_DEFINED_FUNCTIONS: List[FunctionDocRegistryItem] = [
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
