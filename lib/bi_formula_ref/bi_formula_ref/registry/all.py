from bi_formula_ref.registry.base import FunctionDocCategory
from bi_formula_ref.categories.aggregation import CATEGORY_AGGREGATION
from bi_formula_ref.categories.array import CATEGORY_ARRAY
from bi_formula_ref.categories.date import CATEGORY_DATE
from bi_formula_ref.categories.logical import CATEGORY_LOGICAL
from bi_formula_ref.categories.markup import CATEGORY_MARKUP
from bi_formula_ref.categories.mathematical import CATEGORY_MATHEMATICAL
from bi_formula_ref.categories.operator import CATEGORY_OPERATOR
from bi_formula_ref.categories.string import CATEGORY_STRING
from bi_formula_ref.categories.time_series import CATEGORY_TIME_SERIES
from bi_formula_ref.categories.type_conversion import CATEGORY_TYPE_CONVERSION
from bi_formula_ref.categories.window import CATEGORY_WINDOW


CATEGORIES = {
    category.name: category
    for category in (
        CATEGORY_AGGREGATION,
        CATEGORY_ARRAY,
        CATEGORY_DATE,
        CATEGORY_LOGICAL,
        CATEGORY_MARKUP,
        CATEGORY_MATHEMATICAL,
        CATEGORY_OPERATOR,
        CATEGORY_STRING,
        CATEGORY_TIME_SERIES,
        CATEGORY_TYPE_CONVERSION,
        CATEGORY_WINDOW,
        FunctionDocCategory(name='__internal', description='', keywords='')
    )
}
