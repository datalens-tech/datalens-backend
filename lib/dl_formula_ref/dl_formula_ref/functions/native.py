from dl_formula_ref.categories.native import CATEGORY_NATIVE
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedTextResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample
from dl_formula_ref.registry.note import (
    Note,
    NoteLevel,
)


_ = get_gettext()


NATIVE_FUNCTION_NOTE = Note(
    Translatable(
        "This function allows you to call database-specific functions that are not available "
        "as standard functions in DataLens. The availability and behavior of native functions "
        "depends on your database type and version."
    ),
    level=NoteLevel.warning,
)

DESCRIPTION_TEMPLATE = _(
    "Calls a native database {text:function_type} by name. Native function should return {text:return_type}. "
    "{text:execution_type} Parameters are passed in the same order as written in the formula."
    "\n\n"
    "The first argument {arg:0} must be a constant string with the name of the "
    "database function to call. All subsequent arguments are passed to the "
    "native function and can be of any type, including types that are not currently supported by DataLens."
    "\n\n"
    "The function name must contain only alphanumeric characters, underscore and colon characters."
)

SIMPLE_FUNCTION_BASE_RESOURCES = {
    "function_type": AliasedTextResource(body=_("function")),
    "execution_type": AliasedTextResource(
        body=_("The function is executed for every row in the dataset (non-aggregated).")
    ),
}


FUNCTION_DB_CALL_INT = FunctionDocRegistryItem(
    name="db_call_int",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    notes=[NATIVE_FUNCTION_NOTE],
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("an integer result")),
            **SIMPLE_FUNCTION_BASE_RESOURCES,
        }
    ),
    examples=[
        SimpleExample(
            'DB_CALL_INT("positionCaseInsensitive", "Hello", "l") = 3 '
            '-- ClickHouse: find first occurrence of "l" in "Hello" ignoring case'
        ),
        SimpleExample(
            'DB_CALL_INT("JSON_VALUE", [json_field], "$.age") -- PostgreSQL: extract age attribute from JSON'
        ),
    ],
)

FUNCTION_DB_CALL_FLOAT = FunctionDocRegistryItem(
    name="db_call_float",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    notes=[NATIVE_FUNCTION_NOTE],
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("a float result")),
            **SIMPLE_FUNCTION_BASE_RESOURCES,
        }
    ),
    examples=[
        SimpleExample('DB_CALL_FLOAT("sign", -5.0) = -1.0 -- ClickHouse: sign of -5.0 is -1.0'),
        SimpleExample('DB_CALL_FLOAT("sign", 5.0) = 1.0 -- ClickHouse: sign of 5.0 is 1.0'),
        SimpleExample('DB_CALL_FLOAT("log10", 100.0) = 2.0 -- ClickHouse: log10 of 100.0 is 2.0'),
    ],
)

FUNCTION_DB_CALL_STRING = FunctionDocRegistryItem(
    name="db_call_string",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    notes=[NATIVE_FUNCTION_NOTE],
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("a string result")),
            **SIMPLE_FUNCTION_BASE_RESOURCES,
        }
    ),
    examples=[
        SimpleExample(
            'DB_CALL_STRING("dictGetStringOrDefault", "categories", "category_name", [category_id], "other") '
            "-- ClickHouse: read from dictionary with default value"
        ),
        SimpleExample(
            'DB_CALL_STRING("JSONExtractString", [json_field], "category", "last_order_date") '
            "-- ClickHouse: extract nested JSON attribute"
        ),
        SimpleExample(
            'DB_CALL_STRING("hex", DB_CALL_STRING("SHA1", [salt_param] + [some_id])) '
            "-- ClickHouse: calculate hash with salt"
        ),
    ],
)

FUNCTION_DB_CALL_BOOL = FunctionDocRegistryItem(
    name="db_call_bool",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    notes=[NATIVE_FUNCTION_NOTE],
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("a boolean result")),
            **SIMPLE_FUNCTION_BASE_RESOURCES,
        }
    ),
    examples=[
        SimpleExample('DB_CALL_BOOL("isFinite", 5) = TRUE -- ClickHouse: check if 5 is a finite number'),
        SimpleExample('DB_CALL_BOOL("isInfinite", 5) = FALSE -- ClickHouse: check if 5 is an infinite number'),
        SimpleExample(
            'DB_CALL_BOOL("bitTest", [int_field], 4) '
            "-- ClickHouse: check if the 4th bit equals 1, useful when multiple values are encoded in one field"
        ),
    ],
)

FUNCTION_DB_CALL_ARRAY_INT = FunctionDocRegistryItem(
    name="db_call_array_int",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    notes=[NATIVE_FUNCTION_NOTE],
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("an array of integers")),
            **SIMPLE_FUNCTION_BASE_RESOURCES,
        }
    ),
    examples=[
        SimpleExample(
            'DB_CALL_ARRAY_INT("range", 5) = ARRAY(0, 1, 2, 3, 4) '
            "-- ClickHouse: generate array of integers from 0 to 4"
        ),
        SimpleExample(
            'DB_CALL_ARRAY_INT("arrayCompact", [int_arr_field]) '
            "-- ClickHouse: remove duplicate consecutive values from array of integers"
        ),
    ],
)

FUNCTION_DB_CALL_ARRAY_FLOAT = FunctionDocRegistryItem(
    name="db_call_array_float",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("an array of floats")),
            **SIMPLE_FUNCTION_BASE_RESOURCES,
        }
    ),
    notes=[NATIVE_FUNCTION_NOTE],
    examples=[
        SimpleExample(
            'DB_CALL_ARRAY_FLOAT("arrayConcat", ARRAY(1.0, 2.0), ARRAY(3.0)) = ARRAY(1.0, 2.0, 3.0) '
            "-- ClickHouse: concatenate arrays of floats"
        ),
        SimpleExample(
            'DB_CALL_ARRAY_FLOAT("arrayCompact", [float_arr_field]) '
            "-- ClickHouse: remove duplicate consecutive values from array of floats"
        ),
    ],
)

FUNCTION_DB_CALL_ARRAY_STRING = FunctionDocRegistryItem(
    name="db_call_array_string",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("an array of strings")),
            **SIMPLE_FUNCTION_BASE_RESOURCES,
        }
    ),
    notes=[NATIVE_FUNCTION_NOTE],
    examples=[
        SimpleExample(
            'DB_CALL_ARRAY_STRING("splitByChar", ",", "a,b,c") = ARRAY("a", "b", "c") '
            "-- ClickHouse: split string by comma"
        ),
        SimpleExample(
            'DB_CALL_ARRAY_STRING("arrayCompact", [string_arr_field]) '
            "-- ClickHouse: remove duplicate consecutive values from array of strings"
        ),
    ],
)


AGGREGATE_FUNCTION_BASE_RESOURCES = {
    "function_type": AliasedTextResource(body=_("aggregate function")),
    "execution_type": AliasedTextResource(body=_("The function is executed as an aggregation across grouped rows.")),
}

FUNCTION_DB_CALL_AGG_INT = FunctionDocRegistryItem(
    name="db_call_agg_int",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("an integer result")),
            **AGGREGATE_FUNCTION_BASE_RESOURCES,
        }
    ),
    notes=[NATIVE_FUNCTION_NOTE],
    examples=[
        SimpleExample(
            'DB_CALL_AGG_INT("uniqMerge", [uniqStateField]) '
            "-- ClickHouse: merge uniqState aggregations to get unique count"
        ),
    ],
)

FUNCTION_DB_CALL_AGG_FLOAT = FunctionDocRegistryItem(
    name="db_call_agg_float",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("a float result")),
            **AGGREGATE_FUNCTION_BASE_RESOURCES,
        }
    ),
    notes=[NATIVE_FUNCTION_NOTE],
    examples=[
        SimpleExample(
            'DB_CALL_AGG_FLOAT("avgWeighted", [amount], [weight_field]) '
            "-- ClickHouse: calculate weighted average of amount by weight"
        ),
        SimpleExample(
            'DB_CALL_AGG_FLOAT("corr", [x], [y]) -- ClickHouse: calculate correlation coefficient between x and y'
        ),
    ],
)

FUNCTION_DB_CALL_AGG_STRING = FunctionDocRegistryItem(
    name="db_call_agg_string",
    category=CATEGORY_NATIVE,
    description=DESCRIPTION_TEMPLATE,
    resources=SimpleAliasedResourceRegistry(
        resources={
            "return_type": AliasedTextResource(body=_("a string result")),
            **AGGREGATE_FUNCTION_BASE_RESOURCES,
        }
    ),
    notes=[NATIVE_FUNCTION_NOTE],
    examples=[
        SimpleExample(
            'DB_CALL_AGG_STRING("anyHeavy", [str_field]) '
            "-- ClickHouse: select a frequently occurring value (more intelligent than random any)"
        ),
    ],
)

FUNCTIONS_NATIVE = [
    FUNCTION_DB_CALL_INT,
    FUNCTION_DB_CALL_FLOAT,
    FUNCTION_DB_CALL_STRING,
    FUNCTION_DB_CALL_BOOL,
    FUNCTION_DB_CALL_ARRAY_INT,
    FUNCTION_DB_CALL_ARRAY_FLOAT,
    FUNCTION_DB_CALL_ARRAY_STRING,
    FUNCTION_DB_CALL_AGG_INT,
    FUNCTION_DB_CALL_AGG_FLOAT,
    FUNCTION_DB_CALL_AGG_STRING,
]
