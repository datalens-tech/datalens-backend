from dl_formula.core.datatype import DataType
from dl_formula_ref.categories.array import CATEGORY_ARRAY
from dl_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import (
    DataExample,
    SimpleExample,
)
from dl_formula_ref.registry.impl_selector import ArgAwareImplementationSelector
from dl_formula_ref.registry.naming import CategoryPostfixNamingProvider
from dl_formula_ref.registry.note import Note

_ = get_gettext()

_EXAMPLE_SOURCE_UNNEST = ExampleSource(
    columns=[
        ("City", DataType.STRING),
        ("Category", DataType.ARRAY_STR),
    ],
    data=[
        ["Moscow", ["Office Supplies", "Furniture"]],
        ["London", ["Office Supplies"]],
    ],
)

_EXAMPLE_SOURCE_ARRAY_TYPE_CONVERSION = ExampleSource(
    columns=[
        ("string_array", DataType.ARRAY_STR),
        ("float_array", DataType.ARRAY_FLOAT),
        ("int_array", DataType.ARRAY_INT),
    ],
    data=[
        [["100", "300", "200"], [12.3, 0.43, 42], [12, 0, 42]],
        [["100", "300", None, None], [0, None, 12.0], [-3, None, 0]],
        [["150", None, "192"], [3.2, 2.3, 3.5], [132, 637, None]],
    ],
)

_EXAMPLE_SOURCE_REPLACE = ExampleSource(
    columns=[
        ("array", DataType.ARRAY_INT),
    ],
    data=[
        [[100, 300, 200, 100, 300]],
        [[100, 300, None, None]],
        [[150, None, 130, 192]],
    ],
)

_EXAMPLE_SOURCE_CONTAINS = ExampleSource(
    columns=[
        ("array", DataType.ARRAY_INT),
        ("value", DataType.INTEGER),
    ],
    data=[
        [[1, 2, 3], 2],
        [[1, 2, 3], 4],
    ],
)

_EXAMPLE_SOURCE_CONTAINS_ALL = ExampleSource(
    columns=[
        ("array1", DataType.ARRAY_INT),
        ("array2", DataType.ARRAY_INT),
    ],
    data=[
        [[1, 2, 3], [1, 2]],
        [[1, 2, 3], [1, 4]],
    ],
)

_EXAMPLE_SOURCE_CONTAINS_ANY = ExampleSource(
    columns=[
        ("array1", DataType.ARRAY_INT),
        ("array2", DataType.ARRAY_INT),
    ],
    data=[
        [[1, 2, 3], [1, 5]],
        [[1, 2, 3], [4, 5]],
    ],
)

_EXAMPLE_SOURCE_CONTAINS_SUBSEQUENCE = ExampleSource(
    columns=[
        ("array1", DataType.ARRAY_INT),
        ("array2", DataType.ARRAY_INT),
    ],
    data=[
        [[1, 2, 3], [1, 2]],
        [[1, 2, 3], [2, 1]],
        [[1, 2, 3], [1, 3]],
    ],
)

_EXAMPLE_SOURCE_ARRAY_AGGREGATION_FUNCTIONS = ExampleSource(
    columns=[
        ("float_array", DataType.ARRAY_FLOAT),
        ("int_array", DataType.ARRAY_INT),
    ],
    data=[
        [[14.3, 0.42, 15], [21, 12, 0]],
        [[0, -3, 12.0], [-4, 12, 0]],
        [[3.2, 2.3, 3.2], [5, 7, 9]],
    ],
)

FUNCTION_ARRAY = FunctionDocRegistryItem(
    name="array",
    category=CATEGORY_ARRAY,
    description=_("Returns an array containing the passed values."),
    notes=[
        Note(_("All passed values must be of the same type or `NULL`. At least one value must be non-`NULL`.")),
    ],
    examples=[
        SimpleExample("ARRAY(1, 2, NULL, 3)"),
        SimpleExample("ARRAY('a', 'b', NULL, 'c')"),
        SimpleExample("ARRAY(0, 2.3, NULL, 0.18)"),
    ],
)

FUNCTION_UNNEST = FunctionDocRegistryItem(
    name="unnest",
    category=CATEGORY_ARRAY,
    description=_("Expands the {arg:0} array expression to a set of rows."),
    notes=[
        Note(
            _(
                "{dialects: POSTGRESQL} doesn't allow filtering fields containing the UNNEST "
                "function. If the data source is {dialects: POSTGRESQL}, do not use such "
                "fields in selectors."
            )
        ),
    ],
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_UNNEST,
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("unnest_category", "UNNEST([Category])")],
            ),
        ),
    ],
)

FUNCTION_GET_ITEM = FunctionDocRegistryItem(
    name="get_item",
    category=CATEGORY_ARRAY,
    description=_(
        "Returns the element with the index {arg:1} from the array {arg:0}. "
        "Index must be any integer. Indexes in an array begin with one."
    ),
    examples=[
        SimpleExample("GET_ITEM([array_field], 2)"),
    ],
)

FUNCTION_ARR_STR = FunctionDocRegistryItem(
    name="arr_str",
    category=CATEGORY_ARRAY,
    description=_(
        "Concatenates elements of the array {arg:0} using {arg:1} as a delimiter (comma by default) "
        "and {arg:2} as a `NULL` string (`NULL` items are skipped by default).\n"
        "\n"
        "See also {ref:STR}"
    ),
    examples=[
        SimpleExample("ARR_STR(ARRAY(1, 2, NULL, 4)) = 1,2,4"),
        SimpleExample("ARR_STR(ARRAY(1, 2, NULL, 4), ';') = 1;2;4"),
        SimpleExample("ARR_STR(ARRAY(1, 2, NULL, 4), ';', '*') = 1;2;*;4"),
        SimpleExample("ARR_STR(ARRAY('a', 'b', '', NULL), ';', '*') = a;b;;*"),
    ],
)

FUNCTION_COUNT_ITEM = FunctionDocRegistryItem(
    name="count_item",
    category=CATEGORY_ARRAY,
    description=_(
        "Returns the number of elements in the array {arg:0} equal to {arg:1}. "
        "The type of {arg:1} must match the type of the {arg:0} elements."
    ),
    examples=[
        SimpleExample("COUNT_ITEM(ARRAY(1, 2, 2, 3), 2) = 2"),
        SimpleExample("COUNT_ITEM(ARRAY(1, 2, 2, 3), 4) = 0"),
        SimpleExample("COUNT_ITEM(ARRAY(1, NULL, 2, NULL), NULL) = 2"),
    ],
)

FUNCTION_CONTAINS = FunctionDocRegistryItem(
    name="contains",
    category=CATEGORY_ARRAY,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {DataType.ARRAY_INT, DataType.ARRAY_FLOAT, DataType.ARRAY_STR},
        },
    ),
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="contains_array",
    ),
    description=_("Returns `TRUE` if {arg:0} contains {arg:1}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_CONTAINS,
                formula_fields=[
                    ("array", "[array]"),
                    ("value", "[value]"),
                    ("contains", "CONTAINS([array], [value])"),
                ],
            ),
        ),
    ],
)

FUNCTION_CONTAINS_ALL = FunctionDocRegistryItem(
    name="contains_all",
    category=CATEGORY_ARRAY,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {DataType.ARRAY_INT, DataType.ARRAY_FLOAT, DataType.ARRAY_STR},
        },
    ),
    description=_("Returns `TRUE` if {arg:0} contains all elements of {arg:1}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_CONTAINS_ALL,
                formula_fields=[
                    ("array1", "[array1]"),
                    ("array2", "[array2]"),
                    ("contains_all", "CONTAINS_ALL([array1], [array2])"),
                ],
            ),
        ),
    ],
)

FUNCTION_CONTAINS_ANY = FunctionDocRegistryItem(
    name="contains_any",
    category=CATEGORY_ARRAY,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {DataType.ARRAY_INT, DataType.ARRAY_FLOAT, DataType.ARRAY_STR},
        },
    ),
    description=_("Returns `TRUE` if {arg:0} contains any elements of {arg:1}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_CONTAINS_ANY,
                formula_fields=[
                    ("array1", "[array1]"),
                    ("array2", "[array2]"),
                    ("contains_any", "CONTAINS_ANY([array1], [array2])"),
                ],
            ),
        ),
    ],
)

FUNCTION_CONTAINS_SUBSEQUENCE = FunctionDocRegistryItem(
    name="contains_subsequence",
    category=CATEGORY_ARRAY,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {DataType.ARRAY_INT, DataType.ARRAY_FLOAT, DataType.ARRAY_STR},
        },
    ),
    description=_(
        "Returns `TRUE` if {arg:1} is a continuous subsequence of {arg:0}. "
        "In other words, returns `TRUE` if and only if `array1 = prefix + array2 + suffix`."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_CONTAINS_SUBSEQUENCE,
                formula_fields=[
                    ("array1", "[array1]"),
                    ("array2", "[array2]"),
                    ("contains_subsequence", "CONTAINS_SUBSEQUENCE([array1], [array2])"),
                ],
            ),
        ),
    ],
)

FUNCTION_STARTSWITH = FunctionDocRegistryItem(
    name="startswith",
    category=CATEGORY_ARRAY,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {DataType.ARRAY_INT, DataType.ARRAY_FLOAT, DataType.ARRAY_STR},
        },
    ),
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="startswith_array",
    ),
    description=_("Returns `TRUE` if {arg:0} starts with {arg:1}."),
    examples=[
        SimpleExample("STARTSWITH(ARRAY(1, 2, 3), ARRAY(1, 2)) = TRUE"),
        SimpleExample("STARTSWITH(ARRAY(1, 2, 3), ARRAY(2, 3)) = FALSE"),
    ],
)

FUNCTION_SLICE = FunctionDocRegistryItem(
    name="slice",
    category=CATEGORY_ARRAY,
    description=_(
        "Returns the part of array {arg:0} of length {arg:2} starting from index {arg:1}. Indexes in an array begin with one."
    ),
    examples=[
        SimpleExample("SLICE(ARRAY(1, 2, 3, 4, 5), 3, 2) = [3, 4]"),
        SimpleExample("SLICE(ARRAY(1, 2, 3, 4, 5), 3, 1) = [3]"),
    ],
)

FUNCTION_ARRAY_INT = FunctionDocRegistryItem(
    name="cast_arr_int",
    category=CATEGORY_ARRAY,
    description=_("Converts {arg:0} to an array of integers. The conversion rules are the same as for {ref:INT}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_ARRAY_TYPE_CONVERSION,
                formula_fields=[
                    ("string_array", "[string_array]"),
                    ("float_array", "[float_array]"),
                    ("Array of integers from string", "CAST_ARR_INT([string_array])"),
                    ("Array of integers from float", "CAST_ARR_INT([float_array])"),
                ],
            ),
        ),
    ],
)

FUNCTION_ARRAY_STR = FunctionDocRegistryItem(
    name="cast_arr_str",
    category=CATEGORY_ARRAY,
    description=_("Converts {arg:0} to an array of strings. The conversion rules are the same as for {ref:STR}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_ARRAY_TYPE_CONVERSION,
                formula_fields=[
                    ("int_array", "[int_array]"),
                    ("float_array", "[float_array]"),
                    ("Array of string from integer", "CAST_ARR_STR([int_array])"),
                    ("Array of integers from float", "CAST_ARR_STR([float_array])"),
                ],
            ),
        ),
    ],
)

FUNCTION_ARRAY_FLOAT = FunctionDocRegistryItem(
    name="cast_arr_float",
    category=CATEGORY_ARRAY,
    description=_(
        "Converts {arg:0} to an array of fractional numbers. The conversion rules are the same as for {ref:FLOAT}."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_ARRAY_TYPE_CONVERSION,
                formula_fields=[
                    ("string_array", "[string_array]"),
                    ("int_array", "[int_array]"),
                    ("Array of float from string", "CAST_ARR_FLOAT([string_array])"),
                    ("Array of floats from integer", "CAST_ARR_FLOAT([int_array])"),
                ],
            ),
        ),
    ],
)

FUNCTION_REPLACE = FunctionDocRegistryItem(
    name="replace",
    category=CATEGORY_ARRAY,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {DataType.ARRAY_INT, DataType.ARRAY_FLOAT, DataType.ARRAY_STR},
        },
    ),
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="replace_array",
    ),
    description=_("Replaces each {arg:0} element equal to {arg:1} with {arg:2}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_REPLACE,
                formula_fields=[
                    ("array", "[array]"),
                    ("replace", "REPLACE([array], 300, -1)"),
                    ("No NULLs", "REPLACE([array], NULL, 0)"),
                ],
            ),
        ),
    ],
)

ARRAY_AGGREGATION_FUNCTIONS_NOTE = Note(
    _(
        "This function cannot work with arrays with `Nullable` items. To remove `NULL` items from the array,"
        " use {ref:ARR_REMOVE} or {ref:array/REPLACE}."
    )
)


def _make_array_aggregation_function_example(func: str) -> list[DataExample]:
    func = func.upper()
    examples = [
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_ARRAY_AGGREGATION_FUNCTIONS,
                formula_fields=[
                    ("[int_array]", "[int_array]"),
                    ("[float_array]", "[float_array]"),
                    (f"{func}([int_array])", f"{func}(REPLACE([int_array], NULL, 0))"),
                    (f"{func}([float_array])", f"{func}(REPLACE([float_array], NULL, 0))"),
                ],
                override_formula_fields=[
                    ("[int_array]", "[int_array]"),
                    ("[float_array]", "[float_array]"),
                    (f"{func}([int_array])", f"{func}([int_array])"),
                    (f"{func}([float_array])", f"{func}([float_array])"),
                ],
            ),
        ),
    ]
    return examples


FUNCTION_ARR_MIN = FunctionDocRegistryItem(
    name="arr_min",
    category=CATEGORY_ARRAY,
    description=_("Returns the least of elements in the {arg:0}."),
    notes=[ARRAY_AGGREGATION_FUNCTIONS_NOTE],
    examples=[
        *_make_array_aggregation_function_example("arr_min"),
    ],
)

FUNCTION_ARR_MAX = FunctionDocRegistryItem(
    name="arr_max",
    category=CATEGORY_ARRAY,
    description=_("Returns the greatest of elements in the {arg:0}."),
    notes=[ARRAY_AGGREGATION_FUNCTIONS_NOTE],
    examples=[
        *_make_array_aggregation_function_example("arr_max"),
    ],
)

FUNCTION_ARR_SUM = FunctionDocRegistryItem(
    name="arr_sum",
    category=CATEGORY_ARRAY,
    description=_("Returns the sum of elements in the {arg:0}."),
    notes=[ARRAY_AGGREGATION_FUNCTIONS_NOTE],
    examples=[
        *_make_array_aggregation_function_example("arr_sum"),
    ],
)

FUNCTION_ARR_AVG = FunctionDocRegistryItem(
    name="arr_avg",
    category=CATEGORY_ARRAY,
    description=_("Returns the average of elements in the {arg:0}."),
    notes=[ARRAY_AGGREGATION_FUNCTIONS_NOTE],
    examples=[
        *_make_array_aggregation_function_example("arr_avg"),
    ],
)

FUNCTION_ARR_PRODUCT = FunctionDocRegistryItem(
    name="arr_product",
    category=CATEGORY_ARRAY,
    description=_("Returns the product of elements in the {arg:0}."),
    notes=[ARRAY_AGGREGATION_FUNCTIONS_NOTE],
    examples=[
        *_make_array_aggregation_function_example("arr_product"),
    ],
)

FUNCTION_ARR_REMOVE = FunctionDocRegistryItem(
    name="arr_remove",
    category=CATEGORY_ARRAY,
    description=_("Removes all {arg:0} elements equal to {arg:1}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_EXAMPLE_SOURCE_REPLACE,
                formula_fields=[
                    ("array", "[array]"),
                    ("No NULLs", "ARR_REMOVE([array], NULL)"),
                ],
            ),
        ),
    ],
)

FUNCTIONS_ARRAY = [
    FUNCTION_ARRAY,
    FUNCTION_UNNEST,
    FUNCTION_GET_ITEM,
    FUNCTION_ARR_STR,
    FUNCTION_COUNT_ITEM,
    FUNCTION_CONTAINS,
    FUNCTION_CONTAINS_ALL,
    FUNCTION_CONTAINS_ANY,
    FUNCTION_CONTAINS_SUBSEQUENCE,
    FUNCTION_STARTSWITH,
    FUNCTION_SLICE,
    FUNCTION_ARRAY_INT,
    FUNCTION_ARRAY_STR,
    FUNCTION_ARRAY_FLOAT,
    FUNCTION_REPLACE,
    FUNCTION_ARR_MIN,
    FUNCTION_ARR_MAX,
    FUNCTION_ARR_SUM,
    FUNCTION_ARR_AVG,
    FUNCTION_ARR_PRODUCT,
    FUNCTION_ARR_REMOVE,
]
