from dl_formula.core.datatype import DataType
from dl_formula_ref.categories.aggregation import CATEGORY_AGGREGATION
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


_ = get_gettext()

_SOURCE_INT_FLOAT_1 = ExampleSource(
    columns=[
        ("City", DataType.STRING),
        ("Category", DataType.STRING),
        ("Orders", DataType.INTEGER),
        ("Profit", DataType.FLOAT),
    ],
    data=[
        ["London", "Office Supplies", 8, 120.10],
        ["London", "Furniture", 1, 750.0],
        ["Moscow", "Furniture", 2, 1250.50],
        ["Moscow", "Office Supplies", 4, 85.34],
        ["San Francisco", "Office Supplies", 23, 723.0],
        ["San Francisco", "Technology", 12, 1542.0],
        ["Detroit", "Furniture", 5, 6205.87],
        ["Detroit", "Technology", 9, 2901.0],
    ],
)

_SOURCE_TEMP = ExampleSource(
    columns=[
        ("Month", DataType.STRING),
        ("Temperature", DataType.FLOAT),
    ],
    data=[
        ["January", -8.0],
        ["February", -4.0],
        ["March", -1.0],
        ["April", 7.0],
        ["May", 14.0],
        ["June", 18.0],
        ["July", 22.0],
        ["August", 19.0],
        ["September", 13.0],
        ["October", 5.0],
        ["November", 1.0],
        ["December", -4.0],
    ],
)


FUNCTION_SUM = FunctionDocRegistryItem(
    name="sum",
    category=CATEGORY_AGGREGATION,
    description=_("Returns the sum of all expression values. Applicable to numeric data types only."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_INT_FLOAT_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Sum Orders", "SUM([Orders])"), ("Sum Profit", "SUM([Profit])")],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTION_SUM_IF = FunctionDocRegistryItem(
    name="sum_if",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the sum of all the expression values that meet the {arg:1} condition. "
        "Applicable to numeric data types only."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_INT_FLOAT_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[
                    ("City", "[City]"),
                    ("Sum Orders", "SUM_IF([Orders], [Orders] >= 5)"),
                    ("Sum Profit", "SUM_IF([Profit], [Profit] >= 500)"),
                ],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTION_AVG = FunctionDocRegistryItem(
    name="avg",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the average of all values. Applicable to numeric data types as well as {type:DATE|DATETIME}."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_INT_FLOAT_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Avg Orders", "AVG([Orders])"), ("Avg Profit", "AVG([Profit])")],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTION_AVG_IF = FunctionDocRegistryItem(
    name="avg_if",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the average of all values that meet the {arg:1} condition. "
        "If the values don't exist, it returns `NULL`. Applicable to numeric data types only."
    ),
    examples=[
        SimpleExample("AVG_IF([Profit], [Profit] > 5)"),
    ],
)

FUNCTION_MAX = FunctionDocRegistryItem(
    name="max",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the maximum value.\n\n"
        "If {arg:0}:\n"
        "- number — Returns the largest number.\n"
        "- date — Returns the latest date.\n"
        "- string — Returns the last value in the alphabetic order.\n"
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_INT_FLOAT_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Max Orders", "MAX([Orders])"), ("Max Profit", "MAX([Profit])")],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTION_MIN = FunctionDocRegistryItem(
    name="min",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the minimum value.\n\n"
        "If {arg:0}:\n"
        "- number — Returns the smallest number.\n"
        "- date — Returns the earliest date.\n"
        "- string — Returns the first value in the alphabetic order.\n"
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_INT_FLOAT_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Min Orders", "MIN([Orders])"), ("Min Profit", "MIN([Profit])")],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTION_COUNT = FunctionDocRegistryItem(
    name="count",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the number of items in the group.\n"
        "\n"
        "Can be used with constants, such as `COUNT(1)` or `COUNT()`. "
        "If the chart does not use other measures and dimensions, the result of such "
        "an expression will always be `1`. This is because the function does not "
        "include any fields, so {{ datalens-short-name }} accesses no source tables in the query."
    ),
    examples=[
        SimpleExample("COUNT()"),
        SimpleExample("COUNT([OrderID])"),
    ],
)

FUNCTION_COUNT_IF = FunctionDocRegistryItem(
    name="count_if",
    category=CATEGORY_AGGREGATION,
    description=_("Returns the number of items in the group meeting the {arg:0} condition."),
    examples=[
        SimpleExample("COUNT_IF([Profit] > 5)"),
    ],
)

FUNCTION_COUNTD = FunctionDocRegistryItem(
    name="countd",
    category=CATEGORY_AGGREGATION,
    description=_("Returns the number of unique values in the group.\n" "\n" "See also {ref:COUNTD_APPROX}."),
    examples=[
        SimpleExample("COUNTD([ClientID])"),
    ],
)

FUNCTION_COUNTD_IF = FunctionDocRegistryItem(
    name="countd_if",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the number of unique values in the group that meet the {arg:1} "
        "condition.\n"
        "\n"
        "See also {ref:COUNTD_APPROX}."
    ),
    examples=[
        SimpleExample("COUNTD_IF([ClientID], [Profit] > 5)"),
    ],
)

FUNCTION_COUNTD_APPROX = FunctionDocRegistryItem(
    name="countd_approx",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the approximate number of unique values in the group. Faster than "
        "{ref:COUNTD}, but doesn't guarantee accuracy."
    ),
    examples=[
        SimpleExample("COUNTD_APPROX([ClienID])"),
    ],
)

FUNCTION_STDEV = FunctionDocRegistryItem(
    name="stdev",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the statistical {link: standard_dev: standard deviation} of all values in the expression "
        "based on a selection from the population."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TEMP,
                show_source_table=True,
                formulas_as_names=False,
                formula_fields=[("Temperature SD", "STDEV([Temperature])")],
            ),
        ),
    ],
)

FUNCTION_STDEVP = FunctionDocRegistryItem(
    name="stdevp",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the statistical {link: standard_dev: standard deviation} of all values in the expression "
        "based on the biased population. "
        "The function shows how far data points are from their average. "
        "In other words, standard deviation shows to what extent values in a dataset deviate from their average."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TEMP,
                show_source_table=True,
                formulas_as_names=False,
                formula_fields=[("Temperature SD", "STDEVP([Temperature])")],
            ),
        ),
    ],
)

FUNCTION_VAR = FunctionDocRegistryItem(
    name="var",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the statistical variance of all values in an expression based on a " "selection from the population."
    ),
    examples=[
        SimpleExample("VAR([Profit])"),
    ],
)

FUNCTION_VARP = FunctionDocRegistryItem(
    name="varp",
    category=CATEGORY_AGGREGATION,
    description=_("Returns the statistical variance of all values in an expression across the " "entire population."),
    examples=[
        SimpleExample("VARP([Profit])"),
    ],
)

FUNCTION_QUANTILE = FunctionDocRegistryItem(
    name="quantile",
    category=CATEGORY_AGGREGATION,
    description=_("Returns the precise {arg:1}-level quantile ({arg:1} should be in range " "from 0 to 1)."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TEMP,
                show_source_table=True,
                formulas_as_names=False,
                formula_fields=[
                    ("0.25 quantile", "QUANTILE([Temperature], 0.25)"),
                    ("0.5 quantile", "QUANTILE([Temperature], 0.5)"),
                    ("0.75 quantile", "QUANTILE([Temperature], 0.75)"),
                ],
            ),
        ),
    ],
)

FUNCTION_QUANTILE_APPROX = FunctionDocRegistryItem(
    name="quantile_approx",
    category=CATEGORY_AGGREGATION,
    description=_("Returns the approximate {arg:1}-level quantile ({arg:1} should be in range from " "0 to 1)."),
)

FUNCTION_MEDIAN = FunctionDocRegistryItem(
    name="median",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns the {link: median_link: median} value. "
        "For an even number of items, it returns the greatest of the neighboring items in the central position."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_INT_FLOAT_1,
                show_source_table=True,
                formulas_as_names=False,
                formula_fields=[("Median Profit", "MEDIAN([Profit])")],
            ),
        ),
    ],
)

FUNCTION_ANY = FunctionDocRegistryItem(
    name="any",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns one of the values of {arg:0} from the group. This is a "
        "nondeterministic aggregation — the result may vary for the same data over "
        "multiple queries."
    ),
    examples=[
        SimpleExample("ANY([Profit])"),
    ],
)

FUNCTION_ARG_MIN = FunctionDocRegistryItem(
    name="arg_min",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns {arg:0} for the minimum value of {arg:1} in the group. If multiple "
        "values of {arg:0} match the minimum value of {arg:1}, then the first one "
        "encountered is returned. This makes the function non-deterministic."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TEMP,
                show_source_table=True,
                formulas_as_names=False,
                formula_fields=[
                    ("Coldest Month", "ARG_MIN([Month],[Temperature])"),
                ],
            ),
        ),
    ],
)

FUNCTION_ARG_MAX = FunctionDocRegistryItem(
    name="arg_max",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns {arg:0} for the maximum value of {arg:1} in the group. If multiple "
        "values of {arg:0} match the maximum value of {arg:1}, then the first one "
        "encountered is returned. This makes the function non-deterministic."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TEMP,
                show_source_table=True,
                formulas_as_names=False,
                formula_fields=[
                    ("Warmest Month", "ARG_MAX([Month],[Temperature])"),
                ],
            ),
        ),
    ],
)

FUNCTION_ALL_CONCAT = FunctionDocRegistryItem(
    name="all_concat",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns a string that contains all unique values of {arg:0} delimited by {arg:1} "
        "(if {arg:1} is not specified, a comma is used)."
    ),
    examples=[
        SimpleExample("ALL_CONCAT([Profit])"),
        SimpleExample("ALL_CONCAT([Profit], '; ')"),
    ],
)

FUNCTION_TOP_CONCAT = FunctionDocRegistryItem(
    name="top_concat",
    category=CATEGORY_AGGREGATION,
    description=_(
        "Returns a string that contains top {arg:1} unique values of {arg:0} delimited by {arg:2} "
        "(if {arg:2} is not specified, a comma is used)."
    ),
    examples=[
        SimpleExample("TOP_CONCAT([Profit], 3)"),
        SimpleExample("TOP_CONCAT([Profit], 3, '; ')"),
    ],
)

FUNCTIONS_AGGREGATION = [
    FUNCTION_SUM,
    FUNCTION_SUM_IF,
    FUNCTION_AVG,
    FUNCTION_AVG_IF,
    FUNCTION_MAX,
    FUNCTION_MIN,
    FUNCTION_COUNT,
    FUNCTION_COUNT_IF,
    FUNCTION_COUNTD,
    FUNCTION_COUNTD_IF,
    FUNCTION_COUNTD_APPROX,
    FUNCTION_STDEV,
    FUNCTION_STDEVP,
    FUNCTION_VAR,
    FUNCTION_VARP,
    FUNCTION_QUANTILE,
    FUNCTION_QUANTILE_APPROX,
    FUNCTION_MEDIAN,
    FUNCTION_ANY,
    FUNCTION_ARG_MIN,
    FUNCTION_ARG_MAX,
    FUNCTION_ALL_CONCAT,
    FUNCTION_TOP_CONCAT,
]
