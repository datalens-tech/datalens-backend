from typing import List

from dl_formula.core.datatype import DataType
from dl_formula_ref.categories.window import CATEGORY_WINDOW
from dl_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedTableResource,
    AliasedTextResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import (
    DataExample,
    SimpleExample,
)
from dl_formula_ref.registry.naming import CategoryPostfixNamingProvider
from dl_formula_ref.registry.note import (
    Note,
    NoteLevel,
)


_ = get_gettext()

_SOURCE_SALES_DATA_1 = ExampleSource(
    columns=[
        ("Date", DataType.STRING),
        ("City", DataType.STRING),
        ("Category", DataType.STRING),
        ("Orders", DataType.INTEGER),
        ("Profit", DataType.FLOAT),
    ],
    data=[
        ["2019-03-01", "London", "Office Supplies", 8, 120.8],
        ["2019-03-04", "London", "Office Supplies", 2, 100.0],
        ["2019-03-05", "London", "Furniture", 1, 750.0],
        ["2019-03-02", "Moscow", "Furniture", 2, 1250.5],
        ["2019-03-03", "Moscow", "Office Supplies", 4, 85.0],
        ["2019-03-01", "San Francisco", "Office Supplies", 23, 723.0],
        ["2019-03-01", "San Francisco", "Furniture", 1, 1000.0],
        ["2019-03-03", "San Francisco", "Furniture", 4, 4000.0],
        ["2019-03-02", "Detroit", "Furniture", 5, 3700.0],
        ["2019-03-04", "Detroit", "Office Supplies", 25, 1200.0],
        ["2019-03-04", "Detroit", "Furniture", 2, 3500.00],
    ],
)


def _make_standard_window_examples(func: str) -> List[DataExample]:
    func = func.upper()
    examples = [
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with grouping"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]", "[Category]"],
                order_by=["[City]", "[Category]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Category", "[Category]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Category", "[Category]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f"{func}([Order Sum] TOTAL)"),
                        (f"{func} 2", f"{func}([Order Sum] WITHIN [City])"),
                        (f"{func} 3", f"{func}([Order Sum] WITHIN [Category])"),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Category", "[Category]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f"{func}(SUM([Orders]) TOTAL)"),
                    (f"{func} 2", f"{func}(SUM([Orders]) WITHIN [City])"),
                    (f"{func} 3", f"{func}(SUM([Orders]) AMONG [City])"),
                ],
            ),
        ),
    ]
    return examples


def _make_rank_examples(func: str) -> List[DataExample]:
    func = func.upper()
    examples = [
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with two arguments"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f'{func}([Order Sum], "desc")'),
                        (f"{func} 2", f'{func}([Order Sum], "asc")'),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f'{func}(SUM([Orders]), "desc")'),
                    (f"{func} 2", f'{func}(SUM([Orders]), "asc")'),
                ],
            ),
        ),
    ]
    return examples


_RANK_RESOURCES = SimpleAliasedResourceRegistry(
    resources={
        "rank_direction_description": AliasedTextResource(
            body=_(
                'If {arg:1} is `"desc"` or omitted, then ranking is done from greatest to '
                'least, if `"asc"`, then from least to greatest.'
            ),
        ),
    }
)

FUNCTION_RANK = FunctionDocRegistryItem(
    name="rank",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the rank of the current row if ordered by the given argument. Rows "
        "corresponding to the same value used for sorting have the same rank. If the "
        "first two rows both have rank of `1`, then the next row (if it features a "
        "different value) will have rank `3`, so, in effect, it is rank with gaps.\n"
        "\n"
        "{text:rank_direction_description}\n"
        "\n"
        "See also {ref: RANK_DENSE}, {ref: RANK_UNIQUE}, {ref:RANK_PERCENTILE}."
    ),
    resources=_RANK_RESOURCES,
    examples=[
        *_make_rank_examples("rank"),
        *_make_standard_window_examples("rank"),
    ],
)

FUNCTION_RANK_DENSE = FunctionDocRegistryItem(
    name="rank_dense",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the rank of the current row if ordered by the given argument. Rows "
        "corresponding to the same value used for sorting have the same rank. If the "
        "first two rows both have rank of `1`, then the next row (if it features a "
        "different value) will have rank `2`, (rank without gaps).\n"
        "\n"
        "{text:rank_direction_description}\n"
        "\n"
        "See also {ref: RANK}, {ref: RANK_UNIQUE}, {ref:RANK_PERCENTILE}."
    ),
    resources=_RANK_RESOURCES,
    examples=[
        *_make_rank_examples("rank_dense"),
        *_make_standard_window_examples("rank_dense"),
    ],
)

FUNCTION_RANK_UNIQUE = FunctionDocRegistryItem(
    name="rank_unique",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the rank of the current row if ordered by the given argument. Rows "
        "corresponding to the same value have different rank values. This means that "
        "rank values are sequential and different for all rows, always increasing by "
        "`1` for the next row.\n"
        "\n"
        "{text:rank_direction_description}\n"
        "\n"
        "See also {ref: RANK}, {ref: RANK_DENSE}, {ref:RANK_PERCENTILE}."
    ),
    resources=_RANK_RESOURCES,
    examples=[
        *_make_rank_examples("rank_unique"),
        *_make_standard_window_examples("rank_unique"),
    ],
)

FUNCTION_RANK_PERCENTILE = FunctionDocRegistryItem(
    name="rank_percentile",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the relative rank (from `0` to `1`) of the current row if ordered by the "
        "given argument. Calculated as `(RANK(...) - 1) / (row count) `.\n"
        "\n"
        "{text:rank_direction_description}\n"
        "\n"
        "See also {ref: RANK}, {ref: RANK_DENSE}, {ref: RANK_UNIQUE}."
    ),
    resources=_RANK_RESOURCES,
    examples=[
        *_make_rank_examples("rank_percentile"),
        *_make_standard_window_examples("rank_percentile"),
    ],
)

FUNCTION_SUM_WINDOW = FunctionDocRegistryItem(
    name="sum",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="sum_window",
    ),
    category=CATEGORY_WINDOW,
    description=_("Returns the sum of all expression values. Applicable to numeric data types only."),
    examples=[*_make_standard_window_examples("sum")],
)

FUNCTION_MIN_WINDOW = FunctionDocRegistryItem(
    name="min",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="min_window",
    ),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the minimum value.\n\n"
        "If {arg:0}:\n"
        "- number — Returns the smallest number.\n"
        "- date — Returns the earliest date.\n"
        "- string — Returns the first value in the alphabetic order.\n"
    ),
    examples=[*_make_standard_window_examples("min")],
)

FUNCTION_MAX_WINDOW = FunctionDocRegistryItem(
    name="max",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="max_window",
    ),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the maximum value.\n\n"
        "If {arg:0}:\n"
        "- number — Returns the largest number.\n"
        "- date — Returns the latest date.\n"
        "- string — Returns the last value in the alphabetic order.\n"
    ),
    examples=[*_make_standard_window_examples("max")],
)

FUNCTION_COUNT_WINDOW = FunctionDocRegistryItem(
    name="count",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="count_window",
    ),
    category=CATEGORY_WINDOW,
    description=_("Returns the number of items in the specified window."),
    examples=[*_make_standard_window_examples("count")],
)

FUNCTION_AVG_WINDOW = FunctionDocRegistryItem(
    name="avg",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="avg_window",
    ),
    category=CATEGORY_WINDOW,
    description=_("Returns the average of all values. Applicable to numeric data types."),
    examples=[*_make_standard_window_examples("avg")],
)

FUNCTION_SUM_IF_WINDOW = FunctionDocRegistryItem(
    name="sum_if",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="sum_if_window",
    ),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the sum of all the expression values that meet the {arg:1} condition. "
        "Applicable to numeric data types only."
    ),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "SUM_IF([Profit], [Category] = 'Office Supplies' TOTAL)",  # translates into:
            #    SUM("Profit") FILTER (WHERE "Category" = 'Office Supplies')
            #    OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            "SUM_IF([Profit], [Category] = 'Office Supplies' WITHIN [Date])",  # translates into:
            #    SUM("Profit") FILTER (WHERE "Category" = 'Office Supplies')
            #    OVER (
            #        PARTITION BY "Date"
            #        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            #    )
            "SUM_IF([Profit], [Category] = 'Office Supplies' AMONG [Date])",  # translates into:
            #    SUM("Profit") FILTER (WHERE "Category" = 'Office Supplies')
            #    OVER (
            #        PARTITION BY <all dimensions except "Date">
            #        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            #    )
        )
    ],
)

FUNCTION_COUNT_IF_WINDOW = FunctionDocRegistryItem(
    name="count_if",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="count_if_window",
    ),
    category=CATEGORY_WINDOW,
    description=_("Returns the number of items in the specified window meeting the {arg:0} condition."),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "COUNT_IF([Profit], [Category] = 'Office Supplies' TOTAL)",
            "COUNT_IF([Profit], [Category] = 'Office Supplies' WITHIN [Date])",
            "COUNT_IF([Profit], [Category] = 'Office Supplies' AMONG [Date])",
        )
    ],
)

FUNCTION_AVG_IF_WINDOW = FunctionDocRegistryItem(
    name="avg_if",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(
        internal_name="avg_if_window",
    ),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the average of all values that meet the {arg:1} condition. "
        "If the values don't exist, it returns `NULL`. Applicable to numeric data types only."
    ),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "AVG_IF([Profit], [Category] = 'Office Supplies' TOTAL)",
            "AVG_IF([Profit], [Category] = 'Office Supplies' WITHIN [Date])",
            "AVG_IF([Profit], [Category] = 'Office Supplies' AMONG [Date])",
        )
    ],
)


def _make_simple_order_by_examples(func: str) -> List[DataExample]:
    func = func.upper()
    examples = [
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with ORDER BY"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f"{func}([Order Sum] ORDER BY [City] DESC)"),
                        (f"{func} 2", f"{func}([Order Sum] ORDER BY [Order Sum])"),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f"{func}(SUM([Orders]) ORDER BY [City] DESC)"),
                    (f"{func} 2", f"{func}(SUM([Orders]) ORDER BY [Order Sum])"),
                ],
            ),
        ),
    ]
    return examples


_ORDERED_WFUNC_NOTES = [
    Note(
        level=NoteLevel.warning,
        text=Translatable(
            "The sorting order is based on the fields listed in the sorting section of "
            "the chart and in the `ORDER BY` clause. First, `ORDER BY` fields are used, "
            "and then they are complemented by the fields from the chart."
        ),
    ),
]


def _make_rfunc_examples(func: str) -> list[DataExample]:
    func = func.upper()
    examples = [
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with ORDER BY"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f'{func}([Order Sum], "desc" ORDER BY [City])'),
                        (f"{func} 2", f'{func}([Order Sum], "asc" ORDER BY [City] DESC)'),
                        (f"{func} 3", f"{func}([Order Sum] ORDER BY [Order Sum], [City])"),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f'{func}(SUM([Orders]), "desc")'),
                    (f"{func} 2", f'{func}(SUM([Orders]), "asc" ORDER BY [City] DESC)'),
                    (f"{func} 3", f"{func}(SUM([Orders]) ORDER BY [Order Sum])"),
                ],
            ),
        ),
    ]
    return examples


_RFUNC_RESOURCES = SimpleAliasedResourceRegistry(
    resources={
        "rwindow_description_table": AliasedTableResource(
            table_body=[
                ["{arg:1}", _("Window")],
                ['`"asc"`', _("Starts from the first row and ends at the current row.")],
                ['`"desc"`', _("Starts from the current row and ends at the last row.")],
            ]
        ),
        "rwindow_description": AliasedTextResource(
            body=_("\n" "{table:rwindow_description_table}\n" "\n" 'By default `"asc"` is used.\n')
        ),
    }
)

FUNCTION_RSUM = FunctionDocRegistryItem(
    name="rsum",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the sum of all values in a growing (or shrinking) window defined by "
        "the sort order and the value of {arg:1}:\n"
        "{text:rwindow_description}\n"
        "\n"
        "Window functions with a similar behavior: {ref:RCOUNT}, {ref:RMIN}, {ref:RMAX}, {ref:RAVG}.\n"
        "\n"
        "See also {ref:aggregation/SUM}, {ref:MSUM}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_RFUNC_RESOURCES,
    examples=[
        *_make_standard_window_examples("rsum"),
        *_make_rfunc_examples("rsum"),
    ],
)

FUNCTION_RCOUNT = FunctionDocRegistryItem(
    name="rcount",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the count of all values in a growing (or shrinking) window defined "
        "by the sort order and the value of {arg:1}:\n"
        "{text:rwindow_description}\n"
        "\n"
        "Window functions with a similar behavior: {ref:RSUM}, {ref:RMIN}, {ref:RMAX}, {ref:RAVG}.\n"
        "\n"
        "See also {ref:aggregation/COUNT}, {ref:MCOUNT}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_RFUNC_RESOURCES,
    examples=[
        *_make_standard_window_examples("rcount"),
        *_make_rfunc_examples("rcount"),
    ],
)

FUNCTION_RMIN = FunctionDocRegistryItem(
    name="rmin",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the minimum of all values in a growing (or shrinking) window defined "
        "by the sort order and the value of {arg:1}:\n"
        "{text:rwindow_description}\n"
        "\n"
        "Window functions with a similar behavior: {ref:RSUM}, {ref:RCOUNT}, {ref:RMAX}, {ref:RAVG}.\n"
        "\n"
        "See also {ref:aggregation/MIN}, {ref:MMIN}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_RFUNC_RESOURCES,
    examples=[
        *_make_standard_window_examples("rmin"),
        *_make_rfunc_examples("rmin"),
    ],
)

FUNCTION_RMAX = FunctionDocRegistryItem(
    name="rmax",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the maximum of all values in a growing (or shrinking) window defined "
        "by the sort order and the value of {arg:1}:\n"
        "{text:rwindow_description}\n"
        "\n"
        "Window functions with a similar behavior: {ref:RSUM}, {ref:RCOUNT}, "
        "{ref:RMIN}, {ref:RAVG}.\n"
        "\n"
        "See also {ref:aggregation/MAX}, {ref:MMAX}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_RFUNC_RESOURCES,
    examples=[
        *_make_standard_window_examples("rmax"),
        *_make_rfunc_examples("rmax"),
    ],
)

FUNCTION_RAVG = FunctionDocRegistryItem(
    name="ravg",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the average of all values in a growing (or shrinking) window defined "
        "by the sort order and the value of {arg:1}:\n"
        "{text:rwindow_description}\n"
        "\n"
        "Window functions with a similar behavior: {ref:RSUM}, {ref:RCOUNT}, {ref:RMIN}, {ref:RMAX}.\n"
        "\n"
        "See also {ref:aggregation/AVG}, {ref:MAVG}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_RFUNC_RESOURCES,
    examples=[
        *_make_standard_window_examples("ravg"),
        *_make_rfunc_examples("ravg"),
    ],
)


def _make_mfunc_examples(func: str) -> List[DataExample]:
    func = func.upper()
    examples = [
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with two and three arguments"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f"{func}([Order Sum], 1 ORDER BY [City])"),
                        (f"{func} 2", f"{func}([Order Sum], -2 ORDER BY [City])"),
                        (f"{func} 3", f"{func}([Order Sum], 1, 1 ORDER BY [City])"),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f"{func}(SUM([Orders]), 1)"),
                    (f"{func} 2", f"{func}(SUM([Orders]), -2)"),
                    (f"{func} 3", f"{func}(SUM([Orders]) 1, 1)"),
                ],
            ),
        ),
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with ORDER BY"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f"{func}([Order Sum], 1 ORDER BY [City] DESC)"),
                        (f"{func} 2", f"{func}([Order Sum], 1 ORDER BY [Order Sum])"),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f"{func}(SUM([Orders]), 1 ORDER BY [City] DESC)"),
                    (f"{func} 2", f"{func}(SUM([Orders]), 1 ORDER BY [Order Sum])"),
                ],
            ),
        ),
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with grouping"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]", "[Category]"],
                order_by=["[City]", "[Category]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Category", "[Category]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Category", "[Category]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f"{func}([Order Sum], 1 TOTAL)"),
                        (f"{func} 2", f"{func}([Order Sum], 1 WITHIN [City])"),
                        (f"{func} 3", f"{func}([Order Sum], 1 WITHIN [Category])"),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Category", "[Category]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f"{func}(SUM([Orders]), 1 TOTAL ORDER BY [City])"),
                    (f"{func} 2", f"{func}(SUM([Orders]), 1 WITHIN [City] ORDER BY [City])"),
                    (f"{func} 3", f"{func}(SUM([Orders]), 1 AMONG [City] ORDER BY [City])"),
                ],
            ),
        ),
    ]
    return examples


_MFUNC_RESOURCES = SimpleAliasedResourceRegistry(
    resources={
        "mwindow_description_table": AliasedTableResource(
            table_body=[
                ["{arg:1}", "{arg:2}", _("Window")],
                [_("positive"), "-", _("The current row and {arg:1} preceding rows.")],
                [_("negative"), "-", _("The current row and -{arg:1} following rows.")],
                [
                    _("any sign"),
                    _("any sign"),
                    _("{arg:1} preceding rows, the current row and {arg:2} following rows."),
                ],
            ]
        ),
        "mwindow_description": AliasedTextResource(body=("\n" "{table:mwindow_description_table}\n" "\n")),
    }
)

FUNCTION_MSUM = FunctionDocRegistryItem(
    name="msum",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the moving sum of values in a fixed-size window defined by "
        "the sort order and arguments:\n"
        "{text:mwindow_description}\n"
        "Window functions with a similar behavior: {ref:MCOUNT}, {ref:MMIN}, {ref:MMAX}, {ref:MAVG}.\n"
        "\n"
        "See also {ref:aggregation/SUM}, {ref:RSUM}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_MFUNC_RESOURCES,
    examples=[
        *_make_mfunc_examples("msum"),
    ],
)

FUNCTION_MCOUNT = FunctionDocRegistryItem(
    name="mcount",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the moving count of (non-`NULL`) values in a fixed-size window defined by "
        "the sort order and arguments:\n"
        "{text:mwindow_description}\n"
        "Window functions with a similar behavior: {ref:MSUM}, {ref:MMIN}, {ref:MMAX}, {ref:MAVG}.\n"
        "\n"
        "See also {ref:aggregation/COUNT}, {ref:RCOUNT}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_MFUNC_RESOURCES,
    examples=[
        *_make_mfunc_examples("mcount"),
    ],
)

FUNCTION_MMIN = FunctionDocRegistryItem(
    name="mmin",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the moving minimum of values in a fixed-size window defined by "
        "the sort order and arguments:\n"
        "{text:mwindow_description}\n"
        "Window functions with a similar behavior: {ref:MSUM}, {ref:MCOUNT}, {ref:MMAX}, {ref:MAVG}.\n"
        "\n"
        "See also {ref:aggregation/MIN}, {ref:RMIN}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_MFUNC_RESOURCES,
    examples=[
        *_make_mfunc_examples("mmin"),
    ],
)

FUNCTION_MMAX = FunctionDocRegistryItem(
    name="mmax",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the moving maximum of values in a fixed-size window defined by "
        "the sort order and arguments:\n"
        "{text:mwindow_description}\n"
        "Window functions with a similar behavior: {ref:MSUM}, {ref:MCOUNT}, {ref:MMIN}, {ref:MAVG}.\n"
        "\n"
        "See also {ref:aggregation/MAX}, {ref:RMAX}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_MFUNC_RESOURCES,
    examples=[
        *_make_mfunc_examples("mmax"),
    ],
)

FUNCTION_MAVG = FunctionDocRegistryItem(
    name="mavg",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns the moving average of values in a fixed-size window defined by "
        "the sort order and arguments:\n"
        "{text:mwindow_description}\n"
        "Window functions with a similar behavior: {ref:MSUM}, {ref:MCOUNT}, {ref:MMIN}, {ref:MMAX}.\n"
        "\n"
        "See also {ref:aggregation/AVG}, {ref:RAVG}."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    resources=_MFUNC_RESOURCES,
    examples=[
        *_make_mfunc_examples("mavg"),
    ],
)


def _make_lag_examples(func: str) -> List[DataExample]:
    func = func.upper()
    examples = [
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with the optional argument"),
                source=_SOURCE_SALES_DATA_1,
                group_by=["[City]"],
                order_by=["[City]"],
                show_source_table=True,
                formula_fields=[("City", "[City]"), ("Order Sum", "SUM([Orders])")],
                additional_transformations=[
                    [
                        ("City", "[City]"),
                        ("Order Sum", "[Order Sum]"),
                        (f"{func} 1", f"{func}([Order Sum], 1 ORDER BY [City])"),
                        (f"{func} 2", f"{func}([Order Sum], -2 ORDER BY [City])"),
                    ],
                ],
                override_formula_fields=[
                    ("City", "[City]"),
                    ("Order Sum", "SUM([Orders])"),
                    (f"{func} 1", f"{func}(SUM([Orders]), 1)"),
                    (f"{func} 2", f"{func}(SUM([Orders]), -2)"),
                ],
            ),
        ),
    ]
    return examples


FUNCTION_LAG = FunctionDocRegistryItem(
    name="lag",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_(
        "Returns {arg:0} re-evaluated against the row that is offset from the current "
        "row by {arg:1} within the specified window:\n"
        "- Positive {arg:1} seeks among preceding rows.\n"
        "- Negative {arg:1} seeks among following rows.\n"
        "\n"
        "By default {arg:1} is `1`.\n"
        "\n"
        "If there is no available value ({arg:1} reaches before the first row or "
        "after the last one), then {arg:2} is returned. If {arg:2} is not specified, "
        "then `NULL` is used.\n"
        "\n"
        "See also {ref:AGO} for a non-window function alternative."
    ),
    notes=_ORDERED_WFUNC_NOTES,
    examples=[
        *_make_standard_window_examples("lag"),
        *_make_lag_examples("lag"),
        *_make_simple_order_by_examples("lag"),
    ],
)

FUNCTION_FIRST = FunctionDocRegistryItem(
    name="first",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_("Returns the value of {arg:0} from the first row in the window. See also {ref:LAST}."),
    notes=_ORDERED_WFUNC_NOTES,
    examples=[
        *_make_standard_window_examples("first"),
        *_make_simple_order_by_examples("first"),
    ],
)

FUNCTION_LAST = FunctionDocRegistryItem(
    name="last",
    is_window=True,
    naming_provider=CategoryPostfixNamingProvider(),
    category=CATEGORY_WINDOW,
    description=_("Returns the value of {arg:0} from the last row in the window. See also {ref:FIRST}."),
    notes=_ORDERED_WFUNC_NOTES,
    examples=[
        *_make_standard_window_examples("last"),
        *_make_simple_order_by_examples("last"),
    ],
)

FUNCTIONS_WINDOW = [
    FUNCTION_RANK,
    FUNCTION_RANK_DENSE,
    FUNCTION_RANK_UNIQUE,
    FUNCTION_RANK_PERCENTILE,
    FUNCTION_SUM_WINDOW,
    FUNCTION_MIN_WINDOW,
    FUNCTION_MAX_WINDOW,
    FUNCTION_COUNT_WINDOW,
    FUNCTION_AVG_WINDOW,
    FUNCTION_SUM_IF_WINDOW,
    FUNCTION_COUNT_IF_WINDOW,
    FUNCTION_AVG_IF_WINDOW,
    FUNCTION_RSUM,
    FUNCTION_RCOUNT,
    FUNCTION_RMIN,
    FUNCTION_RMAX,
    FUNCTION_RAVG,
    FUNCTION_MSUM,
    FUNCTION_MCOUNT,
    FUNCTION_MMIN,
    FUNCTION_MMAX,
    FUNCTION_MAVG,
    FUNCTION_LAG,
    FUNCTION_FIRST,
    FUNCTION_LAST,
]
