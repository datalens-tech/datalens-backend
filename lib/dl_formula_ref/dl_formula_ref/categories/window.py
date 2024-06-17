from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    AliasedTableResource,
    AliasedTextResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_WINDOW = FunctionDocCategory(
    name="window",
    description=_(
        "Window functions in {{ datalens-short-name }} are calculated as aggregate functions of measures. "
        "At the same time, grouping by dimensions differs from the grouping set in the chart. "
        "Grouping parameters are specified in the function call as a list of dimensions to be included in the grouping (`WITHIN ...`) or excluded from it (`AMONG ...`).\n"
        "\n"
        "Given that measures are aggregated values, one may consider window functions in {{ datalens-short-name }} "
        "as aggregations of aggregations. One should note that not all aggregate functions would have the result of "
        "aggregating particular results that is identical to the general aggregation of values.\n"
        "\n"
        "For example, when calculating the sum of a measure for each group and then adding these values together, "
        "you will get the total sum of the measure in all groups:\n"
        "```\n"
        "SUM(SUM(a) UNION SUM(b)) = SUM(a UNION b)\n"
        "```\n"
        "When counting the amount of measure values for each group and then trying to calculate the amount from the "
        "sum of these values, you will get the amount of amounts (summands in the sum) rather than the total amount of values in all groups:\n"
        "```\n"
        "COUNT(COUNT(a) UNION COUNT(b)) = 2\n"
        "```\n"
        "\n"
        "**Example**\n"
        "\n"
        "{table:win_functions_example}\n"
        "\n"
        "Where:\n"
        "* **Sales**: Sum of the sales measure in the categories\n"
        "* **Order Count**: Amount of the order measure values in the categories\n"
        "* **Sales_Furniture+Technology**: Total sum of the sales measure in all categories:\n"
        "  ```\n"
        "  SUM_IF([Sales], [Category] = 'Furniture' TOTAL) + SUM_IF([Sales], [Category] = 'Technology' TOTAL)\n"
        "  ```\n"
        "* **OrderCount_Furniture+Technology**: Amount of amounts of the order measure values:\n"
        "  ```\n"
        "  COUNT(COUNT_IF([Order Count], [Category] = 'Furniture' TOTAL) +  COUNT_IF([Order Count], [Category] = 'Technology' TOTAL) TOTAL)\n"
        "  ```\n"
        "\n"
        "{text:window_usage_restrictions}\n"
        "{text:window_syntax}\n"
        "{text:agg_as_window}\n"
        "{text:window_examples}\n"
    ),
    resources=SimpleAliasedResourceRegistry(
        resources={
            "window_usage_restrictions": AliasedTextResource(
                body=_(
                    "## Usage Restrictions {#usage-restrictions}\n"
                    "\n"
                    "1. The first argument in window functions can only be {link:"
                    "field_concept_link:measures}.\n"
                    "\n"
                    "   For the `AVG_IF`, `COUNT_IF`, `SUM_IF` window functions, the first argument "
                    "(`expression` in the function description) must always be a measure.\n"
                    "\n"
                    "   Example:\n"
                    "\n"
                    "   `AVG_IF([Profit], [Profit] > 5)`\n"
                    "\n"
                    "   For other window functions, the first (and only) argument (`value` in the "
                    "function description) must always be a measure, too.\n"
                    "\n"
                    "   Examples:\n"
                    "   * Correct formula: `SUM(SUM([Profit]) TOTAL)`.\n"
                    "   * Incorrect formula: `RANK([Profit] TOTAL)`, where `[Profit]` is a non-"
                    "aggregated expression.\n"
                    "\n"
                    "1. For grouping window functions, only the {link:field_concept_link:"
                    "dimensions} used to build the chart can be applied. Only the dimensions used "
                    "to build the chart set the {link:chart_aggregation_link:grouping when "
                    "calculating a measure}. These dimensions define how values are split into "
                    "groups and therefore have fixed values in each group.\n"
                    "\n"
                    "   If you specify a dimension that was not used to build the chart, it won't "
                    "have a fixed value and the value can be different in each group row. As a "
                    "result, it will be impossible to determine which value of this dimension "
                    "must be used to calculate the measure. This limitation applies to the "
                    "`WITHIN` and `AMONG` grouping types.\n"
                    "\n"
                    "   Examples:\n"
                    "   * Correct formula: `RANK(SUM([Profit]) WITHIN [Category])` in the chart "
                    "with grouping by the `[Order Date]` and `[Category]` dimensions. "
                    "`[Category]` is used when building the chart.\n"
                    "   * Correct formula: `RANK(SUM([Profit]) WITHIN [Category])` in the chart with "
                    "grouping by the `[Order Date]` and `[City]` dimensions. `[Category]` is not part "
                    "of the grouping, so it will not be used in the calculation. The result will be "
                    "the same as when using the `RANK(SUM([Profit]) TOTAL)` formula.\n"
                    "   * Incorrect formula: `RANK(SUM([Profit]) AMONG [City])` in the chart with "
                    "grouping by the `[Order Date]` and `[Category]` dimensions. Running the function "
                    "will return the {link: unknown_win_dimension: Unknown dimension for window} error.\n"
                    "1. Window functions do not support managing the {link: lod_link: level of detail}. "
                    "This is only available for aggregate functions.\n"
                    "1. For functions that depend on sorting order, the fields "
                    "listed in `ORDER BY` must be used when building the chart.\n"
                    "1. If a window function is used to build a **Table** chart, we don't "
                    "recommend enabling the display of **Total** in the settings. This may cause "
                    "an error.\n"
                )
            ),
            "win_functions_example": AliasedTableResource(
                table_body=[
                    [
                        "`Category`",
                        "`Sales`",
                        "`OrderCount`",
                        "`Sales_Furniture+Technology`",
                        "`OrderCount_Furniture+Technology`",
                    ],
                    ["Furniture", "100000", "350", "300000", "2"],
                    ["Technology", "200000", "650", "300000", "2"],
                ]
            ),
            "window_func_signature": AliasedTextResource(
                body=_(
                    "```\n"
                    "<WINDOW_FUNCTION_NAME>(\n"
                    "    arg1, arg2, ...\n"
                    "\n"
                    "    [ TOTAL\n"
                    "    | WITHIN dim1, dim2, ...\n"
                    "    | AMONG dim1, dim2, ... ]\n"
                    "\n"
                    "    [ ORDER BY field1, field2, ... ]\n"
                    "\n"
                    "    [ BEFORE FILTER BY filtered_field1, ... ]\n"
                    ")\n"
                    "```\n"
                )
            ),
            "window_syntax": AliasedTextResource(
                body=_(
                    "## Syntax {#syntax}\n"
                    "\n"
                    "The general syntax for window functions is as follows:\n"
                    "{text:window_func_signature}\n"
                    "It starts off, just like a regular function call, with its name and "
                    "arguments (`arg1, arg2, ...` in this case).\n"
                    "\n"
                    "### Grouping {#syntax-grouping}\n"
                    "\n"
                    "The arguments are followed by a window grouping, which can be one of three "
                    "types:\n"
                    "* `TOTAL` (equivalent to `WITHIN` without dimensions): all query entries "
                    "fall into a single window.\n"
                    "* `WITHIN dim1, dim2, ...` : records are grouped by the dimensions `dim1, "
                    "dim2, ...`.\n"
                    "* `AMONG dim1, dim2, ...` : records are grouped by all dimensions from the "
                    "query, except those listed. For example, if we use formula "
                    "`RSUM(SUM([Sales]) AMONG dim1, dim2)` with dimensions `dim1`, `dim2`, "
                    "`dim3`, `dim4` in the data query, then the entries will be grouped by `dim3` "
                    "and `dim4`, so it will be equivalent to `RSUM([Sales] WITHIN dim3, dim4)`.\n"
                    "\n"
                    "The grouping clause is optional. `TOTAL` is used by default.\n"
                    "\n"
                    "{text:window_syntax_ordering}\n"
                    "{text:window_syntax_bfb}"
                )
            ),
            "window_syntax_ordering": AliasedTextResource(
                body=_(
                    "### Ordering {#syntax-order-by}\n"
                    "\n"
                    "After the grouping comes the ordering clause. It is only supported for order-"
                    "dependent functions:\n"
                    "\n"
                    "{table:order_dep_win_functions}\n"
                    "\n"
                    "The ordering clause is optional for these functions.\n"
                    "\n"
                    "See the descriptions of these functions for more information on how this "
                    "order affects the result value.\n"
                    "The `ORDER BY` clause accepts dimensions as well as measures. It also "
                    "supports the standard `ASC`/`DESC` syntax (`ASC` is assumed by default) to "
                    "specify ascending or descending order respectively:\n"
                    "`... ORDER BY [Date] ASC, SUM([Sales]) DESC, [Category] ...`\n"
                    "\n"
                    "Fields listed in `ORDER BY` are combined with fields listed in the chart's "
                    "sorting section.\n"
                    "Example:\n"
                    "* Function — `... ORDER BY [Date] DESC, [City]`.\n"
                    "* Chart — Sorted by `Date` and `Category`.\n"
                    "* Result — `Date` (descending), `City`, `Category`.\n"
                )
            ),
            "order_dep_win_functions": AliasedTableResource(
                table_body=[
                    ["`M*`", "`R*`", _("Positional functions")],
                    ["{ref:MAVG}", "{ref:RAVG}", "{ref:LAG}"],
                    ["{ref:MCOUNT}", "{ref:RCOUNT}", "{ref:FIRST}"],
                    ["{ref:MMAX}", "{ref:RMAX}", "{ref:LAST}"],
                    ["{ref:MMIN}", "{ref:RMIN}", ""],
                    ["{ref:MSUM}", "{ref:RSUM}", ""],
                ]
            ),
            "window_syntax_bfb": AliasedTextResource(
                body=_(
                    "### BEFORE FILTER BY {#syntax-before-filter-by}\n"
                    "\n"
                    "If any fields are listed in `BEFORE FILTER BY`, then this window function is "
                    "calculated before data is filtered using these fields.\n"
                    "\n"
                    "`BEFORE FILTER BY` applies to all nested window functions too.\n"
                    "Example:\n"
                    "* Formula — `MAVG(RSUM([Sales]), 10 BEFORE FILTER BY [Date])`.\n"
                    "* Equivalent — `MAVG(RSUM([Sales] BEFORE FILTER BY [Date]), 10 BEFORE FILTER "
                    "BY [Date])`.\n"
                    "\n"
                    "Do not use conflicting `BEFORE FILTER BY` clauses:\n"
                    "* Correct formula: `MAVG(RSUM([Sales] BEFORE FILTER BY [Date], [Category]), 10 BEFORE "
                    "FILTER BY [Date])` — functions are nested and (`[Date]`) is a subset of "
                    "(`[Date], [Category]`).\n"
                    "* Correct formula: `MAVG(RSUM([Sales] BEFORE FILTER BY [Category]), 10 BEFORE FILTER "
                    "BY [Date])` — functions are nested, so field lists are combined in the "
                    "second of the two functions.\n"
                    "* Correct formula: `RSUM([Sales] BEFORE FILTER BY [Date], [Category]) - RSUM([Sales] "
                    "BEFORE FILTER BY [Date])` — (`[Date]`) is a subset of (`[Date], "
                    "[Category]`).\n"
                    "* Incorrect formula: `RSUM([Sales] BEFORE FILTER BY [Category]) - RSUM([Sales] "
                    "BEFORE FILTER BY [Date])` — functions are not nested and neither of "
                    "(`[Category]`) and (`[Date]`) is a subset of the other.\n"
                )
            ),
            "agg_as_window": AliasedTextResource(
                body=_(
                    "## Aggregate Functions as Window Functions {#aggregate-functions-as-window-functions}\n"
                    "\n"
                    "The following aggregations can also be used as window functions:\n"
                    "\n"
                    "{table:agg_as_window_table}\n"
                    "\n"
                    "To use the window version of the aggregate functions, you must explicitly "
                    "specify the grouping (unlike other window functions, where it is optional).\n"
                    "\n"
                    "For example, the `SUM([Sales]) / SUM(SUM([Sales]) TOTAL)` expression can be used to calculate the "
                    "ratio of a group's sum of `[Sales]` to the sum of `[Sales]` among all "
                    "entries.\n"
                )
            ),
            "agg_as_window_table": AliasedTableResource(
                table_body=[
                    [_("Aggregations"), _("Conditional Aggregations")],
                    ["{ref:window/SUM}", "{ref:window/SUM_IF}"],
                    ["{ref:window/COUNT}", "{ref:window/COUNT_IF}"],
                    ["{ref:window/AVG}", "{ref:window/AVG_IF}"],
                    ["{ref:window/MAX}", ""],
                    ["{ref:window/MIN}", ""],
                ]
            ),
            "window_examples": AliasedTextResource(body=""),  # TODO
            "unknown_win_dimension": AliasedLinkResource(
                url=_("../troubleshooting/errors/ERR-DS_API-FORMULA-UNKNOWN_WINDOW_DIMENSION.md")
            ),
            "lod_link": AliasedLinkResource(url=_("./aggregation-functions.md#syntax-lod")),
        }
    ),
    keywords="",
)
