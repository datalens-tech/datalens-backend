from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedTextResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_TIME_SERIES = FunctionDocCategory(
    name="time-series",
    description=_(
        "Time series functions provide various ways to look up values corresponding "
        "to a specific time or offset along a given time axis.\n"
        "\n"
        "In a certain way this functionality is similar to the window function {ref:"
        "LAG}.\n"
        "The main difference is that `LAG` is indifferent to the actual values of the "
        "dimensions being used, and operates over positional offsets specified in "
        "_rows_, while time series functions use specific values and value offsets in "
        "date/time units like days, hours or seconds. This makes them sensitive to "
        'missing values in data. As a result of this `AGO(SUM([Sales]), [Date], "year'
        '")` will return `NULL` if the same-date row for the previous year is '
        "missing.\n"
        "\n"
        "Time series functions work with specific values or deviations specified in "
        "time units (days, hours, or seconds). Due to that, they become sensitive to "
        'missing values in data. As the result, the `AGO(SUM([Sales]), [Date], "year")` '
        "formula will return `NULL` if there is no data for the same date in the previous "
        "year. Also, note that when you use data in date and time format, you might have "
        "data for the same date, but not for the exact same second. Besides, when rounding "
        "dates to weeks, a shift in years will also return `NULL`.\n"
        "\n"
        "{text:time_series_syntax}\n"
    ),
    resources=SimpleAliasedResourceRegistry(
        resources={
            "time_series_syntax": AliasedTextResource(
                body=_(
                    "## Syntax {#syntax}\n"
                    "\n"
                    "Time series functions support extended syntax:\n"
                    "{text:time_series_signature}\n"
                    "### BEFORE FILTER BY {#syntax-before-filter-by}\n"
                    "\n"
                    "If any fields are listed in `BEFORE FILTER BY`, then the function is "
                    "calculated before data is filtered using these fields.\n"
                    "Let's say you are trying to calculate the value of `[Sales]` a year ago from "
                    "today's date:\n"
                    "```\n"
                    'AGO([Sales], [Date], "year", 1)\n'
                    "```\n"
                    "and also have a filter in the chart limiting the dates to a specific year "
                    "(`2018 < [Year] <= 2019`, where `Year` is calculated as `YEAR([Date])`). All "
                    "data pertaining to the year 2018 will be omitted from the result. Because of "
                    "this the function will return `NULL`.\n"
                    "\n"
                    "If `BEFORE FILTER BY` is added to the function:\n"
                    "```\n"
                    'AGO([Sales], [Date], "year", 1 BEFORE FILTER BY [Year])\n'
                    "```\n"
                    "it will return the value of `[Sales]`.\n"
                    "\n"
                    "The date/time dimension specified as the second argument of time series "
                    "functions is implicitly added to the `BEFORE FILTER BY` clause. For example, "
                    "the following to formulas are equivalent:\n"
                    "```\n"
                    'AGO([Sales], [Date], "week")\n'
                    "```\n"
                    "```\n"
                    'AGO([Sales], [Date], "week" BEFORE FILTER BY [Date])\n'
                    "```\n"
                    "\n"
                    "### IGNORE DIMENSIONS {#syntax-ignore-dimensions}\n"
                    "\n"
                    "The `IGNORE DIMENSIONS` clause allows the exclusion of dimensions from the "
                    "search criteria. When looking up a value for a certain date, the search is "
                    "done by matching dimension values in each row against the values of the same "
                    "dimensions in the original row. If any of the other dimensions correlate "
                    "with the date dimension, then the data query may return an empty result "
                    "(`NULL`).\n"
                    "\n"
                    "For example, if the data request contains `[Date]`, `[Month]` and "
                    '`AGO([Sales], [Date], "month")`, then it will be impossible to find a row '
                    "with `[Date]` being a month earlier, but the value of `[Month]` being the "
                    "same as in the current row. This will result in `AGO` always returning "
                    "`NULL`.\n"
                    "\n"
                    "To get the correct value of `[Sales]` exclude `[Month]` using the `IGNORE "
                    "DIMENSIONS` clause:\n"
                    "```\n"
                    'AGO([Sales], [Date], "month" IGNORE DIMENSIONS [Month])\n'
                    "```"
                )
            ),
            "time_series_signature": AliasedTextResource(
                body=_(
                    "```\n"
                    "<FUNCTION_NAME>(\n"
                    "    arg1, arg2, ...\n"
                    "\n"
                    "    [ BEFORE FILTER BY filtered_field_1, ... ]\n"
                    "    [ IGNORE DIMENSIONS dimension_1, ... ]\n"
                    ")\n"
                    "```\n"
                )
            ),
        }
    ),
    keywords="",
)
