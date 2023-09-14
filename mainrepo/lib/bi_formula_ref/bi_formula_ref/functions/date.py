import datetime

from bi_formula.core.datatype import DataType
from bi_formula_ref.categories.date import CATEGORY_DATE
from bi_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from bi_formula_ref.localization import get_gettext
from bi_formula_ref.registry.base import FunctionDocRegistryItem
from bi_formula_ref.registry.example import (
    DataExample,
    SimpleExample,
)
from bi_formula_ref.registry.note import Note

_ = get_gettext()

_d = datetime.date
_dt = datetime.datetime

_SOURCE_DATE_1 = ExampleSource(
    columns=[("Date", DataType.DATE)], data=[[_d(2014, 10, 6)], [_d(2014, 10, 7)], [_d(2017, 3, 8)], [_d(2024, 2, 12)]]
)
_SOURCE_DATETIME_1 = ExampleSource(
    columns=[("DateTime", DataType.GENERICDATETIME)],
    data=[
        [_dt(2014, 10, 6, 7, 45, 12)],
        [_dt(2014, 10, 7, 11, 10, 15)],
        [_dt(2017, 3, 8, 23, 59, 59)],
        [_dt(2024, 2, 12, 7, 40, 33)],
    ],
)


FUNCTION_DATEADD = FunctionDocRegistryItem(
    name="dateadd",
    category=CATEGORY_DATE,
    description=_(
        "Returns the date obtained by adding {arg:1} in the amount of {arg:2} to the "
        "specified date {arg:0}.\n"
        "\n"
        "The {arg:2} argument is an integer. It can be negative.\n"
        "The {arg:1} argument takes the following values:\n"
        '- `"year"`;\n'
        '- `"month"`;\n'
        '- `"day"`;\n'
        '- `"hour"`;\n'
        '- `"minute"`;\n'
        '- `"second"`.'
    ),
    notes=[
        Note(
            _("For all sources except {dialects:CLICKHOUSE}, {arg:2} takes " "only constant values."),
        ),
    ],
    examples=[
        SimpleExample(example_str)
        for example_str in (
            'DATEADD(#2018-01-12#, "day", 6) = #2018-01-18#',
            'DATEADD(#2018-01-12#, "month", 6) = #2018-07-12#',
            'DATEADD(#2018-01-12#, "year", 6) = #2024-01-12#',
            'DATEADD(#2018-01-12 01:02:03#, "second", 6) = #2018-01-12 01:02:09#',
            'DATEADD(#2018-01-12 01:02:03#, "minute", 6) = #2018-01-12 01:08:03#',
            'DATEADD(#2018-01-12 01:02:03#, "hour", 6) = #2018-01-12 07:02:03#',
            'DATEADD(#2018-01-12 01:02:03#, "day", 6) = #2018-01-18 01:02:03#',
            'DATEADD(#2018-01-12 01:02:03#, "month", 6) = #2018-07-12 01:02:03#',
            'DATEADD(#2018-01-12 01:02:03#, "year", 6) = #2024-01-12 01:02:03#',
        )
    ],
)

FUNCTION_DATEPART = FunctionDocRegistryItem(
    name="datepart",
    category=CATEGORY_DATE,
    description=_(
        "Returns a part of the date as an integer.\n"
        "\n"
        "Depending on the argument, {arg:1} returns the following values:\n"
        '- `"year"` — the year number (see {ref:YEAR});\n'
        '- `"quarter"` — the number of the quarter (from `1` to `4`) of the year '
        "(see {ref:QUARTER});\n"
        '- `"month"` — the number of the month in the year (see {ref:MONTH});\n'
        '- `"week"` — the number of the week in the year according to {link: '
        "iso_8601: ISO 8601} (see {ref:WEEK});\n"
        '- `"dayofweek"`, `"dow"` — the number of the day of the week (see {ref:'
        "DAYOFWEEK});\n"
        '- `"day"` — the number of the day in the month (see {ref:DAY});\n'
        '- `"hour"` — the number of the hour in the day (see {ref:HOUR});\n'
        '- `"minute"` — the number of the minute in the hour (see {ref:MINUTE});\n'
        '- `"second"` — the number of the second in the minute (see {ref:SECOND}).\n'
        "\n"
        'If you select `"dayofweek"`, you can use the additional parameter {arg:2} '
        "to specify the first day of the week (Monday by default). Learn more about "
        "this parameter in the {ref:DAYOFWEEK} function description.\n"
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with date"),
                source=_SOURCE_DATE_1,
                formula_fields=[
                    ("Date", "[Date]"),
                    ("Year", 'DATEPART([Date], "year")'),
                    ("Month", 'DATEPART([Date], "month")'),
                    ("Day", 'DATEPART([Date], "day")'),
                    ("DayOfWeek", 'DATEPART([Date], "dayofweek")'),
                    ("DOW", 'DATEPART([Date], "dow")'),
                ],
                formulas_as_names=False,
            )
        ),
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with custom first day of the week"),
                source=_SOURCE_DATE_1,
                formula_fields=[
                    ("Date", "[Date]"),
                    ("DOW", 'DATEPART([Date], "dow")'),
                    ("DOW sun", 'DATEPART([Date], "dow", "sun")'),
                    ("DOW Monday", 'DATEPART([Date], "dow", "Monday")'),
                    ("DOW wed", 'DATEPART([Date], "dow", "wed")'),
                ],
                formulas_as_names=False,
            )
        ),
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with date and time"),
                source=_SOURCE_DATETIME_1,
                formula_fields=[
                    ("DateTime", "[DateTime]"),
                    ("Year", 'DATEPART([DateTime], "year")'),
                    ("Month", 'DATEPART([DateTime], "month")'),
                    ("Day", 'DATEPART([DateTime], "day")'),
                    ("Hour", 'DATEPART([DateTime], "hour")'),
                    ("Minute", 'DATEPART([DateTime], "minute")'),
                    ("Second", 'DATEPART([DateTime], "second")'),
                ],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTION_DATETRUNC = FunctionDocRegistryItem(
    name="datetrunc",
    category=CATEGORY_DATE,
    description=_(
        "Rounds {arg:0} down to the given {arg:1}. If optional {arg:2} is given, then "
        "the value is rounded down to a {arg:2} multiple of {arg:1} (omitting {arg:2} "
        "is the same as `{argn:2} = 1`).\n"
        "\n"
        "Supported units:\n"
        '- `"second"`;\n'
        '- `"minute"`;\n'
        '- `"hour"`;\n'
        '- `"day"` (acts as the day of the year if {arg:2} is specified);\n'
        '- `"week"`;\n'
        '- `"month"`;\n'
        '- `"quarter"`;\n'
        '- `"year"`.'
    ),
    notes=[
        Note(
            _(
                "The function with three arguments is only available for the sources "
                "{dialects:CLICKHOUSE_21_8} or higher."
            ),
        ),
    ],
    examples=[
        SimpleExample(example_str)
        for example_str in (
            'DATETRUNC(#2018-07-12 11:07:13#, "minute") = #2018-07-12 11:07:00#',
            'DATETRUNC(#2018-07-12#, "year", 5) = #2015-01-01#',
            'DATETRUNC(#2018-07-12 11:07:13#, "second", 5) = #2018-07-12 11:07:10#',
            'DATETRUNC(#2018-07-12 11:07:13#, "month", 4) = #2018-05-01 00:00:00#',
        )
    ],
)

FUNCTION_SECOND = FunctionDocRegistryItem(
    name="second",
    category=CATEGORY_DATE,
    description=_(
        "Returns the number of the second in the minute of the specified date "
        "{arg:0}. When the date is specified without time, it returns `0`."
    ),
    examples=[
        SimpleExample("SECOND(#2019-01-23 15:07:47#) = 47"),
    ],
)

FUNCTION_MINUTE = FunctionDocRegistryItem(
    name="minute",
    category=CATEGORY_DATE,
    description=_(
        "Returns the number of the minute in the hour of the specified date {arg:0}. "
        "When the date is specified without time, it returns `0`."
    ),
    examples=[
        SimpleExample("MINUTE(#2019-01-23 15:07:47#) = 7"),
    ],
)

FUNCTION_HOUR = FunctionDocRegistryItem(
    name="hour",
    category=CATEGORY_DATE,
    description=_(
        "Returns the number of the hour in the day of the specified date and time "
        "{arg:0}. When the date is specified without time, it returns `0`."
    ),
    examples=[
        SimpleExample("HOUR(#2019-01-23 15:07:47#) = 15"),
    ],
)

FUNCTION_DAY = FunctionDocRegistryItem(
    name="day",
    category=CATEGORY_DATE,
    description=_("Returns the number of the day in the month of the specified date {arg:0}."),
    examples=[
        SimpleExample("DAY(#2019-01-23#) = 23"),
    ],
)

FUNCTION_MONTH = FunctionDocRegistryItem(
    name="month",
    category=CATEGORY_DATE,
    description=_("Returns the number of the month in the year of the specified date {arg:0}."),
    examples=[
        SimpleExample("MONTH(#2019-01-23#) = 1"),
    ],
)

FUNCTION_QUARTER = FunctionDocRegistryItem(
    name="quarter",
    category=CATEGORY_DATE,
    description=_("Returns the number of the quarter (from `1` to `4`) of the year of the specified date {arg:0}."),
    examples=[
        SimpleExample("QUARTER(#2019-01-23#) = 1"),
    ],
)

FUNCTION_YEAR = FunctionDocRegistryItem(
    name="year",
    category=CATEGORY_DATE,
    description=_("Returns the year number in the specified date {arg:0}."),
    examples=[
        SimpleExample("YEAR(#2019-01-23#) = 2019"),
    ],
)

FUNCTION_DAYOFWEEK = FunctionDocRegistryItem(
    name="dayofweek",
    category=CATEGORY_DATE,
    description=_(
        "Returns the day of the week according to {link: iso_8601: ISO 8601}.\n"
        "- Monday — 1.\n"
        "- Sunday — 7.\n"
        "\n"
        "If the additional parameter {arg:1} is specified, this day is considered the "
        "first day of the week. Valid values:\n"
        '- `"Monday"`, `"Mon"` — Monday;\n'
        '- `"Tuesday"`. `"Tue"` — Tuesday;\n'
        '- `"Wednesday"`, `"Wed"` — Wednesday;\n'
        '- `"Thursday"`, `"Thu"` — Thursday;\n'
        '- `"Friday"`, `"Fri"` — Friday;\n'
        '- `"Saturday"`, ` "Sat"` — Saturday;\n'
        '- `"Sunday"`, `"Sun"` — Sunday.\n'
    ),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "DAYOFWEEK(#1971-01-14 01:02:03#) = 4",
            'DAYOFWEEK(#1971-01-14#, "wed") = 2',
            'DAYOFWEEK(#1971-01-14#, "wednesday") = 2',
        )
    ],
)

FUNCTION_WEEK = FunctionDocRegistryItem(
    name="week",
    category=CATEGORY_DATE,
    description=_(
        "The number of the week according to {link: iso_8601: ISO 8601}. The first "
        "week is the week that contains the first Thursday of the year or January 4th."
    ),
    examples=[
        SimpleExample("WEEK(#1971-01-14 01:02:03#) = 2"),
    ],
)

FUNCTION_NOW = FunctionDocRegistryItem(
    name="now",
    category=CATEGORY_DATE,
    description=_("Returns the current date and time, depending on the data source and " "connection type."),
    notes=[
        Note(
            _("On {dialects:YQL}, the function always returns the UTC date and time."),
        ),
    ],
    examples=[
        SimpleExample("NOW() = #2019-01-23 12:53:07#"),
    ],
)

FUNCTION_TODAY = FunctionDocRegistryItem(
    name="today",
    category=CATEGORY_DATE,
    description=_("Returns the current date, depending on the data source and connection type."),
    examples=[
        SimpleExample("TODAY() = #2019-01-23#"),
    ],
)

FUNCTIONS_DATE = [
    FUNCTION_DATEADD,
    FUNCTION_DATEPART,
    FUNCTION_DATEPART,
    FUNCTION_DATETRUNC,
    FUNCTION_SECOND,
    FUNCTION_MINUTE,
    FUNCTION_HOUR,
    FUNCTION_DAY,
    FUNCTION_MONTH,
    FUNCTION_QUARTER,
    FUNCTION_YEAR,
    FUNCTION_DAYOFWEEK,
    FUNCTION_WEEK,
    FUNCTION_NOW,
    FUNCTION_TODAY,
]
