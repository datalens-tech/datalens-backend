from dl_formula_ref.categories.time_series import CATEGORY_TIME_SERIES
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample
from dl_formula_ref.registry.note import Note


_ = get_gettext()


_NOTES_TIME_SERIES = [
    Note(
        Translatable(
            "The first argument must be a measure (aggregated expression), otherwise an " "error will be raised."
        )
    ),
]

FUNCTION_AGO = FunctionDocRegistryItem(
    name="ago",
    category=CATEGORY_TIME_SERIES,
    description=_(
        "Re-evaluate {arg:0} for a date/time with a given offset.\n"
        "The {arg:1} argument is the dimension along which the offset is made.\n"
        "The {arg:3} argument is an integer. It can be negative.\n"
        "The {arg:2} argument takes the following values:\n"
        '- `"year"`;\n'
        '- `"month"`;\n'
        '- `"week"`;\n'
        '- `"day"`;\n'
        '- `"hour"`;\n'
        '- `"minute"`;\n'
        '- `"second"`.\n'
        "\n"
        "Can also be used as `AGO( measure, date_dimension, number )`. In this case, "
        "the third argument is interpreted as the number of days.\n"
        "\n"
        "See also {ref:AT_DATE}, {ref:LAG}."
    ),
    notes=_NOTES_TIME_SERIES,
    examples=[
        SimpleExample('AGO(SUM([Sales]), [Order Date], "month", 3)'),
        SimpleExample('AGO(SUM([Sales]), [Order Date], "year")'),
        SimpleExample("AGO(SUM([Sales]), [Order Date], 1)"),
    ],
)

FUNCTION_AT_DATE = FunctionDocRegistryItem(
    name="at_date",
    category=CATEGORY_TIME_SERIES,
    description=_(
        "Re-evaluate {arg:0} for a date/time specified by {arg:2}.\n"
        "The {arg:1} argument is the dimension along which the offset is made.\n"
        "\n"
        "See also {ref:AGO}, {ref:LAG}."
    ),
    notes=_NOTES_TIME_SERIES,
    examples=[
        SimpleExample("AT_DATE(SUM([Sales]), [Order Date], #2019-01-01#)"),
        SimpleExample('AT_DATE(SUM([Sales]), [Order Date], DATETRUNC([Order Date], "month"))'),
    ],
)

FUNCTIONS_TIME_SERIES = [
    FUNCTION_AGO,
    FUNCTION_AT_DATE,
]
