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
        "The {arg:3} argument sets the offset in units of the {arg:2} argument. "
        "Set as an integer. It may take negative values. The default value is `1`.\n"
        "The {arg:2} argument sets the unit for {arg:3}. It may take the following values:\n"
        '- `"year"`;\n'
        '- `"month"`;\n'
        '- `"week"`;\n'
        '- `"day"` (default value);\n'
        '- `"hour"`;\n'
        '- `"minute"`;\n'
        '- `"second"`.\n'
        "\n"
        "Can also be used as `AGO( measure, date_dimension, number )`. In this case, "
        'the {arg:2} argument takes the `"day"` value.\n'
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
        "Re-evaluate {arg:0} for a date/time specified by {arg:2}. It allows to "
        "get the measure at the beginning and at the end of a period, or for the specified date.\n"
        "The {arg:1} argument is the dimension along which the offset is made.\n"
        "\n"
        "You can use the following as the {arg:2} argument:\n"
        "\n"
        "* Certain date.\n"
        "* Function {ref:TODAY} to obtain the current date.\n"
        "* Functions to calculate date and time.\n"
        "\n"
        "See also {ref:AGO}, {ref:LAG}."
    ),
    notes=_NOTES_TIME_SERIES,
    examples=[
        SimpleExample("AT_DATE(SUM([Sales]), [Order Date], #2019-01-01#)"),
        SimpleExample("AT_DATE(SUM([Sales]), [Order Date], TODAY())"),
        SimpleExample('AT_DATE(SUM([Sales]), [Order Date], DATETRUNC([Order Date], "month"))'),
    ],
)

FUNCTIONS_TIME_SERIES = [
    FUNCTION_AGO,
    FUNCTION_AT_DATE,
]
