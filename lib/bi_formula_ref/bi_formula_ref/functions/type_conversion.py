from bi_formula_ref.registry.base import FunctionDocRegistryItem
from bi_formula_ref.registry.example import SimpleExample
from bi_formula_ref.registry.aliased_res import (
    AliasedResourceRegistry, AliasedTableResource,
)
from bi_formula_ref.categories.type_conversion import CATEGORY_TYPE_CONVERSION
from bi_formula_ref.registry.note import Note, NoteLevel
from bi_formula.core.dialect import DialectCombo
from bi_formula.core.datatype import DataType
from bi_formula.definitions.functions_type import DataTypeSpec
from bi_formula_ref.localization import get_gettext
from bi_connector_postgresql.formula.constants import PostgreSQLDialect
from bi_connector_postgresql.formula.definitions.functions_type import FuncDbCastPostgreSQLBase
from bi_connector_clickhouse.formula.definitions.functions_type import FuncDbCastClickHouseBase


_ = get_gettext()


FUNCTION_DATE = FunctionDocRegistryItem(
    name='date',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to date format.\n"
        "\n"
        "The date must be in the format `YYYY-MM-DD`.\n"
        "\n"
        "If {arg:0} is a number, then the {arg:1} option can be used to convert the "
        "date to the specified time zone."
    ),
    notes=[
        Note(
            _(
                "Argument {arg:1} is available only for {dialects:CLICKHOUSE} "
                "sources."
            ),
        ),
        Note(
            _(
                "For {dialects:CLICKHOUSE} data sources, numeric {arg:0} values less than or "
                "equal to `65535` are interpreted as the number of days (not seconds, like in "
                "all other cases) since January 1st 1970. This is the result of the behavior "
                "of available {dialects:CLICKHOUSE} functions.\n"
                "\n"
                "One way to surpass this is to use the following formula: "
                "`DATE(DATETIME([value]))`. The result is more consistent, but is likely to "
                "be much slower."
            ),
            level=NoteLevel.warning
        ),
    ],
    examples=[
        SimpleExample('DATE("2019-01-23") = #2019-01-23#'),
    ],
)

FUNCTION_DATETIME = FunctionDocRegistryItem(
    name='datetime',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to date and time format. When converting "
        "`Date` to `DateTime`, the time is set to '00:00:00'.\n"
        "The date must be in the format `YYYY-MM-DDThh:mm:ss` or `YYYY-MM-DD hh:mm:"
        "ss`.\n"
        "\n"
        "The date and time can be converted to the "
        "specified time zone when the {arg:1} option is available."
    ),
    notes=[
        Note(
            _(
                "Argument {arg:1} is available only for {dialects:CLICKHOUSE} "
                "sources."
            ),
        ),
    ],
    examples=[
        SimpleExample('DATETIME("2019-01-23 15:07:47") = #2019-01-23 15:07:47#'),
    ],
)


FUNCTION_FLOAT = FunctionDocRegistryItem(
    name='float',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to fractional number format according to the "
        "following rules:\n"
        "\n"
        "{table: conversion}"
    ),
    resources=AliasedResourceRegistry(resources={
        'conversion': AliasedTableResource(table_body=[
            [_('Type'), _('Value')],
            ['{type:INTEGER|FLOAT}', _("Original value.")],
            ['{type:DATE|DATETIME|GENERICDATETIME}', _(
                "{link: unix_ts: Unix time} corresponding to the date and time. If the value "
                "contains time zone data, it's used in the calculation. If the time zone is "
                "unknown, the time is set in UTC."
            )],
            ['{type:STRING}', _("A number from a decimal string.")],
            ['{type:BOOLEAN}', _('`TRUE` — `1.0`, `FALSE` — `0.0`.')],
        ]),
    }),
    examples=[
        SimpleExample('FLOAT(7) = 7.0'),
        SimpleExample('FLOAT("34.567") = 34.567'),
        SimpleExample('FLOAT(TRUE) = 1.0'),
    ],
)

FUNCTION_INT = FunctionDocRegistryItem(
    name='int',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to integer format according to the following "
        "rules:\n"
        "\n"
        "{table: conversion}"
    ),
    resources=AliasedResourceRegistry(resources={
        'conversion': AliasedTableResource(table_body=[
            [_('Type'), _('Value')],
            ['{type:INTEGER}', _('Original value.')],
            ['{type:FLOAT}', _("Integer part of the number (rounded down).")],
            ['{type:DATE|DATETIME|GENERICDATETIME}', _(
                "{link: unix_ts: Unix time} corresponding to the date and time. If the value "
                "contains time zone data, it's used in the calculation. If the time zone is "
                "unknown, the time is set in UTC."
            )],
            ['{type:STRING}', _('A number from a decimal string.')],
            ['{type:BOOLEAN}', _('`TRUE` — `1`, `FALSE` — `0`.')],
        ]),
    }),
    examples=[
        SimpleExample('INT(7.7) = 7'),
        SimpleExample('INT("365") = 365'),
        SimpleExample('INT(TRUE) = 1'),
    ],
)

FUNCTION_BOOL = FunctionDocRegistryItem(
    name='bool',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to Boolean type according to the following "
        "rules:\n"
        "\n"
        "{table:conversion}"
    ),
    resources=AliasedResourceRegistry(resources={
        'conversion': AliasedTableResource(table_body=[
            [_('Type'), '`FALSE`', '`TRUE`'],
            ['{type:INTEGER|FLOAT}', '`0`, `0.0`', _('All others')],
            ['{type:STRING}', _('Empty string (`""`)'), _('All others')],
            ['{type:BOOLEAN}', '`FALSE`', '`TRUE`'],
            ['{type:DATE|DATETIME|GENERICDATETIME}', '-', '`TRUE`'],
        ]),
    }),
    examples=[
        SimpleExample('BOOL(0) = FALSE'),
        SimpleExample('BOOL(#2019-04-04#) = TRUE'),
        SimpleExample('BOOL("Lorem ipsum") = TRUE'),
    ],
)

FUNCTION_STR = FunctionDocRegistryItem(
    name='str',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        'Converts the {arg:0} expression to string type.'
    ),
    examples=[
        SimpleExample('STR(77) = "77"'),
        SimpleExample('STR(1 != 1) = "False"'),
        SimpleExample('STR(#2019-01-01#) = "2019-01-01"'),
        SimpleExample('STR(ARRAY(1, 2, 3, NULL)) = {1,2,3,NULL}  PostgreSQL'),
        SimpleExample('STR(ARRAY(1, 2, 3, NULL)) = [1,2,3,NULL]  ClickHouse'),
        SimpleExample("STR(ARRAY('a', 'b', '', NULL)) = {a,b,"",NULL}  PostgreSQL"),
        SimpleExample("STR(ARRAY('a', 'b', '', NULL)) = ['a','b','',NULL]  ClickHouse"),
    ],
    notes=[
        Note(_(
            "If an array is passed (only for {dialects:CLICKHOUSE|POSTGRESQL} sources), the conversion is "
            "performed by a function in the source database and results may vary for different data sources. "
            "For consistent results use {ref:ARR_STR}."
        )),
    ],
)

FUNCTION_DATETIME_PARSE = FunctionDocRegistryItem(
    name='datetime_parse',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to date and time format. Unlike {ref: "
        "DATETIME}, it supports multiple formats."
    ),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            'DATETIME_PARSE("2019-01-02 03:04:05") = #2019-01-02 03:04:05#',
            'DATETIME_PARSE("20190102030405") = #2019-01-02 03:04:05#',
            'DATETIME_PARSE("20190102 030405") = #2019-01-02 03:04:05#',
            'DATETIME_PARSE("2019.01.02 03:04:05") = #2019-01-02 03:04:05#',
            'DATETIME_PARSE("02/01/2019 03:04:05") = #2019-01-02 03:04:05#',
            'DATETIME_PARSE("2019-01-02 03:04") = #2019-01-02 03:04:00#',
            'DATETIME_PARSE("2019-01-02 030405") = #2019-01-02 03:04:05#',
            'DATETIME_PARSE("2019 Jan 02 03:04:05") = #2019-01-02 03:04:05#',
        )
    ],
)

FUNCTION_DATE_PARSE = FunctionDocRegistryItem(
    name='date_parse',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to date format. Unlike {ref: DATE}, it "
        "supports multiple formats."
    ),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            'DATE_PARSE("2019-01-02 03:04:05") = #2019-01-02#',
            'DATE_PARSE("2019-01-02") = #2019-01-02#',
            'DATE_PARSE("20190102") = #2019-01-02#',
            'DATE_PARSE("2019.01.02") = #2019-01-02#',
            'DATE_PARSE("02/01/2019") = #2019-01-02#',
            'DATE_PARSE("02/01/19") = #2019-01-02#',
            'DATE_PARSE("2019 Jan 02") = #2019-01-02#',
            'DATE_PARSE("2019 Jan") = #2019-01-01#',
            'DATE_PARSE("201901") = #2019-01-01#',
            'DATE_PARSE("2019") = #2019-01-01#',
        )
    ],
)

FUNCTION_GEOPOINT = FunctionDocRegistryItem(
    name='geopoint',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Generates a Geopoint type value. For the input, it accepts a string, a "
        "\"geopoint\" type value, or coordinates — latitude {arg:0} and longitude "
        "{arg:1}. If a single string is input, it must contain a list of two numbers "
        "(latitude and longitude) in JSON syntax."
    ),
    examples=[
        SimpleExample('GEOPOINT("[55.75222,37.61556]") = "[55.75222,37.61556]"'),
        SimpleExample('GEOPOINT(55.75222, 37.61556) = "[55.75222,37.61556]"'),
    ],
)

FUNCTION_GEOPOLYGON = FunctionDocRegistryItem(
    name='geopolygon',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        'Converts the {arg:0} expression to geopolygon format.'
    ),
    examples=[
        SimpleExample(
            'GEOPOLYGON("[[[55.79421,37.65046],[55.79594,37.6513],[55.79642,37.65133],'
            '[55.7969, 37.65114],[55.79783, 37.65098],[55.78871,37.75101]]]")'
        ),
        SimpleExample(
            'GEOPOLYGON("[[[55.75,37.52],[55.75,37.68],[55.65,37.60]],'
            '[[55.79,37.60],[55.76,37.57],[55.76,37.63]]]")'
        ),
    ],
)


def _make_type_macro_from_dtype_spec(data_type_spec: DataTypeSpec) -> str:
    if isinstance(data_type_spec, DataType):
        return f'{{type:{data_type_spec.name}}}'
    elif isinstance(data_type_spec, tuple):
        return f'{{type:{"|".join([sub_data_type_spec.name for sub_data_type_spec in data_type_spec])}}}'
    else:
        raise TypeError(type(data_type_spec))


def _get_comment_for_type(dialect: DialectCombo, native_type_name: str) -> str:
    return _DB_CAST_TYPE_COMMENTS.get((dialect, native_type_name), '')


_DB_CAST_TYPE_COMMENTS: dict[tuple[DialectCombo, str], str] = {
    (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, 'character'): _('Alias: `char`'),
    (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, 'character varying'): _('Alias: `varchar`'),
    (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, 'char'): _('Alias for `character`'),
    (PostgreSQLDialect.NON_COMPENG_POSTGRESQL, 'varchar'): _('Alias for `character varying`'),
}

_DB_CAST_WHITELIST = {
    # FIXME: Find a non-hard-code way to do this
    **FuncDbCastClickHouseBase.WHITELISTS,
    **FuncDbCastPostgreSQLBase.WHITELISTS,
}


FUNCTION_DB_CAST = FunctionDocRegistryItem(
    name='db_cast',
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to database's native type {arg:1}.\n"
        "\n"
        "The following type casts are supported:\n"
        "\n"
        "{table:supported_native_types}\n"
    ),
    resources=AliasedResourceRegistry(resources={
        'supported_native_types': AliasedTableResource(table_body=[
            [_('Data source'), _('Data type'), _('Native data type'), _('Parameters for native type'), _('Comment')],
            *[
                [
                    f'{{dialects:{dialect.common_name.name}}}',
                    f'{{type:{data_type.name}}}',
                    f'`{spec.name}`',
                    f'{", ".join([_make_type_macro_from_dtype_spec(arg_type) for arg_type in spec.arg_types])}',
                    _get_comment_for_type(dialect=dialect, native_type_name=spec.name),
                ]
                for dialect, by_data_type in sorted(
                    _DB_CAST_WHITELIST.items(),
                    key=lambda item: item[0].common_name.value
                )
                if dialect != PostgreSQLDialect.COMPENG
                for data_type, spec_list in sorted(by_data_type.items(), key=lambda item: item[0].name)
                for spec in spec_list
            ]
        ]),
    }),
    examples=[
        SimpleExample('DB_CAST([float_value], "Decimal", 10, 5)'),
        SimpleExample('DB_CAST([float_value], "double precision")'),
        SimpleExample('DB_CAST([float_value], "numeric", 10, 5)'),
    ],
)

FUNCTIONS_TYPE_CONVERSION = [
    FUNCTION_DATE,
    FUNCTION_DATETIME,
    FUNCTION_FLOAT,
    FUNCTION_INT,
    FUNCTION_BOOL,
    FUNCTION_STR,
    FUNCTION_DATETIME_PARSE,
    FUNCTION_DATE_PARSE,
    FUNCTION_GEOPOINT,
    FUNCTION_GEOPOLYGON,
    FUNCTION_DB_CAST,
]
