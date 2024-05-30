from typing import MutableMapping

import attr

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.definitions.functions_type import (
    DataTypeSpec,
    WhitelistTypeSpec,
)
from dl_formula_ref.categories.type_conversion import CATEGORY_TYPE_CONVERSION
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedResource,
    AliasedResourceRegistryBase,
    AliasedTableResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample
from dl_formula_ref.registry.note import (
    Note,
    NoteLevel,
)
from dl_i18n.localizer_base import Translatable as BaseTranslatable


_ = get_gettext()


FUNCTION_DATE = FunctionDocRegistryItem(
    name="date",
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
            # FIXME: Connectorize dialect mentions (https://github.com/datalens-tech/datalens-backend/issues/81)
            Translatable("Argument {arg:1} is available only for {dialects:CLICKHOUSE} " "sources."),
        ),
    ],
    examples=[
        SimpleExample('DATE("2019-01-23") = #2019-01-23#'),
    ],
)

FUNCTION_DATETIME = FunctionDocRegistryItem(
    name="datetime",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to date and time format. When converting "
        "`Date` to `DateTime`, the time is set to '00:00:00'.\n"
        "The date must be in the format `YYYY-MM-DDThh:mm:ss` or `YYYY-MM-DD hh:mm:"
        "ss`.\n"
        "Numeric values are rendered as time in {link: unix_ts: Unix time} format "
        "or equal to the number of seconds elapsed since 00:00:00 on January 1, 1970, "
        "less the adjustments for leap seconds.\n"
        "\n"
        "The date and time can be converted to the "
        "specified {link: timezone_link: time zone} when the {arg:1} option is available. "
        "The {arg:1} parameter must be specified in `Region/Data_Zone` format."
    ),
    notes=[
        Note(
            Translatable("Argument {arg:1} is available only for {dialects:CLICKHOUSE} " "sources."),
        ),
    ],
    examples=[
        SimpleExample('DATETIME("2019-01-23 15:07:47") = #2019-01-23 15:07:47#'),
        SimpleExample('DATETIME("2019.01.02 03:04:05") = #2019-01-02 03:04:05#'),
        SimpleExample('DATETIME("2019-01-23") = #2019-01-23 00:00:00#'),
        SimpleExample("DATETIME(1576807650) = #2019-12-20 02:07:30#"),
        SimpleExample("DATETIME(1576807650.793) = #2019-12-20 02:07:30#"),
        SimpleExample("DATETIME(1576807650.793, 'Asia/Hong_Kong') = #2019-12-20 10:07:30#"),
    ],
)


FUNCTION_FLOAT = FunctionDocRegistryItem(
    name="float",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to fractional number format according to the "
        "following rules:\n"
        "\n"
        "{table: conversion}"
    ),
    resources=SimpleAliasedResourceRegistry(
        resources={
            "conversion": AliasedTableResource(
                table_body=[
                    [_("Type"), _("Value")],
                    ["{type:INTEGER|FLOAT}", _("Original value.")],
                    [
                        "{type:DATE|DATETIME|GENERICDATETIME}",
                        _(
                            "{link: unix_ts: Unix time} corresponding to the date and time. If the value "
                            "contains time zone data, it's used in the calculation. If the time zone is "
                            "unknown, the time is set in UTC."
                        ),
                    ],
                    ["{type:STRING}", _("A number from a decimal string.")],
                    ["{type:BOOLEAN}", _("`TRUE` — `1.0`, `FALSE` — `0.0`.")],
                ]
            ),
        }
    ),
    examples=[
        SimpleExample("FLOAT(7) = 7.0"),
        SimpleExample('FLOAT("34.567") = 34.567'),
        SimpleExample("FLOAT(TRUE) = 1.0"),
    ],
)

FUNCTION_INT = FunctionDocRegistryItem(
    name="int",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to integer format according to the following "
        "rules:\n"
        "\n"
        "{table: conversion}"
    ),
    resources=SimpleAliasedResourceRegistry(
        resources={
            "conversion": AliasedTableResource(
                table_body=[
                    [_("Type"), _("Value")],
                    ["{type:INTEGER}", _("Original value.")],
                    ["{type:FLOAT}", _("Integer part of the number (rounded down).")],
                    [
                        "{type:DATE|DATETIME|GENERICDATETIME}",
                        _(
                            "{link: unix_ts: Unix time} corresponding to the date and time. If the value "
                            "contains time zone data, it's used in the calculation. If the time zone is "
                            "unknown, the time is set in UTC."
                        ),
                    ],
                    ["{type:STRING}", _("A number from a decimal string.")],
                    ["{type:BOOLEAN}", _("`TRUE` — `1`, `FALSE` — `0`.")],
                ]
            ),
        }
    ),
    examples=[
        SimpleExample("INT(7.7) = 7"),
        SimpleExample('INT("365") = 365'),
        SimpleExample("INT(TRUE) = 1"),
    ],
)

FUNCTION_BOOL = FunctionDocRegistryItem(
    name="bool",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to Boolean type according to the following "
        "rules:\n"
        "\n"
        "{table:conversion}"
    ),
    resources=SimpleAliasedResourceRegistry(
        resources={
            "conversion": AliasedTableResource(
                table_body=[
                    [_("Type"), "`FALSE`", "`TRUE`"],
                    ["{type:INTEGER|FLOAT}", "`0`, `0.0`", _("All others")],
                    ["{type:STRING}", _('Empty string (`""`)'), _("All others")],
                    ["{type:BOOLEAN}", "`FALSE`", "`TRUE`"],
                    ["{type:DATE|DATETIME|GENERICDATETIME}", "-", "`TRUE`"],
                ]
            ),
        }
    ),
    examples=[
        SimpleExample("BOOL(0) = FALSE"),
        SimpleExample("BOOL(#2019-04-04#) = TRUE"),
        SimpleExample('BOOL("Lorem ipsum") = TRUE'),
    ],
)

FUNCTION_STR = FunctionDocRegistryItem(
    name="str",
    category=CATEGORY_TYPE_CONVERSION,
    description=_("Converts the {arg:0} expression to string type."),
    examples=[
        SimpleExample('STR(77) = "77"'),
        SimpleExample('STR(1 != 1) = "False"'),
        SimpleExample('STR(#2019-01-01#) = "2019-01-01"'),
        SimpleExample("STR(ARRAY(1, 2, 3, NULL)) = {1,2,3,NULL}  PostgreSQL"),
        SimpleExample("STR(ARRAY(1, 2, 3, NULL)) = [1,2,3,NULL]  ClickHouse"),
        SimpleExample("STR(ARRAY('a', 'b', '', NULL)) = {a,b," ",NULL}  PostgreSQL"),
        SimpleExample("STR(ARRAY('a', 'b', '', NULL)) = ['a','b','',NULL]  ClickHouse"),
    ],
    notes=[
        Note(
            Translatable(
                "If an array is passed (only for {dialects:CLICKHOUSE|POSTGRESQL} sources), the conversion is "
                "performed by a function in the source database and results may vary for different data sources. "
                "For consistent results use {ref:ARR_STR}."
            )
        ),
    ],
)

FUNCTION_DATETIME_PARSE = FunctionDocRegistryItem(
    name="datetime_parse",
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
    name="date_parse",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to date format. Unlike {ref: DATE}, it " "supports multiple formats."
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
    name="geopoint",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Generates a Geopoint type value. For the input, it accepts a string, a "
        '"geopoint" type value, or coordinates — latitude {arg:0} and longitude '
        "{arg:1}. If a single string is input, it must contain a list of two numbers "
        "(latitude and longitude) in JSON syntax."
    ),
    examples=[
        SimpleExample('GEOPOINT("[55.75222,37.61556]") = "[55.75222,37.61556]"'),
        SimpleExample('GEOPOINT(55.75222, 37.61556) = "[55.75222,37.61556]"'),
    ],
)

FUNCTION_GEOPOLYGON = FunctionDocRegistryItem(
    name="geopolygon",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to {link: geopolygon_link: geopolygon} format. "
        "At input, the function accepts strings in `[[[lat_1,lon_1], [lat_2,lon_2], ..., [lat_N-1,lon_N-1], [lat_N,lon_N]]]` format."
    ),
    examples=[
        SimpleExample(
            'GEOPOLYGON("[[[55.79421,37.65046],[55.79594,37.6513],[55.79642,37.65133],'
            '[55.7969, 37.65114],[55.79783, 37.65098],[55.78871,37.75101]]]")'
        ),
        SimpleExample(
            'GEOPOLYGON("[[[55.75,37.52],[55.75,37.68],[55.65,37.60]],' '[[55.79,37.60],[55.76,37.57],[55.76,37.63]]]")'
        ),
    ],
)


def _make_type_macro_from_dtype_spec(data_type_spec: DataTypeSpec) -> str:
    if isinstance(data_type_spec, DataType):
        return f"{{type:{data_type_spec.name}}}"
    elif isinstance(data_type_spec, tuple):
        return f'{{type:{"|".join([sub_data_type_spec.name for sub_data_type_spec in data_type_spec])}}}'
    else:
        raise TypeError(type(data_type_spec))


def _get_comment_for_type(dialect: DialectCombo, native_type_name: str) -> str | BaseTranslatable:
    return _DB_CAST_TYPE_COMMENTS.get((dialect, native_type_name), "")


@attr.s
class DbCastExtension:
    type_whitelists: dict[DialectCombo, dict[DataType, list[WhitelistTypeSpec]]] = attr.ib(kw_only=True, factory=dict)
    type_comments: dict[tuple[DialectCombo, str], str | BaseTranslatable] = attr.ib(kw_only=True, factory=dict)


_DB_CAST_TYPE_COMMENTS: dict[tuple[DialectCombo, str], str | BaseTranslatable] = {}
_DB_CAST_WHITELIST: dict[DialectCombo, dict[DataType, list[WhitelistTypeSpec]]] = {}


def register_db_cast_extension(extension: DbCastExtension) -> None:
    _DB_CAST_WHITELIST.update(extension.type_whitelists)
    _DB_CAST_TYPE_COMMENTS.update(extension.type_comments)


class DbCastWhiteListAliasedResourceRegistry(AliasedResourceRegistryBase):
    def get_resources(self) -> MutableMapping[str, AliasedResource]:
        resources = {
            "supported_native_types": AliasedTableResource(
                table_body=[
                    [
                        _("Data source"),
                        _("Data type"),
                        _("Native data type"),
                        _("Parameters for native type"),
                        _("Comment"),
                    ],
                    *[
                        [
                            f"{{dialects:{dialect.common_name.name}}}",
                            f"{{type:{data_type.name}}}",
                            f"`{spec.name}`",
                            f'{", ".join([_make_type_macro_from_dtype_spec(arg_type) for arg_type in spec.arg_types])}',
                            _get_comment_for_type(dialect=dialect, native_type_name=spec.name),
                        ]
                        for dialect, by_data_type in sorted(
                            _DB_CAST_WHITELIST.items(), key=lambda item: item[0].common_name.value
                        )
                        for data_type, spec_list in sorted(by_data_type.items(), key=lambda item: item[0].name)
                        for spec in spec_list
                    ],
                ]
            ),
        }
        return resources  # type: ignore  # 2024-01-24 # TODO: Incompatible return value type (got "dict[str, AliasedTableResource]", expected "MutableMapping[str, AliasedResource]")  [return-value]


FUNCTION_DB_CAST = FunctionDocRegistryItem(
    name="db_cast",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to database's native type {arg:1}.\n"
        "\n"
        "The following type casts are supported:\n"
        "\n"
        "{table:supported_native_types}\n"
    ),
    # Using a custom resource registry here
    # because it needs to read from global maps
    # that are filled from plugins after import time
    resources=DbCastWhiteListAliasedResourceRegistry(),
    examples=[
        SimpleExample('DB_CAST([float_value], "Decimal", 10, 5)'),
        SimpleExample('DB_CAST([float_value], "double precision")'),
        SimpleExample('DB_CAST([float_value], "numeric", 10, 5)'),
    ],
)

FUNCTION_TREE = FunctionDocRegistryItem(
    name="tree",
    category=CATEGORY_TYPE_CONVERSION,
    description=_(
        "Converts the {arg:0} expression to `Tree of strings` format. Can be used to create {link: tree_link: tree hierarchies}."
    ),
    examples=[
        SimpleExample("TREE(ARRAY([Country], [Region], [City]))"),
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
    FUNCTION_TREE,
]
