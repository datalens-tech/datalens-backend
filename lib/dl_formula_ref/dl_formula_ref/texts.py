from enum import Enum
from typing import NamedTuple

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D
from dl_formula_ref.localization import get_gettext
from dl_i18n.localizer_base import Translatable


_ = get_gettext()

EXAMPLE_TITLE = _("Example")
DOC_TOC_TITLE = _("Function Reference")
DOC_ALL_TITLE = _("All Functions")
DOC_OVERVIEW_TEXT = _("Overview")
DOC_AVAIL_TITLE = _("Function Availability")
COMMON_TYPE_NOTE = _("Arguments ({text:args}) must be of the same type.")
CONST_TYPE_NOTE = _("Only constant values are accepted for the arguments ({text:args}).")
ALSO_IN_OTHER_CATEGORIES = _("Function `{text:func_name}` is also found in the following categories: ")
DEPENDS_ON_ARGS = _("Depends on argument types")
FROM_ARGS = _("Same type as ({text:args})")
ANY_TYPE = _("Any")
HUMAN_CATEGORIES = {
    "aggregation": _("Aggregate functions"),
    "string": _("String functions"),
    "numeric": _("Mathematical functions"),
    "logical": _("Logical functions"),
    "date": _("Date/Time functions"),
    "type-conversion": _("Type conversion functions"),
    "operator": _("Operators"),
    "geographical": _("Geographical functions"),
    "markup": _("Text markup functions"),
    "window": _("Window functions"),
    "time-series": _("Time series functions"),
    "array": _("Array functions"),
}
FUNCTION_CATEGORY_TAG = {
    "aggregation": _("aggregation"),
    "string": _("string"),
    "numeric": _("mathematical"),
    "logical": _("logical"),
    "date": _("date/time"),
    "type-conversion": _("type conversion"),
    "operator": _("operator"),
    "geographical": _("geographical"),
    "markup": _("text markup"),
    "window": _("window"),
    "time-series": _("time series"),
    "array": _("array"),
}
HUMAN_DATA_TYPES = {
    DataType.INTEGER: _("Integer"),
    DataType.FLOAT: _("Fractional number"),
    DataType.BOOLEAN: _("Boolean"),
    DataType.STRING: _("String"),
    DataType.DATE: _("Date"),
    DataType.DATETIME: _("Datetime (deprecated)"),
    DataType.DATETIMETZ: _("Datetime with timezone"),
    DataType.GENERICDATETIME: _("Datetime"),
    DataType.GEOPOINT: _("Geopoint"),
    DataType.GEOPOLYGON: _("Geopolygon"),
    DataType.MARKUP: _("Markup"),
    DataType.UUID: _("UUID"),
    DataType.ARRAY_FLOAT: _("Array of fractional numbers"),
    DataType.ARRAY_INT: _("Array of integers"),
    DataType.ARRAY_STR: _("Array of strings"),
    DataType.TREE_STR: _("Tree"),
    (
        DataType.FLOAT,
        DataType.INTEGER,
        DataType.BOOLEAN,
        DataType.STRING,
        DataType.DATE,
        DataType.DATETIME,
        DataType.GENERICDATETIME,
        DataType.GEOPOINT,
        DataType.GEOPOLYGON,
        DataType.MARKUP,
        DataType.UUID,
        DataType.ARRAY_FLOAT,
        DataType.ARRAY_INT,
        DataType.ARRAY_STR,
    ): ANY_TYPE,  # TODO: remove after DATETIMETZ is fully adopted
    (
        DataType.FLOAT,
        DataType.INTEGER,
        DataType.BOOLEAN,
        DataType.STRING,
        DataType.DATE,
        DataType.DATETIME,
        DataType.DATETIMETZ,
        DataType.GENERICDATETIME,
        DataType.GEOPOINT,
        DataType.GEOPOLYGON,
        DataType.MARKUP,
        DataType.UUID,
        DataType.ARRAY_FLOAT,
        DataType.ARRAY_INT,
        DataType.ARRAY_STR,
        DataType.TREE_STR,
    ): ANY_TYPE,
}


class DialectStyle(Enum):
    simple = "simple"
    multiline = "multiline"
    split_version = "split_version"


class StyledDialect(NamedTuple):
    simple: str | Translatable
    multiline: str | Translatable
    split_version: str | Translatable

    def for_style(self, item):
        if isinstance(item, DialectStyle):
            item = item.name
        if isinstance(item, str):
            return getattr(self, item)


# These should be filled from plugins
HIDDEN_DIALECTS: set[DialectCombo] = set()
ANY_DIALECTS: set[DialectCombo] = set()


HUMAN_DIALECTS: dict[DialectCombo, StyledDialect] = {
    D.SQLITE: StyledDialect(  # TODO: remove
        "`SQLite`",
        "`SQLite`",
        "`SQLite`",
    ),
}

SIGNATURE_TITLE_STANDARD = _("Standard")
SIGNATURE_TITLE_EXTENDED = _("Extended")
SIGNATURE_DESC_EXTENDED_HEADER = _("More info:")


def register_any_dialects(any_dialects: frozenset[DialectCombo]) -> None:
    ANY_DIALECTS.update(any_dialects)


def register_human_dialects(human_dialects: dict[DialectCombo, StyledDialect]) -> None:
    HUMAN_DIALECTS.update(human_dialects)
