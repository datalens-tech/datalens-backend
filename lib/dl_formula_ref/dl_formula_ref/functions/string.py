from dl_formula.core.datatype import DataType
from dl_formula_ref.categories.string import CATEGORY_STRING
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample
from dl_formula_ref.registry.impl_selector import ArgAwareImplementationSelector
from dl_formula_ref.registry.naming import CategoryPostfixNamingProvider
from dl_formula_ref.registry.note import Note


_ = get_gettext()


FUNCTION_ASCII = FunctionDocRegistryItem(
    name="ascii",
    category=CATEGORY_STRING,
    description=_("Returns the numeric representation of the first character of the string."),
    examples=[
        SimpleExample('ASCII("N") = 78'),
        SimpleExample('ASCII("¹") = 185'),  # \u00b9 = \xb9 = 185;  utf-8 \xc2\xb9, i.e. 194 185
        SimpleExample('ASCII("¹") = 194'),  # binary utf-8 interpretation, e.g. mysql, clickhouse
        SimpleExample('ASCII("…") = 8230'),  # \u2026 = 8230;  utf-8 \xe2\x80\xa6 = 226 128 166
    ],
)

FUNCTION_CHAR = FunctionDocRegistryItem(
    name="char",
    category=CATEGORY_STRING,
    description=_("Converts the numeric representation of an ASCII character to a value."),
    examples=[
        SimpleExample('CHAR(78) = "N"'),
    ],
)

FUNCTION_CONCAT = FunctionDocRegistryItem(
    name="concat",
    category=CATEGORY_STRING,
    description=_(
        "Merges any number of strings. When non-string types are used, they're " "converted to strings and then merged."
    ),
    examples=[
        SimpleExample('CONCAT("Date of birth ", #2019-01-23#) = "Date of birth 2019-01-23"'),
        SimpleExample('CONCAT(2019, 01, 23) = "20190123"'),
    ],
)

FUNCTION_CONTAINS = FunctionDocRegistryItem(
    name="contains",
    category=CATEGORY_STRING,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {
                DataType.BOOLEAN,
                DataType.DATE,
                DataType.DATETIME,
                DataType.GENERICDATETIME,
                DataType.FLOAT,
                DataType.GEOPOINT,
                DataType.GEOPOLYGON,
                DataType.INTEGER,
                DataType.STRING,
                DataType.UUID,
            },
        },
    ),
    naming_provider=CategoryPostfixNamingProvider(),
    description=_("Returns `TRUE` if {arg:0} contains {arg:1}. For case-insensitive searches, " "see {ref:ICONTAINS}."),
    examples=[
        SimpleExample('CONTAINS("RU0891923", "RU") = TRUE'),
        SimpleExample('CONTAINS("Lorem ipsum", "abc") = FALSE'),
    ],
)

FUNCTION_ICONTAINS = FunctionDocRegistryItem(
    name="icontains",
    category=CATEGORY_STRING,
    description=_("Case-insensitive version of {ref:string/CONTAINS}. Returns `TRUE` if {arg:0} " "contains {arg:1}."),
    examples=[
        SimpleExample('ICONTAINS("RU0891923", "ru") = TRUE'),
        SimpleExample('ICONTAINS("Lorem ipsum", "abc") = FALSE'),
    ],
)

FUNCTION_ENDSWITH = FunctionDocRegistryItem(
    name="endswith",
    category=CATEGORY_STRING,
    description=_("Returns `TRUE` if {arg:0} ends in {arg:1}. For case-insensitive searches, " "see {ref:IENDSWITH}."),
    examples=[
        SimpleExample('ENDSWITH("Petrov Ivan", "Ivan") = TRUE'),
        SimpleExample('ENDSWITH("Lorem ipsum", "sum") = TRUE'),
        SimpleExample('ENDSWITH("Lorem ipsum", "abc") = FALSE'),
    ],
)

FUNCTION_IENDSWITH = FunctionDocRegistryItem(
    name="iendswith",
    category=CATEGORY_STRING,
    description=_("Case-insensitive version of {ref:ENDSWITH}. Returns `TRUE` if {arg:0} ends " "in {arg:1}."),
    examples=[
        SimpleExample('IENDSWITH("PETROV IVAN", "Ivan") = TRUE'),
        SimpleExample('IENDSWITH("Lorem ipsum", "SUM") = TRUE'),
        SimpleExample('IENDSWITH("Lorem ipsum", "abc") = FALSE'),
    ],
)

FUNCTION_STARTSWITH = FunctionDocRegistryItem(
    name="startswith",
    category=CATEGORY_STRING,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {
                DataType.BOOLEAN,
                DataType.DATE,
                DataType.DATETIME,
                DataType.GENERICDATETIME,
                DataType.FLOAT,
                DataType.GEOPOINT,
                DataType.GEOPOLYGON,
                DataType.INTEGER,
                DataType.STRING,
                DataType.UUID,
            },
        },
    ),
    naming_provider=CategoryPostfixNamingProvider(),
    description=_(
        "Returns `TRUE` if {arg:0} starts with {arg:1}. For case-insensitive " "searches, see {ref:ISTARTSWITH}."
    ),
    examples=[
        SimpleExample('STARTSWITH("Petrov Ivan", "Petrov") = TRUE'),
        SimpleExample('STARTSWITH("Lorem ipsum", "Lore") = TRUE'),
        SimpleExample('STARTSWITH("Lorem ipsum", "abc") = FALSE'),
    ],
)

FUNCTION_ISTARTSWITH = FunctionDocRegistryItem(
    name="istartswith",
    category=CATEGORY_STRING,
    description=_(
        "Case-insensitive version of {ref:string/STARTSWITH}. Returns `TRUE`  if {arg:0} " "starts with {arg:1}."
    ),
    examples=[
        SimpleExample('ISTARTSWITH("petrov ivan", "Petrov") = TRUE'),
        SimpleExample('ISTARTSWITH("Lorem ipsum", "LORE") = TRUE'),
        SimpleExample('ISTARTSWITH("Lorem ipsum", "abc") = FALSE'),
    ],
)

FUNCTION_FIND = FunctionDocRegistryItem(
    name="find",
    category=CATEGORY_STRING,
    description=_(
        "Returns the index of the position of the first character of the substring "
        "{arg:1} in the string {arg:0}.\n"
        "\n"
        "If the `start_index` option is specified, the search starts from the "
        "specified position."
    ),
    examples=[
        SimpleExample('FIND("Lorem ipsum dolor sit amet", "abc") = 0'),
        SimpleExample('FIND("Lorem ipsum dolor sit amet", "or") = 2'),
        SimpleExample('FIND("Lorem ipsum dolor sit amet", "or", 7) = 16'),
    ],
)

FUNCTION_LEFT = FunctionDocRegistryItem(
    name="left",
    category=CATEGORY_STRING,
    description=_(
        "Returns a string that contains the number of characters specified in {arg:1} "
        "from the beginning of the string {arg:0}."
    ),
    examples=[
        SimpleExample('LEFT("Computer", 4) = "Comp"'),
    ],
)

FUNCTION_RIGHT = FunctionDocRegistryItem(
    name="right",
    category=CATEGORY_STRING,
    description=_(
        "Returns a string that contains the number of characters specified in {arg:1} "
        "from the end of the string {arg:0}."
    ),
    examples=[
        SimpleExample('RIGHT("Computer", 3) = "ter"'),
    ],
)

FUNCTION_LEN = FunctionDocRegistryItem(
    name="len",
    category=CATEGORY_STRING,
    description=_("Returns the number of characters in the string or items in array {arg:0}."),
    examples=[
        SimpleExample('LEN("Computer") = 8'),
    ],
)

FUNCTION_LOWER = FunctionDocRegistryItem(
    name="lower",
    category=CATEGORY_STRING,
    description=_("Returns the string {arg:0} in lowercase."),
    examples=[
        SimpleExample('LOWER("Lorem ipsum") = "lorem ipsum"'),
        SimpleExample('LOWER("Карл у Клары") = "карл у клары"'),
    ],
)

FUNCTION_UPPER = FunctionDocRegistryItem(
    name="upper",
    category=CATEGORY_STRING,
    description=_("Returns the string {arg:0} in uppercase."),
    examples=[
        SimpleExample('UPPER("Lorem ipsum") = "LOREM IPSUM"'),
        SimpleExample('UPPER("Карл у Клары") = "КАРЛ У КЛАРЫ"'),
    ],
)

FUNCTION_LTRIM = FunctionDocRegistryItem(
    name="ltrim",
    category=CATEGORY_STRING,
    description=_("Returns the string {arg:0} without spaces at the beginning of the string."),
    examples=[
        SimpleExample('LTRIM(" Computer") = "Computer"'),
    ],
)

FUNCTION_RTRIM = FunctionDocRegistryItem(
    name="rtrim",
    category=CATEGORY_STRING,
    description=_("Returns the string {arg:0} without spaces at the end of the string."),
    examples=[
        SimpleExample('RTRIM("Computer ") = "Computer"'),
    ],
)

FUNCTION_TRIM = FunctionDocRegistryItem(
    name="trim",
    category=CATEGORY_STRING,
    description=_("Returns the string {arg:0} without spaces at the beginning or end of the " "string."),
    examples=[
        SimpleExample('TRIM(" Computer ") = "Computer"'),
    ],
)

FUNCTION_SUBSTR = FunctionDocRegistryItem(
    name="substr",
    category=CATEGORY_STRING,
    description=_(
        "Returns the substring {arg:0} starting from the index {arg:1}. The numbering starts with one.\n"
        "\n"
        "If an additional argument `{argn:2}` is specified, a substring of the "
        "specified length is returned."
    ),
    examples=[
        SimpleExample('SUBSTR("Computer", 3) = "mputer"'),
        SimpleExample('SUBSTR("Computer", 3, 2) = "mp"'),
    ],
)

_REGEXP_NOTES = [
    Note(
        Translatable(
            "See the documentation of the data source to clarify the regular expression syntax. "
            "For example, {dialects:CLICKHOUSE} uses the {link: ch_re_link: RE2 syntax} "
            "to compose regular expressions."
        )
    ),
]

FUNCTION_REGEXP_EXTRACT = FunctionDocRegistryItem(
    name="regexp_extract",
    category=CATEGORY_STRING,
    description=_("Returns the substring {arg:0} that matches the regular expression " "{arg:1}."),
    notes=_REGEXP_NOTES,
    examples=[
        SimpleExample('REGEXP_EXTRACT("RU 912873", "[A-Z]+\\s+(\\d+)") = "912873"'),
    ],
)

FUNCTION_REGEXP_EXTRACT_ALL = FunctionDocRegistryItem(
    name="regexp_extract_all",
    category=CATEGORY_STRING,
    description=_("Returns all substring {arg:0} that matches the regular expression for first subgroup" "{arg:1}."),
    notes=_REGEXP_NOTES,
    examples=[
        SimpleExample("REGEXP_EXTRACT_ALL('100-200, 300-400', '(\\d+)-(\\d+)') = ['100','300']"),
    ],
)

FUNCTION_REGEXP_EXTRACT_NTH = FunctionDocRegistryItem(
    name="regexp_extract_nth",
    category=CATEGORY_STRING,
    description=_(
        "Returns a substring {arg:0} that matches the regular expression pattern "
        "{arg:1} starting from the specified index."
    ),
    notes=_REGEXP_NOTES,
    examples=[
        SimpleExample('REGEXP_EXTRACT_NTH("RU 912 EN 873", "[A-Z]+\\s+(\\d+)", 2) = "873"'),
    ],
)

FUNCTION_REGEXP_MATCH = FunctionDocRegistryItem(
    name="regexp_match",
    category=CATEGORY_STRING,
    description=_(
        "Returns 'TRUE' if the string {arg:0} has a substring that matches the " "regular expression pattern {arg:1}."
    ),
    notes=_REGEXP_NOTES,
    examples=[
        SimpleExample('REGEXP_MATCH("RU 912873","\\w\\s\\d") = TRUE'),
    ],
)

FUNCTION_REGEXP_REPLACE = FunctionDocRegistryItem(
    name="regexp_replace",
    category=CATEGORY_STRING,
    description=_(
        "Searches for a substring in the string {arg:0} using the regular expression "
        "pattern {arg:1} and replaces it with the string {arg:2}.\n"
        "\n"
        "If the substring is not found, the string is not changed."
    ),
    notes=_REGEXP_NOTES,
    examples=[
        SimpleExample('REGEXP_REPLACE("123 456", "\\s", "-") = "123-456"'),
    ],
)

FUNCTION_REPLACE = FunctionDocRegistryItem(
    name="replace",
    category=CATEGORY_STRING,
    impl_selector=ArgAwareImplementationSelector(
        exp_arg_types={
            0: {DataType.STRING},
        },
    ),
    naming_provider=CategoryPostfixNamingProvider(),
    description=_(
        "Searches for the substring {arg:1} in the string {arg:0} and replaces it "
        "with the string {arg:2}.\n"
        "\n"
        "If the substring is not found, the string is not changed."
    ),
    examples=[
        SimpleExample('REPLACE("350 RUB", "RUB", "USD") = "350 USD"'),
    ],
)

FUNCTION_SPACE = FunctionDocRegistryItem(
    name="space",
    category=CATEGORY_STRING,
    description=_("Returns a string with the specified number of spaces."),
    examples=[
        SimpleExample('SPACE(5) = "     "'),
    ],
)

FUNCTION_SPLIT = FunctionDocRegistryItem(
    name="split",
    category=CATEGORY_STRING,
    # FIXME: Connectorize dialect mentions (https://github.com/datalens-tech/datalens-backend/issues/81)
    description=_(
        "It splits {arg:0} into a sequence of substrings using the {arg:1} "
        "character as separator and returns the substring whose number is equal "
        "to the {arg:2} parameter. By default, the delimiting character is comma. "
        "If {arg:2} is negative, the substring to return is counted from "
        "the end of {arg:0}. If the number of substrings is less than "
        "the {arg:2} {link: abs_value_link: absolute value}, the function "
        "returns an empty string. If {arg:2} was not provided, the function "
        "returns an array of the substrings (only for {dialects:CLICKHOUSE|POSTGRESQL} sources)."
    ),
    examples=[
        SimpleExample('SPLIT("192.168.0.1", ".", 1) = "192"'),
        SimpleExample('SPLIT("192.168.0.1", ".", -1) = "1"'),
        SimpleExample('SPLIT("192.168.0.1", ".", 5) = ""'),
        SimpleExample("SPLIT(\"192.168.0.1\", \".\") = \"['192 ','168 ','0 ','1']\""),
        SimpleExample('SPLIT("192.168.0.1") = "192.168.0.1"'),
        SimpleExample("SPLIT(\"a,b,c,d\") = \"['a','b','c','d']\""),
    ],
)

FUNCTION_UTF8 = FunctionDocRegistryItem(
    name="utf8",
    category=CATEGORY_STRING,
    description=_("Converts the {arg:0} string encoding to `UTF8`."),
    examples=[
        SimpleExample('UTF8([Name], "CP-1251")'),
    ],
)

FUNCTIONS_STRING = [
    FUNCTION_ASCII,
    FUNCTION_CHAR,
    FUNCTION_CONCAT,
    FUNCTION_CONTAINS,
    FUNCTION_ICONTAINS,
    FUNCTION_ENDSWITH,
    FUNCTION_IENDSWITH,
    FUNCTION_STARTSWITH,
    FUNCTION_ISTARTSWITH,
    FUNCTION_FIND,
    FUNCTION_LEFT,
    FUNCTION_RIGHT,
    FUNCTION_LEN,
    FUNCTION_LOWER,
    FUNCTION_UPPER,
    FUNCTION_LTRIM,
    FUNCTION_RTRIM,
    FUNCTION_TRIM,
    FUNCTION_SUBSTR,
    FUNCTION_REGEXP_EXTRACT,
    FUNCTION_REGEXP_EXTRACT_ALL,
    FUNCTION_REGEXP_EXTRACT_NTH,
    FUNCTION_REGEXP_MATCH,
    FUNCTION_REGEXP_REPLACE,
    FUNCTION_REPLACE,
    FUNCTION_SPACE,
    FUNCTION_SPLIT,
    FUNCTION_UTF8,
]
