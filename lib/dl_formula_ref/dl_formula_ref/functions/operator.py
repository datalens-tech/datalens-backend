from dl_formula_ref.categories.operator import CATEGORY_OPERATOR
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedTableResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample
from dl_formula_ref.registry.naming import CustomNamingProvider
from dl_formula_ref.registry.note import (
    Note,
    NoteLevel,
)
from dl_formula_ref.registry.signature_gen import (
    SignatureTemplate,
    TemplatedSignatureGenerator,
)


_ = get_gettext()


_SIGNATURE_OP_UNARY = TemplatedSignatureGenerator(signature_templates=(SignatureTemplate(body="{0} {1}"),))
_SIGNATURE_OP_BINARY = TemplatedSignatureGenerator(signature_templates=(SignatureTemplate(body="{1} {0} {2}"),))


FUNCTION_OP_NOT = FunctionDocRegistryItem(
    name="not",
    naming_provider=CustomNamingProvider(
        internal_name="op_not",
    ),
    category=CATEGORY_OPERATOR,
    description=_("Inverts a Boolean value."),
    signature_gen=_SIGNATURE_OP_UNARY,
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "NOT FALSE = TRUE",
            "NOT TRUE = FALSE",
            'NOT "" = TRUE',
            'NOT "text" = FALSE',
            "NOT 0 = TRUE",
            "NOT 1 = FALSE",
            "NOT #2019-01-01# = FALSE",
            "NOT #2019-01-01 03:00:00# = FALSE",
        )
    ],
)

FUNCTION_OP_NEGATION = FunctionDocRegistryItem(
    name="neg",
    naming_provider=CustomNamingProvider(
        title=_("Negation (-)"),
        internal_name="op_negation",
    ),
    category=CATEGORY_OPERATOR,
    description=_("Returns the number {arg:0} with the opposite sign."),
    signature_gen=TemplatedSignatureGenerator(signature_templates=(SignatureTemplate(body="-{1}"),)),
    examples=[
        SimpleExample("- (5) = -5"),
    ],
)

FUNCTION_OP_IS_TRUE = FunctionDocRegistryItem(
    name="istrue",
    naming_provider=CustomNamingProvider(
        title="IS TRUE",  # Not translatable
    ),
    category=CATEGORY_OPERATOR,
    description=_(
        "Checks whether the value of {arg:0} is true (`TRUE`).\n"
        "\n"
        "The `{argn:0} IS NOT TRUE` option returns the opposite value."
    ),
    signature_gen=TemplatedSignatureGenerator(signature_templates=(SignatureTemplate(body="{1} IS [ NOT ] TRUE"),)),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            '"qwerty" IS TRUE = TRUE',
            "123 IS TRUE = TRUE",
            "TRUE IS TRUE = TRUE",
            "FALSE IS TRUE = FALSE",
            "FALSE IS NOT TRUE = TRUE",
            "#2019-03-05# IS TRUE = TRUE",
            "#2019-03-05 01:02:03# IS TRUE = TRUE",
        )
    ],
)

FUNCTION_OP_IS_FALSE = FunctionDocRegistryItem(
    name="isfalse",
    naming_provider=CustomNamingProvider(
        title="IS FALSE",  # Not translatable
    ),
    category=CATEGORY_OPERATOR,
    description=_(
        "Checks whether the {arg:0} value is false (`FALSE`).\n"
        "\n"
        "The `{argn:0} IS NOT FALSE` option returns the opposite value."
    ),
    signature_gen=TemplatedSignatureGenerator(signature_templates=(SignatureTemplate(body="{1} IS [ NOT ] FALSE"),)),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            '"" IS FALSE = TRUE',
            "0 IS FALSE = TRUE",
            "FALSE IS FALSE = TRUE",
            "TRUE IS FALSE = FALSE",
            "TRUE IS NOT FALSE = TRUE",
            "#2019-03-05# IS FALSE = FALSE",
            "#2019-03-05 01:02:03# IS FALSE = FALSE",
        )
    ],
)

FUNCTION_OP_POWER = FunctionDocRegistryItem(
    name="^",
    naming_provider=CustomNamingProvider(
        title=_("Power (^)"),
        internal_name="op_power",
    ),
    category=CATEGORY_OPERATOR,
    description=_("Raises {arg:0} to the power of {arg:1}."),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample("2 ^ 3 = 8.0"),
        SimpleExample("2.1 ^ -0.3 = 0.80045"),
    ],
)

FUNCTION_OP_MULT = FunctionDocRegistryItem(
    name="*",
    naming_provider=CustomNamingProvider(
        title=_("Multiplication (*)"),
        internal_name="op_mult",
    ),
    category=CATEGORY_OPERATOR,
    description=_(
        "If both arguments are numbers, it returns the result by multiplying {arg:0} "
        "by {arg:1}.\n"
        "\n"
        "If one of the arguments is {type:STRING} and the other is {type:INTEGER}, it "
        "returns the string repeated the specified number of times."
    ),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample("2 * 3 = 6"),
        SimpleExample("2.1 * 3 = 6.3"),
        SimpleExample("\"Lorem\" * 3 = 'LoremLoremLorem'"),
        SimpleExample("3 * \"Lorem\" = 'LoremLoremLorem'"),
    ],
)

FUNCTION_OP_DIV = FunctionDocRegistryItem(
    name="/",
    naming_provider=CustomNamingProvider(
        title=_("Division (/)"),
        internal_name="op_div",
    ),
    category=CATEGORY_OPERATOR,
    description=_("Divides the number {arg:0} by the number {arg:1}."),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample("4 / 2 = 2.0"),
        SimpleExample("5 / 2 = 2.5"),
        SimpleExample("5.0 / 2 = 2.5"),
        SimpleExample("5 / 2.0 = 2.5"),
    ],
)

FUNCTION_OP_MOD = FunctionDocRegistryItem(
    name="%",
    naming_provider=CustomNamingProvider(
        title=_("Modulo (%)"),
        internal_name="op_mod",
    ),
    category=CATEGORY_OPERATOR,
    description=_("Returns the remainder from dividing the first number {arg:0} by the second " "number {arg:1}."),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample("2 % 3 = 2"),
        SimpleExample("2.1 % 3 = 2.1"),
        SimpleExample("3 % 2.1 = 0.9"),
    ],
)

FUNCTION_OP_PLUS = FunctionDocRegistryItem(
    name="+",
    naming_provider=CustomNamingProvider(
        title=_("Addition and concatenation (+)"),
        internal_name="op_plus",
    ),
    category=CATEGORY_OPERATOR,
    description=_(
        "Behaves differently depending on the argument types. Possible options are "
        "listed in the table:\n"
        "\n"
        "{table: desc_by_type}\n"
        "\n"
        "Changing the order of arguments does not affect the result."
    ),
    resources=SimpleAliasedResourceRegistry(
        {
            "desc_by_type": AliasedTableResource(
                table_body=[
                    [_("Type of {arg:0}"), _("Type of {arg:1}"), _("Return value")],
                    ["{type:FLOAT|INTEGER}", "{type:FLOAT|INTEGER}", _("The sum of the numbers {arg:0} and {arg:1}.")],
                    [
                        "{type:DATE}",
                        "{type:FLOAT|INTEGER}",
                        _(
                            "The date that is {arg:1} days greater than {arg:0} (rounded down to an "
                            "integer number of days)."
                        ),
                    ],
                    [
                        "{type:DATETIME|GENERICDATETIME}",
                        "{type:FLOAT|INTEGER}",
                        _(
                            "The date with time, {arg:1} days greater than {arg:0}. If {arg:1} contains a "
                            "fractional part, it is converted hours (`1/24`), minutes (`1/1440`), and "
                            "seconds (`1/86400`)."
                        ),
                    ],
                    [
                        "{type:STRING}",
                        "{type:STRING}",
                        _("The merging (concatenation) of strings {arg:0} and {arg:1}."),
                    ],
                    [
                        "{type:ARRAY_INT|ARRAY_FLOAT|ARRAY_STR}",
                        "{type:ARRAY_INT|ARRAY_FLOAT|ARRAY_STR}",
                        _("The merging (concatenation) of arrays {arg:0} and {arg:1}."),
                    ],
                ]
            )
        }
    ),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "2 + 3 = 5",
            '"Lorem" + " ipsum" = "Lorem ipsum"',
            "#2019-01-06# + 2 = #2019-01-08#",
            "2.2 + #2019-01-06# = #2019-01-08#",
            "2 + #2019-01-06 03# = #2019-01-08 03:00:00#",
            "#2019-01-06 03# + 2.5 = #2019-01-08 15:00:00#",
        )
    ],
)

FUNCTION_OP_MINUS = FunctionDocRegistryItem(
    name="-",
    naming_provider=CustomNamingProvider(
        title=_("Subtraction (-)"),
        internal_name="op_minus",
    ),
    category=CATEGORY_OPERATOR,
    description=_(
        "Behaves differently depending on the argument types. Possible options are "
        "listed in the table:\n"
        "\n"
        "{table: desc_by_type}"
    ),
    resources=SimpleAliasedResourceRegistry(
        {
            "desc_by_type": AliasedTableResource(
                table_body=[
                    [_("Type of {arg:0}"), _("Type of {arg:1}"), _("Return value")],
                    [
                        "{type:FLOAT|INTEGER}",
                        "{type:FLOAT|INTEGER}",
                        _("The difference between the numbers {arg:0} and {arg:1}."),
                    ],
                    [
                        "{type:DATE}",
                        "{type:FLOAT|INTEGER}",
                        _(
                            "The date that is {arg:1} days smaller than {arg:0} (rounded down to an "
                            "integer number of days)."
                        ),
                    ],
                    [
                        "{type:DATETIME|GENERICDATETIME}",
                        "{type:FLOAT|INTEGER}",
                        _(
                            "The date with time, {arg:1} days smaller than {arg:0}. If {arg:1} contains a "
                            "fractional part, it is converted to hours (`1/24`), minutes (`1/1440`), and "
                            "seconds (`1/86400`)."
                        ),
                    ],
                    ["{type:DATE}", "{type:DATE}", _("The difference between two dates in days.")],
                    [
                        "{type:DATETIME}",
                        "{type:DATETIME}",
                        _(
                            "The difference between two dates in days: the integer part — the number of "
                            "whole days, the fractional part — the number of hours, minutes and seconds "
                            "expressed as a fraction of the whole day (1 hour is '1/24')."
                        ),
                    ],
                    [
                        "{type:GENERICDATETIME}",
                        "{type:GENERICDATETIME}",
                        _(
                            "The difference between two dates in days: the integer part — the number of "
                            "whole days, the fractional part — the number of hours, minutes and seconds "
                            "expressed as a fraction of the whole day (1 hour is '1/24')."
                        ),
                    ],
                ]
            )
        }
    ),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "2 - 3 = -1",
            "2 - 0.5 = 1.5",
            "#2019-01-06# - 2 = #2019-01-04#",
            "#2019-01-06# - 2.2 = #2019-01-03#",
            "#2019-01-06 03:00:00# - 2 = #2019-01-04 03:00:00#",
            "#2019-01-06 03:00:00# - 2.5 = #2019-01-03 15:00:00#",
            "#2019-01-06# - #2019-01-02# = 4",
            "#2019-01-06 15:00:00# - #2019-01-02 03:00:00# = 4.5",
        )
    ],
)

FUNCTION_OP_LIKE = FunctionDocRegistryItem(
    name="like",
    category=CATEGORY_OPERATOR,
    description=_(
        "Matches the string {arg:0} to the template {arg: 1} and returns `TRUE` on "
        "match.\n"
        "You can specify the value in {arg:1} or use the `%` character to match a "
        "string of any length.\n"
        "\n"
        "The `{argn:0} NOT LIKE` option returns the opposite value.\n"
        "\n"
        "When comparing values, the function is case-sensitive. "
        "You can use `LIKE` along with {ref:UPPER} or {ref:LOWER} for case-insensitive comparison."
    ),
    signature_gen=TemplatedSignatureGenerator(signature_templates=(SignatureTemplate(body="{1} [ NOT ] LIKE {2}"),)),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            '"raspberry" LIKE "%spb%" = TRUE',
            '"raspberry" LIKE "%SPB%" = FALSE',
            '"raspberry" NOT LIKE "%spb%" = FALSE',
            "IF([Country] LIKE 'RU', 'Y', 'N')",
            "IF([Phone] LIKE '+7%', 'RU', 'notRU')",
            'UPPER("raspberry") LIKE "%SPB%" = TRUE',
            'UPPER("West") LIKE "WEST" = TRUE',
        )
    ],
)

FUNCTION_OP_COMPARISON = FunctionDocRegistryItem(
    name="==",
    category=CATEGORY_OPERATOR,
    naming_provider=CustomNamingProvider(
        title=_("Comparison"),
        internal_name="op_comparison",
    ),
    description=_("Compares the value {arg:0} with the value {arg:1}."),
    signature_gen=TemplatedSignatureGenerator(
        signature_templates=(
            SignatureTemplate(title=_("Equality"), body="{1} = {2}"),
            SignatureTemplate(title=_("Inequality"), body="{1} != {2}"),
            SignatureTemplate(title=_("Less than"), body="{1} < {2}"),
            SignatureTemplate(title=_("Less than or equal"), body="{1} <= {2}"),
            SignatureTemplate(title=_("Greater than"), body="{1} > {2}"),
            SignatureTemplate(title=_("Greater than or equal"), body="{1} >= {2}"),
        )
    ),
    examples=[
        SimpleExample("1 = 1 = TRUE"),
        SimpleExample("7 > 2 > 1 = TRUE"),
    ],
)

FUNCTION_OP_AND = FunctionDocRegistryItem(
    name="and",
    category=CATEGORY_OPERATOR,
    description=_("Performs a Boolean join of two expressions with the `AND` condition."),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample('[Profit] > 30 AND [Weekday] IN ("Saturday", "Sunday")'),
    ],
)

FUNCTION_OP_OR = FunctionDocRegistryItem(
    name="or",
    category=CATEGORY_OPERATOR,
    description=_("Performs a Boolean join of two expressions with the `OR` condition."),
    signature_gen=_SIGNATURE_OP_BINARY,
    examples=[
        SimpleExample('[Month] = "January" OR [Year] < 2019'),
    ],
)

FUNCTION_OP_IN = FunctionDocRegistryItem(
    name="in",
    category=CATEGORY_OPERATOR,
    description=_(
        "Checks whether the value matches at least one of the values listed in "
        "`IN(...)`.\n"
        "\n"
        "The option `{argn:0} NOT IN (<{arg:1}>)` returns the opposite value."
    ),
    signature_gen=TemplatedSignatureGenerator(signature_templates=(SignatureTemplate(body="{1} [ NOT ] IN (<{2}>)"),)),
    examples=[
        SimpleExample("3 IN (23, 5, 3, 67) = TRUE"),
        SimpleExample("3 NOT IN (23, 5, 3, 67) = FALSE"),
    ],
)

FUNCTION_OP_BETWEEN = FunctionDocRegistryItem(
    name="between",
    category=CATEGORY_OPERATOR,
    description=_(
        "Returns `TRUE` if {arg:0} is in the range from {arg:1} to {arg:2} inclusive.\n"
        "\n"
        "The option `{argn:0} NOT BETWEEN {argn:1} AND {argn:2}` returns the "
        "opposite value."
    ),
    notes=[
        Note(
            Translatable("Arguments {arg:0}, {arg:1}, {arg:2} must be of the same type."),
        ),
    ],
    signature_gen=TemplatedSignatureGenerator(
        signature_templates=(SignatureTemplate(body="{1} [ NOT ] BETWEEN {2} AND {3}"),)
    ),
    examples=[
        SimpleExample("3 BETWEEN 1 AND 100 = TRUE"),
        SimpleExample("100 BETWEEN 1 AND 100 = TRUE"),
        SimpleExample("3 NOT BETWEEN 1 AND 100 = FALSE"),
        SimpleExample("#2018-01-12# BETWEEN #2018-01-10# AND #2018-01-15# = TRUE"),
        SimpleExample("#2018-01-12 01:02:10# BETWEEN #2018-01-12 01:02:00# AND #2018-01-12 01:02:30# = TRUE"),
    ],
)

FUNCTIONS_OPERATOR = [
    FUNCTION_OP_NOT,
    FUNCTION_OP_NEGATION,
    FUNCTION_OP_IS_TRUE,
    FUNCTION_OP_IS_FALSE,
    FUNCTION_OP_POWER,
    FUNCTION_OP_MULT,
    FUNCTION_OP_DIV,
    FUNCTION_OP_MOD,
    FUNCTION_OP_PLUS,
    FUNCTION_OP_MINUS,
    FUNCTION_OP_LIKE,
    FUNCTION_OP_COMPARISON,
    FUNCTION_OP_AND,
    FUNCTION_OP_OR,
    FUNCTION_OP_IN,
    FUNCTION_OP_BETWEEN,
]
