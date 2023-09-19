from dl_formula.core.datatype import DataType
from dl_formula_ref.categories.mathematical import CATEGORY_MATHEMATICAL
from dl_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import (
    DataExample,
    SimpleExample,
)


_ = get_gettext()

_SOURCE_1_VAL_1 = ExampleSource(
    columns=[("value", DataType.FLOAT)],
    data=[[1.0], [0.1], [-2.0], [50.5], [0.0], [-3.5]],
)

_SOURCE_SQ_1 = ExampleSource(
    columns=[("int_value", DataType.INTEGER), ("float_value", DataType.FLOAT)],
    data=[[0, 0.0], [1, 1.0], [3, 3.0], [4, 4.0], [13, 13.0], [16, 16.0], [20, 20.20], [25, 25.0]],
)

_SOURCE_TRIG_1 = ExampleSource(
    columns=[("n", DataType.FLOAT)],
    data=[[-1.0], [-0.5], [-0.25], [0.0], [0.25], [0.5], [1.0]],
)

_SOURCE_TRIG_2 = ExampleSource(
    columns=[("n", DataType.FLOAT)],
    data=[[-1.0], [-0.86602540378], [-0.70710678118], [-0.5], [0.0], [0.5], [0.70710678118], [0.86602540378], [1.0]],
)


_DIV_COLUMNS = [("numerator", DataType.FLOAT), ("denominator", DataType.FLOAT)]

_SOURCE_DIV = ExampleSource(columns=_DIV_COLUMNS, data=[[4, 2], [5, 3], [5, 2.0], [2.5, 1.2]])

_SOURCE_DIV_SAFE = ExampleSource(columns=_DIV_COLUMNS, data=[[5, 2], [5, 0]])


FUNCTION_ABS = FunctionDocRegistryItem(
    name="abs",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the absolute value of {arg:0}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_1_VAL_1,
                formula_fields=[("value", "[value]"), ("result", "ABS([value])")],
            ),
        ),
    ],
)

FUNCTION_ACOS = FunctionDocRegistryItem(
    name="acos",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the arccosine of {arg:0} in radians."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_2,
                formula_fields=[("value", "[n]"), ("result", "ACOS([n])")],
            ),
        ),
    ],
)

FUNCTION_ASIN = FunctionDocRegistryItem(
    name="asin",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the arcsine of {arg:0} in radians."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_2,
                formula_fields=[("value", "[n]"), ("result", "ASIN([n])")],
            ),
        ),
    ],
)

FUNCTION_ATAN = FunctionDocRegistryItem(
    name="atan",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the arctangent of {arg:0} in radians."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_2,
                formula_fields=[("value", "[n]"), ("result", "ATAN([n])")],
            ),
        ),
    ],
)

FUNCTION_ATAN2 = FunctionDocRegistryItem(
    name="atan2",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the arctangent in radians for the specified coordinates {arg:0} and " "{arg:1}."),
    examples=[
        SimpleExample("ATAN2(5, 7) = 0.62024"),
    ],
)

FUNCTION_CEILING = FunctionDocRegistryItem(
    name="ceiling",
    category=CATEGORY_MATHEMATICAL,
    description=_("Rounds the value up to the nearest integer."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_1_VAL_1,
                formula_fields=[("value", "[value]"), ("result", "CEILING([value])")],
            ),
        ),
    ],
)

FUNCTION_FLOOR = FunctionDocRegistryItem(
    name="floor",
    category=CATEGORY_MATHEMATICAL,
    description=_("Rounds the value down to the nearest integer."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_1_VAL_1,
                formula_fields=[("value", "[value]"), ("result", "FLOOR([value])")],
            ),
        ),
    ],
)

FUNCTION_COS = FunctionDocRegistryItem(
    name="cos",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the cosine of {arg:0} in radians."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_1,
                formula_fields=[("n", "[n]"), ("angle", "[n] * PI()"), ("result", "COS([n]*PI())")],
            ),
        ),
    ],
)

FUNCTION_COT = FunctionDocRegistryItem(
    name="cot",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the cotangent of {arg:0} in radians."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_1,
                formula_fields=[("n", "[n]"), ("angle", "[n] * PI()"), ("result", "COT([n]*PI())")],
            ),
        ),
    ],
)

FUNCTION_DEGREES = FunctionDocRegistryItem(
    name="degrees",
    category=CATEGORY_MATHEMATICAL,
    description=_("Converts radians to degrees."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_1,
                formula_fields=[("n", "[n]"), ("angle", "[n] * PI()"), ("result", "DEGREES([n]*PI())")],
            ),
        ),
    ],
)

FUNCTION_DIV = FunctionDocRegistryItem(
    name="div",
    category=CATEGORY_MATHEMATICAL,
    description=_("Divides {arg:0} by {arg:1}. The result is rounded down to the nearest " "integer."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_DIV,
                formula_fields=[
                    ("numerator", "[numerator]"),
                    ("denominator", "[denominator]"),
                    ("result", "DIV([numerator], [denominator])"),
                ],
            ),
        ),
    ],
)

FUNCTION_DIV_SAFE = FunctionDocRegistryItem(
    name="div_safe",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Divides {arg:0} by {arg:1}. Returns {arg:2} if division by zero occurs. "
        "If the number {arg:2} is omitted, it is assumed to be `NULL`.\n"
        "The result is rounded down to the nearest integer."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_DIV_SAFE,
                formula_fields=[
                    ("numerator", "[numerator]"),
                    ("denominator", "[denominator]"),
                    ("result2", "DIV_SAFE([numerator], [denominator])"),
                    ("result3", "DIV_SAFE([numerator], [denominator], 42)"),
                ],
            ),
        ),
    ],
)

FUNCTION_FDIV_SAFE = FunctionDocRegistryItem(
    name="fdiv_safe",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Divides {arg:0} by {arg:1}. Returns {arg:2} if division by zero occurs. "
        "If the number {arg:2} is omitted, it is assumed to be `NULL`."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_DIV_SAFE,
                formula_fields=[
                    ("numerator", "[numerator]"),
                    ("denominator", "[denominator]"),
                    ("result2", "FDIV_SAFE([numerator], [denominator])"),
                    ("result3", "FDIV_SAFE([numerator], [denominator], 42)"),
                ],
            ),
        ),
    ],
)

FUNCTION_EXP = FunctionDocRegistryItem(
    name="exp",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the result of raising the number 'e' to the power of {arg:0}."),
    examples=[
        SimpleExample("EXP(0) = 1.0"),
        SimpleExample("EXP(1) = 2.718282"),
        SimpleExample("EXP(3) = 20.08553"),
    ],
)

FUNCTION_LN = FunctionDocRegistryItem(
    name="ln",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Returns the natural logarithm of the number {arg:0}. Returns `NULL` if the "
        "number is less than or equal to 0."
    ),
    examples=[
        SimpleExample("LN(1) = 0.0"),
        SimpleExample("LN(EXP(2)) = 2.0"),
    ],
)

FUNCTION_LOG10 = FunctionDocRegistryItem(
    name="log10",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Returns the logarithm of the number {arg:0} to base 10. Returns `NULL` if "
        "the number is less than or equal to 0."
    ),
    examples=[
        SimpleExample("LOG10(1) = 0.0"),
        SimpleExample("LOG10(1000) = 3.0"),
        SimpleExample("LOG10(100) = 2.0"),
    ],
)

FUNCTION_LOG = FunctionDocRegistryItem(
    name="log",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Returns the logarithm of {arg:0} to base {arg:1}. Returns `NULL` if the "
        "number {arg:0} is less than or equal to 0."
    ),
    examples=[
        SimpleExample("LOG(1, 2.6) = 0.0"),
        SimpleExample("LOG(1024, 2) = 10.0"),
        SimpleExample("LOG(100, 10) = 2.0"),
    ],
)

FUNCTION_GREATEST = FunctionDocRegistryItem(
    name="greatest",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Returns the greatest value.\n"
        "\n"
        "See also {ref:LEAST}.\n"
        "\n"
        "Depending on the specified data type, it returns:\n"
        "- The greatest number.\n"
        "- The last string in alphabetical order.\n"
        "- The latest date.\n"
        "- `TRUE` when selecting between `TRUE` and `FALSE` for Boolean type."
    ),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "GREATEST(3.4, 2.6) = 3.4",
            'GREATEST("3.4", "2.6") = "3.4"',
            "GREATEST(#2019-01-02#, #2019-01-17#) = #2019-01-17#",
            "GREATEST(#2019-01-02 04:03:02#, #2019-01-17 03:02:01#) = #2019-01-17 03:02:01#",
            "GREATEST(TRUE, FALSE) = TRUE",
            "GREATEST(34, 5, 7, 3, 99, 1, 2, 2, 56) = 99",
            "GREATEST(5.6, 1.2, 7.8, 3.4) = 7.8",
            "GREATEST(#2019-01-02#, #2019-01-17#, #2019-01-10#) = #2019-01-17#",
        )
    ],
)

FUNCTION_LEAST = FunctionDocRegistryItem(
    name="least",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Returns the smallest value.\n"
        "\n"
        "See also {ref:GREATEST}.\n"
        "\n"
        "Depending on the specified data type, it returns:\n"
        "- The smallest number.\n"
        "- The first string in alphabetical order.\n"
        "- The earliest date.\n"
        "- `FALSE` when selecting between `TRUE` and `FALSE` for Boolean type."
    ),
    examples=[
        SimpleExample(example_str)
        for example_str in (
            "LEAST(3.4, 2.6) = 2.6",
            'LEAST("3.4", "2.6") = "2.6"',
            "LEAST(#2019-01-02#, #2019-01-17#) = #2019-01-02#",
            "LEAST(#2019-01-02 04:03:02#, #2019-01-17 03:02:01#) = #2019-01-02 04:03:02#",
            "LEAST(TRUE, FALSE) = FALSE",
            "LEAST(34, 5, 7, 3, 99, 1, 2, 2, 56) = 1",
            "LEAST(5.6, 1.2, 7.8, 3.4) = 1.2",
            "LEAST(#2019-01-02#, #2019-01-17#, #2019-01-10#) = #2019-01-02#",
        )
    ],
)

FUNCTION_PI = FunctionDocRegistryItem(
    name="pi",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns PI. The accuracy depends on the data source."),
    examples=[
        SimpleExample("PI() = 3.14159"),
    ],
)

FUNCTION_POWER = FunctionDocRegistryItem(
    name="power",
    category=CATEGORY_MATHEMATICAL,
    description=_("Raises {arg:0} to the power of {arg:1}."),
    examples=[
        SimpleExample("POWER(2.3, 4.5) = 42.43998894277659"),
        SimpleExample("POWER(6, 2) = 36.0"),
    ],
)

FUNCTION_RADIANS = FunctionDocRegistryItem(
    name="radians",
    category=CATEGORY_MATHEMATICAL,
    description=_("Converts {arg:0} degrees to radians."),
    examples=[
        SimpleExample("RADIANS(0) = 0.0"),
        SimpleExample("RADIANS(180) = 3.14159"),
    ],
)

FUNCTION_ROUND = FunctionDocRegistryItem(
    name="round",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Rounds the number {arg:0} to the number of decimal digits specified in "
        "{arg:1}.\n"
        "If the number {arg:1} is omitted, {arg:0} is rounded to the nearest integer."
    ),
    examples=[
        SimpleExample("ROUND(3.14159) = 3"),
        SimpleExample("ROUND(3.14159, 3) = 3.142"),
    ],
)

FUNCTION_SIGN = FunctionDocRegistryItem(
    name="sign",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Returns the sign of the number {arg:0}:\n"
        "- `-1` if the number is negative.\n"
        "`0` if the number is zero.\n"
        "- `1` if the number is positive."
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_1_VAL_1,
                formula_fields=[("value", "[value]"), ("result", "SIGN([value])")],
            ),
        ),
    ],
)

FUNCTION_SIN = FunctionDocRegistryItem(
    name="sin",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the sine of {arg:0} in radians."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_1,
                formula_fields=[("n", "[n]"), ("angle", "[n] * PI()"), ("result", "SIN([n]*PI())")],
            ),
        ),
    ],
)

FUNCTION_SQRT = FunctionDocRegistryItem(
    name="sqrt",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the square root of the specified number."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_SQ_1,
                formula_fields=[
                    ("int_value", "[int_value]"),
                    ("float_value", "[float_value]"),
                    ("int_result", "SQRT([int_value])"),
                    ("float_result", "SQRT([float_value])"),
                ],
            ),
        ),
    ],
)

FUNCTION_SQUARE = FunctionDocRegistryItem(
    name="square",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the number {arg:0} raised to the power of 2."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_SQ_1,
                formula_fields=[
                    ("int_value", "[int_value]"),
                    ("float_value", "[float_value]"),
                    ("int_result", "SQUARE([int_value])"),
                    ("float_result", "SQUARE([float_value])"),
                ],
            ),
        ),
    ],
)

FUNCTION_TAN = FunctionDocRegistryItem(
    name="tan",
    category=CATEGORY_MATHEMATICAL,
    description=_("Returns the tangent of {arg:0} in radians."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_TRIG_1,
                formula_fields=[("n", "[n]"), ("angle", "[n] * PI()"), ("result", "tan([n]*PI())")],
            ),
        ),
    ],
)

FUNCTION_COMPARE = FunctionDocRegistryItem(
    name="compare",
    category=CATEGORY_MATHEMATICAL,
    description=_(
        "Returns:\n"
        "* 0 if {arg:0} and {arg:1} differs by not more than {arg:2}.\n"
        "* -1 if {arg:0} is less than {arg:1} by more than {arg:2}.\n"
        "* 1 if {arg:0} is greater than {arg:1} by more than {arg:2}."
    ),
    examples=[
        SimpleExample("COMPARE(1.25, 1.26, 0.1) = 0"),
        SimpleExample("COMPARE(1.25, 1.26, 0.001) = -1"),
        SimpleExample("COMPARE(1.26, 1.25, 0.001) = 1"),
    ],
)

FUNCTIONS_MATHEMATICAL = [
    FUNCTION_ABS,
    FUNCTION_ACOS,
    FUNCTION_ASIN,
    FUNCTION_ATAN,
    FUNCTION_ATAN2,
    FUNCTION_CEILING,
    FUNCTION_FLOOR,
    FUNCTION_COS,
    FUNCTION_COT,
    FUNCTION_DEGREES,
    FUNCTION_DIV,
    FUNCTION_DIV_SAFE,
    FUNCTION_FDIV_SAFE,
    FUNCTION_EXP,
    FUNCTION_LN,
    FUNCTION_LOG10,
    FUNCTION_LOG,
    FUNCTION_GREATEST,
    FUNCTION_LEAST,
    FUNCTION_PI,
    FUNCTION_POWER,
    FUNCTION_RADIANS,
    FUNCTION_ROUND,
    FUNCTION_SIGN,
    FUNCTION_SIN,
    FUNCTION_SQRT,
    FUNCTION_SQUARE,
    FUNCTION_TAN,
    FUNCTION_COMPARE,
]
