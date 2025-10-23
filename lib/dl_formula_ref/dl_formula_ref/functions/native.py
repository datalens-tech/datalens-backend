from dl_formula_ref.categories.native import CATEGORY_NATIVE
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample
from dl_formula_ref.registry.note import Note


_ = get_gettext()


FUNCTION_DB_CALL_INT = FunctionDocRegistryItem(
    name="db_call_int",
    category=CATEGORY_NATIVE,
    description=_(
        "Calls a native database function by name and returns an integer result.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters and underscores."
    ),
    notes=[
        Note(
            Translatable(
                "This function allows you to call database-specific functions that are not available "
                "as standard functions in DataLens. The availability and behavior of native functions "
                "depends on your database type and version."
            )
        ),
    ],
    examples=[
        SimpleExample('DB_CALL_INT("sign", -5) = -1'),
        SimpleExample('DB_CALL_INT("sign", 5) = 1'),
        SimpleExample('DB_CALL_INT("positionCaseInsensitive", "Hello", "l") = 3'),
    ],
)

FUNCTION_DB_CALL_FLOAT = FunctionDocRegistryItem(
    name="db_call_float",
    category=CATEGORY_NATIVE,
    description=_(
        "Calls a native database function by name and returns a float result.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters and underscores."
    ),
    notes=[
        Note(
            Translatable(
                "This function allows you to call database-specific functions that are not available "
                "as standard functions in DataLens. The availability and behavior of native functions "
                "depends on your database type and version."
            )
        ),
    ],
    examples=[
        SimpleExample('DB_CALL_FLOAT("sign", -5.0) = -1.0'),
        SimpleExample('DB_CALL_FLOAT("sign", 5.0) = 1.0'),
        SimpleExample('DB_CALL_FLOAT("log10", 100.0) = 2.0'),
    ],
)

FUNCTION_DB_CALL_STRING = FunctionDocRegistryItem(
    name="db_call_string",
    category=CATEGORY_NATIVE,
    description=_(
        "Calls a native database function by name and returns a string result.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters and underscores."
    ),
    notes=[
        Note(
            Translatable(
                "This function allows you to call database-specific functions that are not available "
                "as standard functions in DataLens. The availability and behavior of native functions "
                "depends on your database type and version."
            )
        ),
    ],
    examples=[
        SimpleExample('DB_CALL_STRING("reverse", "hello") = "olleh"'),
    ],
)

FUNCTION_DB_CALL_BOOL = FunctionDocRegistryItem(
    name="db_call_bool",
    category=CATEGORY_NATIVE,
    description=_(
        "Calls a native database function by name and returns a boolean result.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters and underscores."
    ),
    notes=[
        Note(
            Translatable(
                "This function allows you to call database-specific functions that are not available "
                "as standard functions in DataLens. The availability and behavior of native functions "
                "depends on your database type and version."
            )
        ),
    ],
    examples=[
        SimpleExample('DB_CALL_BOOL("isFinite", 5) = TRUE'),
        SimpleExample('DB_CALL_BOOL("isInfinite", 5) = FALSE'),
    ],
)

FUNCTION_DB_CALL_ARRAY_INT = FunctionDocRegistryItem(
    name="db_call_array_int",
    category=CATEGORY_NATIVE,
    description=_(
        "Calls a native database function by name and returns an array of integers.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters and underscores."
    ),
    notes=[
        Note(
            Translatable(
                "This function allows you to call database-specific functions that are not available "
                "as standard functions in DataLens. The availability and behavior of native functions "
                "depends on your database type and version."
            )
        ),
    ],
    examples=[
        SimpleExample('DB_CALL_ARRAY_INT("range", 5) = ARRAY(0, 1, 2, 3, 4)'),
    ],
)

FUNCTION_DB_CALL_ARRAY_FLOAT = FunctionDocRegistryItem(
    name="db_call_array_float",
    category=CATEGORY_NATIVE,
    description=_(
        "Calls a native database function by name and returns an array of floats.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters and underscores."
    ),
    notes=[
        Note(
            Translatable(
                "This function allows you to call database-specific functions that are not available "
                "as standard functions in DataLens. The availability and behavior of native functions "
                "depends on your database type and version."
            )
        ),
    ],
    examples=[
        SimpleExample('DB_CALL_ARRAY_FLOAT("arrayConcat", ARRAY(1.0, 2.0), ARRAY(3.0)) = ARRAY(1.0, 2.0, 3.0)'),
    ],
)

FUNCTION_DB_CALL_ARRAY_STRING = FunctionDocRegistryItem(
    name="db_call_array_string",
    category=CATEGORY_NATIVE,
    description=_(
        "Calls a native database function by name and returns an array of strings.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters and underscores."
    ),
    notes=[
        Note(
            Translatable(
                "This function allows you to call database-specific functions that are not available "
                "as standard functions in DataLens. The availability and behavior of native functions "
                "depends on your database type and version."
            )
        ),
    ],
    examples=[
        SimpleExample('DB_CALL_ARRAY_STRING("splitByChar", ",", "a,b,c") = ARRAY("a", "b", "c")'),
    ],
)

FUNCTIONS_NATIVE = [
    FUNCTION_DB_CALL_INT,
    FUNCTION_DB_CALL_FLOAT,
    FUNCTION_DB_CALL_STRING,
    FUNCTION_DB_CALL_BOOL,
    FUNCTION_DB_CALL_ARRAY_INT,
    FUNCTION_DB_CALL_ARRAY_FLOAT,
    FUNCTION_DB_CALL_ARRAY_STRING,
]
