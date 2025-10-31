from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_NATIVE = FunctionDocCategory(
    name="native",
    description=_(
        "Functions for calling native database functions.\n"
        "\n"
        "The first argument {arg:0} must be a constant string with the name of the "
        "database function to call. All subsequent arguments are passed to the "
        "native function and can be of any type.\n"
        "\n"
        "The function name must contain only alphanumeric characters, underscore "
        "and colon characters."
    ),
    keywords=_("native, database, function, call"),
)
