from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_NATIVE = FunctionDocCategory(
    name="native",
    description=_("Functions for calling native database functions by its name."),
    keywords=_("native, database, function, call"),
)
