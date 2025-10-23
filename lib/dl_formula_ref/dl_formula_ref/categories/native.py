from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_NATIVE = FunctionDocCategory(
    name="native",
    description=_("Functions for calling native database functions"),
    keywords=_("native, database, db, call"),
)
