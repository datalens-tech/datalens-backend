from bi_formula_ref.localization import get_gettext
from bi_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    AliasedResourceRegistry,
)
from bi_formula_ref.registry.base import FunctionDocCategory

_ = get_gettext()

CATEGORY_DATE = FunctionDocCategory(
    name="date",
    description="",
    resources=AliasedResourceRegistry(
        resources={
            "iso_8601": AliasedLinkResource(url=_("https://en.wikipedia.org/wiki/ISO_8601")),
        }
    ),
    keywords="",
)
