from bi_formula_ref.localization import get_gettext
from bi_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    AliasedResourceRegistry,
)
from bi_formula_ref.registry.base import FunctionDocCategory

_ = get_gettext()

CATEGORY_STRING = FunctionDocCategory(
    name="string",
    description="",
    keywords="",
    resources=AliasedResourceRegistry(
        resources={
            "ch_re_link": AliasedLinkResource(url=_("https://github.com/google/re2/wiki/Syntax")),
        }
    ),
)
