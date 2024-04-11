from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_STRING = FunctionDocCategory(
    name="string",
    description="",
    keywords="",
    resources=SimpleAliasedResourceRegistry(
        resources={
            "ch_re_link": AliasedLinkResource(url=_("https://github.com/google/re2/wiki/Syntax")),
            "abs_value_link": AliasedLinkResource(url=_("https://en.wikipedia.org/wiki/Absolute_value")),
        }
    ),
)
