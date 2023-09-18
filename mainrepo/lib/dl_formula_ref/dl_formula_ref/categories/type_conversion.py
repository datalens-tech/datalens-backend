from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    AliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocCategory

_ = get_gettext()

CATEGORY_TYPE_CONVERSION = FunctionDocCategory(
    name="type-conversion",
    description="",
    keywords="",
    resources=AliasedResourceRegistry(
        resources={
            "unix_ts": AliasedLinkResource(url=_("https://en.wikipedia.org/wiki/Unix_time")),
        }
    ),
)
