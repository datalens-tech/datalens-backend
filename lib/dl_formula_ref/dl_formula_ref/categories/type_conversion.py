from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_TYPE_CONVERSION = FunctionDocCategory(
    name="type-conversion",
    description="",
    keywords="",
    resources=SimpleAliasedResourceRegistry(
        resources={
            "unix_ts": AliasedLinkResource(url=_("https://en.wikipedia.org/wiki/Unix_time")),
            "geopolygon_link": AliasedLinkResource(url=_("../concepts/data-types.md#geopolygon")),
            "timezone_link": AliasedLinkResource(
                url=_("https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List")
            ),
        }
    ),
)
