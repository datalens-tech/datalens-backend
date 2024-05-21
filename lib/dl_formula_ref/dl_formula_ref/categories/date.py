from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_DATE = FunctionDocCategory(
    name="date",
    description="",
    resources=SimpleAliasedResourceRegistry(
        resources={
            "iso_8601": AliasedLinkResource(url=_("https://en.wikipedia.org/wiki/ISO_8601")),
            "ch_toStartOfInterval": AliasedLinkResource(
                url=_("https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofinterval")
            ),
        }
    ),
    keywords="",
)
