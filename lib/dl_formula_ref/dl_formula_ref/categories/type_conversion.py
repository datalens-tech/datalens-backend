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
            "ch_parseDateTime": AliasedLinkResource(
                url=_(
                    "https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions#parsedatetime32besteffort"
                )
            ),
            "ch_toStartOfInterval": AliasedLinkResource(
                url=_("https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofinterval")
            ),
            "geopolygon_link": AliasedLinkResource(url=_("../dataset/data-types.md#geopolygon")),
            "timezone_link": AliasedLinkResource(
                url=_("https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List")
            ),
            "tree_link": AliasedLinkResource(url=_("../dataset/data-types.md#tree-hierarchy")),
            "unix_ts": AliasedLinkResource(url=_("https://en.wikipedia.org/wiki/Unix_time")),
        }
    ),
)
