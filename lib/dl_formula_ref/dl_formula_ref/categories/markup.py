from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.base import FunctionDocCategory


_ = get_gettext()

CATEGORY_MARKUP = FunctionDocCategory(
    name="markup",
    description=_(
        "Markup functions are used for creating marked up text (hypertext) from "
        "string values and/or other marked up text.\n"
        "\n"
        "## Usage notes {#usage-notes}\n"
        "\n"
        "There are the following features of using markup:\n"
        "1. These functions return `NULL` when any argument is `NULL`. To get a non-"
        "NULL value, wrap argumens in `IFNULL()`. Example: `bold(ifnull([value], "
        "'NULL'))`.\n"
        "1. Converting markup to a normal string is not currently possible.\n"
        "1. Markup functions can be used within logic functions. Example: "
        "`IF(STARTSWITH([value], 'n'), BOLD([value]), MARKUP([value]))`.\n"
    ),
    keywords="",
    resources=SimpleAliasedResourceRegistry(
        resources={
            "gravity_ui_link": AliasedLinkResource(url=_("https://gravity-ui.com/")),
            "gravity_ui_texts_link": AliasedLinkResource(
                url=_("https://preview.gravity-ui.com/uikit/iframe.html?args=&id=colors--texts&viewMode=story")
            ),
            "jing_link": AliasedLinkResource(url=_("https://jing.yandex-team.ru/")),
            "storage_link": AliasedLinkResource(url=_("../../storage/quickstart.md")),
            "storage_prices_link": AliasedLinkResource(url=_("../../storage/pricing.md#prices-storage")),
        }
    ),
)
