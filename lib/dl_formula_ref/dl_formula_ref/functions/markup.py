from dl_formula_ref.categories.markup import CATEGORY_MARKUP
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import SimpleExample


_ = get_gettext()


FUNCTION_BOLD = FunctionDocRegistryItem(
    name="bold",
    category=CATEGORY_MARKUP,
    description=_("Stylizes the passed text in bold font."),
)

FUNCTION_ITALIC = FunctionDocRegistryItem(
    name="italic",
    category=CATEGORY_MARKUP,
    description=_("Stylizes the passed text in cursive font."),
)

FUNCTION_URL = FunctionDocRegistryItem(
    name="url",
    category=CATEGORY_MARKUP,
    description=_(
        "Wraps {arg:1} into a hyperlink to URL {arg:0}. "
        "When you click on the link, the page opens in a new browser tab."
    ),
    examples=[
        SimpleExample("URL('https://yandex.ru/search/?text=buy+a+' + [Product Name], [Product Name])"),
    ],
)

FUNCTION_MARKUP = FunctionDocRegistryItem(
    name="markup",
    category=CATEGORY_MARKUP,
    description=_("Merges marked up text pieces. " "Can also be used for converting strings to marked up text."),
    examples=[
        SimpleExample("MARKUP(BOLD([a]), ': ', [b])"),
    ],
)

FUNCTION_COLOR = FunctionDocRegistryItem(
    name="color",
    category=CATEGORY_MARKUP,
    description=_(
        "Enables specifying the color for the provided text.\n"
        "\n"
        "We recommend using the {link: gravity_ui_texts_link: color variables} from the "
        "{link: gravity_ui_link: Gravity UI} palette to specify colors. "
        "Such colors are easily discernible with both the light and dark theme.\n"
        "\n"
        "You can also specify the color in any web format, such as HEX, keyword (e.g., `green`), RGB, etc. "
        "In this case, however, we cannot guarantee that the colors will be discernible."
    ),
    examples=[
        SimpleExample("COLOR([text], 'var(--g-color-text-danger)')"),
        SimpleExample("COLOR([text], '#5282ff')"),
        SimpleExample("COLOR([text], 'blue')"),
        SimpleExample("COLOR([text], 'rgb(82, 130, 255)')"),
        SimpleExample("COLOR([text], 'rgba(82, 130, 255, 0.5)')"),
    ],
)

FUNCTION_SIZE = FunctionDocRegistryItem(
    name="size",
    category=CATEGORY_MARKUP,
    description=_("Enables specifying the size (in pixels) for the provided text."),
    examples=[
        SimpleExample("SIZE([text], '26px')"),
    ],
)

FUNCTION_BR = FunctionDocRegistryItem(
    name="br",
    category=CATEGORY_MARKUP,
    description=_("Adds a line break."),
    examples=[
        SimpleExample("'Line 1' + BR() + 'Line 2'"),
    ],
)

FUNCTION_IMAGE = FunctionDocRegistryItem(
    name="image",
    category=CATEGORY_MARKUP,
    description=_(
        "Enables inserting an image located at the {arg:0} address to the table. "
        "The {arg:1} and {arg:2} values are provided in pixels. If one of the dimensions is "
        "`NULL`, it will be calculated automatically in proportion to the other. "
        "If both dimensions are `NULL`, the image will be inserted with the original width and height. "
        "In case there are issues when uploading the image, the function will display the {arg:3} text.\n"
        "\n"
        "{text: image_source_restrictions}\n"
        "\n"
    ),
    examples=[
        SimpleExample(
            "IMAGE('https://storage.yandexcloud.net/functions********/nature-01.jpg', 250, 150, 'alt-text-1')"
        ),
        SimpleExample(
            "IMAGE('https://storage.yandexcloud.net/functions********/nature-02.jpg', NULL, NULL, 'alt-text-2')"
        ),
    ],
)

FUNCTION_USER_INFO = FunctionDocRegistryItem(
    name="user_info",
    category=CATEGORY_MARKUP,
    description=_(
        "Returns the marked up text by {arg:0} to display username or email depending on the {arg:1} value:\n"
        "\n"
        "* `email`: Returns email.\n"
        "* `name`: Returns name.\n"
        "\n"
        "If the user has not been found, the function will return the original string from the source."
    ),
    examples=[
        SimpleExample("USER_INFO('b1ggmp8es27t********', 'name') = 'test_user'"),
        SimpleExample("USER_INFO([UserId], 'email') = 'test_user@example.com'"),
    ],
)

FUNCTION_TOOLTIP = FunctionDocRegistryItem(
    name="tooltip",
    category=CATEGORY_MARKUP,
    description=_(
        "Enables adding a short popup hint and change its position relative to the cell. {arg:2} can be 'top', 'right', 'bottom' or 'left'; default – 'right'."
    ),
    examples=[
        SimpleExample("TOOLTIP(SIZE('Hello', '12px'), URL('https://ya.ru', 'Yandex'), 'top')"),
        SimpleExample("TOOLTIP([main_text], [tooltip_text])"),
    ],
)


FUNCTIONS_MARKUP = [
    FUNCTION_BOLD,
    FUNCTION_ITALIC,
    FUNCTION_URL,
    FUNCTION_MARKUP,
    FUNCTION_COLOR,
    FUNCTION_SIZE,
    FUNCTION_BR,
    FUNCTION_IMAGE,
    FUNCTION_USER_INFO,
    FUNCTION_TOOLTIP,
]
