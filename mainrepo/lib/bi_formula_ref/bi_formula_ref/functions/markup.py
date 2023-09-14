from bi_formula_ref.categories.markup import CATEGORY_MARKUP
from bi_formula_ref.localization import get_gettext
from bi_formula_ref.registry.base import FunctionDocRegistryItem
from bi_formula_ref.registry.example import SimpleExample

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
    description=_("Wraps {arg:1} into a hyperlink to URL {arg:0}."),
    examples=[
        SimpleExample("URL('https://example.com/?value=' + [value], [value])"),
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

FUNCTIONS_MARKUP = [
    FUNCTION_BOLD,
    FUNCTION_ITALIC,
    FUNCTION_URL,
    FUNCTION_MARKUP,
]
