from dl_formula.core.datatype import DataType
from dl_formula_ref.categories.markup import CATEGORY_MARKUP
from dl_formula_ref.i18n.registry import FormulaRefTranslatable as Translatable
from dl_formula_ref.localization import get_gettext
from dl_formula_ref.registry.base import FunctionDocRegistryItem
from dl_formula_ref.registry.example import (
    DataExample,
    SimpleExample,
)
from dl_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from dl_formula_ref.registry.note import Note


_ = get_gettext()

_SOURCE_PRODUCTS_1 = ExampleSource(
    columns=[("Product", DataType.STRING)],
    data=[["Телевизор"], ["Смартфон"], ["Ноутбук"]],
)

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
    description=_("Enables specifying the color for the provided text."),
    examples=[
        SimpleExample("COLOR([text], '#5282ff')"),
        SimpleExample("COLOR([text], 'blue')"),
        SimpleExample("COLOR([text], 'rgb(82, 130, 255)')"),
        SimpleExample("COLOR([text], 'rgba(82, 130, 255, 0.5)')"),
    ],
    notes=[
        Note(
            Translatable("You can specify the color in any web format, such as HEX, keyword (e.g., `green`), RGB, etc.")
        ),
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

FUNCTIONS_MARKUP = [
    FUNCTION_BOLD,
    FUNCTION_ITALIC,
    FUNCTION_URL,
    FUNCTION_MARKUP,
    FUNCTION_COLOR,
    FUNCTION_SIZE,
    FUNCTION_BR,
]
