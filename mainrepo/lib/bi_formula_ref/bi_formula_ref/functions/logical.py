from bi_formula.core.datatype import DataType
from bi_formula_ref.categories.logical import CATEGORY_LOGICAL
from bi_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from bi_formula_ref.localization import get_gettext
from bi_formula_ref.registry.base import FunctionDocRegistryItem
from bi_formula_ref.registry.example import (
    DataExample,
    SimpleExample,
)
from bi_formula_ref.registry.naming import CustomNamingProvider
from bi_formula_ref.registry.signature_base import SignaturePlacement
from bi_formula_ref.registry.signature_gen import (
    SignatureTemplate,
    TemplatedSignatureGenerator,
)

_ = get_gettext()

_SOURCE_SALES_1 = ExampleSource(
    columns=[("sales", DataType.FLOAT)],
    data=[[432.40], [77.0], [12_000.0], [None], [34.25], [128.0], [0.0], [None]],
)
_SOURCE_TIME_1 = ExampleSource(
    columns=[("unit", DataType.STRING)],
    data=[["s"], ["m"], ["h"]],
)


FUNCTION_ISNULL = FunctionDocRegistryItem(
    name="isnull",
    category=CATEGORY_LOGICAL,
    description=_(
        "Returns `TRUE` if {arg:0} is `NULL`, otherwise returns `FALSE`.\n"
        "\n"
        "`{argn:0} IS NOT NULL` returns the opposite result."
    ),
    signature_gen=TemplatedSignatureGenerator(
        signature_templates=[
            SignatureTemplate(title=_("As a function"), body="ISNULL( {1} )"),
            SignatureTemplate(title=_("As an operator"), body="{1} IS [ NOT ] NULL"),
        ]
    ),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_SALES_1,
                formula_fields=[
                    ("sales", "[sales]"),
                    ("result_1", "ISNULL([sales])"),
                    ("result_2", "[sales] IS NULL"),
                    ("result_3", "[sales] IS NOT NULL"),
                ],
            ),
        ),
    ],
)

FUNCTION_IFNULL = FunctionDocRegistryItem(
    name="ifnull",
    category=CATEGORY_LOGICAL,
    description=_("Returns {arg:0} if it's not `NULL`. Otherwise returns {arg:1}."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_SALES_1,
                formula_fields=[
                    ("sales", "[sales]"),
                    ("result", "IFNULL([sales], -100.0)"),
                ],
            ),
        ),
    ],
)

FUNCTION_ISNAN = FunctionDocRegistryItem(
    # FIXME: Disabled
    name="isnan",
    category=CATEGORY_LOGICAL,
    description=_("Returns `TRUE` if {arg:0} is `NaN`, otherwise returns `FALSE`.\n"),
)

FUNCTION_IFNAN = FunctionDocRegistryItem(
    # FIXME: Disabled
    name="ifnan",
    category=CATEGORY_LOGICAL,
    description=_("Returns {arg:0} if it's not `NaN`. Otherwise returns {arg:1}."),
    examples=[
        SimpleExample("IFNAN([Profit], 0)"),
    ],
)

FUNCTION_ZN = FunctionDocRegistryItem(
    name="zn",
    category=CATEGORY_LOGICAL,
    description=_("Returns {arg:0} if it's not `NULL`. Otherwise returns 0."),
    examples=[
        DataExample(
            example_config=ExampleConfig(
                source=_SOURCE_SALES_1,
                formula_fields=[
                    ("sales", "[sales]"),
                    ("result", "ZN([sales])"),
                ],
            ),
        ),
    ],
)

FUNCTION_CASE = FunctionDocRegistryItem(
    name="_case_block_",
    naming_provider=CustomNamingProvider(
        title="CASE",
        internal_name="case",
    ),
    category=CATEGORY_LOGICAL,
    description=_(
        "Compares {arg:0} to {arg:1}, {arg:3}, ... consecutively "
        "and returns the corresponding result for the first match. "
        "If no match is found, it returns {arg:5}."
    ),
    signature_gen=TemplatedSignatureGenerator(
        placement_mode=SignaturePlacement.tabbed,
        signature_templates=(
            SignatureTemplate(
                title=_("As a block"),
                body="""
CASE {1}
    WHEN {2} THEN {3}
    [ WHEN {4} THEN {5}
      ... ]
    ELSE {6}
END""",
            ),
            SignatureTemplate(
                title=_("As a function"),
                body="""
CASE(
    {1},
    {2}, {3},
  [ {4}, {5},
    ... ]
    {6}
)""",
            ),
        ),
    ),
    examples=[
        SimpleExample(
            """
CASE (
    [country],
    "AO", "Angola",
    "AU", "Australia",
    "BY", "Belarus",
    "CA", "Canada",
    "TT", "Trinidad and Tobago",
    "Other Country"
)"""
        ),
        SimpleExample(
            """
CASE [country]
    WHEN "AO" THEN "Angola"
    WHEN "AU" THEN "Australia"
    WHEN "BY" THEN "Belarus"
    WHEN "CA" THEN "Canada"
    WHEN "TT" THEN "Trinidad and Tobago"
    ELSE "Other Country"
END"""
        ),
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with data table"),
                source=_SOURCE_TIME_1,
                formula_fields=[
                    ("unit", "[unit]"),
                    ("case_function", 'CASE([unit], "s", 1, "m", 60, "h", 3600, 0)'),
                    ("case_statement", 'CASE [unit] WHEN "s" THEN 1 WHEN "m" THEN 60 WHEN "h" THEN 3600 ELSE 0 END'),
                ],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTION_IF = FunctionDocRegistryItem(
    name="_if_block_",
    naming_provider=CustomNamingProvider(
        title="IF",
        internal_name="if",
    ),
    category=CATEGORY_LOGICAL,
    description=_(
        "Checks conditional expressions {arg:0}, {arg:1}, ... and returns the "
        "matching result for the first condition found to be `TRUE`. IF all "
        "conditional expressions are `FALSE`, it returns {arg:4}."
    ),
    signature_gen=TemplatedSignatureGenerator(
        placement_mode=SignaturePlacement.tabbed,
        signature_templates=(
            SignatureTemplate(
                title=_("As a block"),
                body="""
IF {1}
    THEN {2}
[ ELSEIF {3}
    THEN {4}
  ... ]
ELSE
    {5}
END""",
            ),
            SignatureTemplate(
                title=_("As a function"),
                body="""
IF(
    {1}, {2},
  [ {3}, {4},
    ... ]
    {5}
)""",
            ),
        ),
    ),
    examples=[
        SimpleExample(
            """
IF
    [Profit] > 100
        THEN "High"
    ELSEIF [Profit] > 25
        THEN "Medium"
    ELSE "Low"
END"""
        ),
        DataExample(
            example_config=ExampleConfig(
                name=_("Example with data table"),
                source=_SOURCE_SALES_1,
                formula_fields=[
                    ("sales", "[sales]"),
                    (
                        "if_function",
                        (
                            'IF(ZN([sales]) < 100, "Less than 100", '
                            '[sales] < 1000, "100 - 1000", '
                            '"1000 and greater")'
                        ),
                    ),
                    (
                        "if_statement",
                        (
                            'IF ZN([sales]) < 100 THEN "Less than 100" '
                            'ELSEIF [sales] < 1000 THEN "100 - 1000" '
                            'ELSE "1000 and greater" END'
                        ),
                    ),
                ],
                formulas_as_names=False,
            ),
        ),
    ],
)

FUNCTIONS_LOGICAL = [
    FUNCTION_ISNULL,
    FUNCTION_IFNULL,
    # FUNCTION_ISNAN,
    # FUNCTION_IFNAN,
    FUNCTION_ZN,
    FUNCTION_CASE,
    FUNCTION_IF,
]
