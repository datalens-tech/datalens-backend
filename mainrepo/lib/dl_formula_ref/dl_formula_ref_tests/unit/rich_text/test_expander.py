from typing import (
    Optional,
    Tuple,
)

from dl_formula.core.datatype import DataType
from dl_formula_ref.registry.aliased_res import (
    AliasedLinkResource,
    AliasedTableResource,
    AliasedTextResource,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.arg_base import FuncArg
from dl_formula_ref.rich_text.elements import (
    AudienceBlock,
    CodeSpanTextElement,
    ConditionalBlock,
    ExtMacroTextElement,
    LinkTextElement,
    ListTextElement,
)
from dl_formula_ref.rich_text.elements import (
    NoteBlock,
    RichText,
    TableTextElement,
    TermTextElement,
)
from dl_formula_ref.rich_text.elements import MacroReplacementKey as MRK
from dl_formula_ref.rich_text.expander import MacroExpander


def test_expand_macro_dialects():
    expander = MacroExpander()
    text = "qwerty {dialects: CLICKHOUSE} uiop"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 29): ListTextElement(
                items=[TermTextElement(term="ClickHouse")],
                sep=", ",
            ),
        },
    )
    assert actual_res == exp_res


def test_expand_macro_type():
    expander = MacroExpander()
    text = "qwerty {type: INTEGER | STRING} uiop"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 31): ListTextElement(
                items=[TermTextElement(term="Integer", wrap=False), TermTextElement(term="String", wrap=False)],
                sep=" | ",
                wrap=True,
            ),
        },
    )
    assert actual_res == exp_res


def test_expand_macro_ext_macro():
    expander = MacroExpander()
    text = "qwerty {macro: some_cloud_macro} uiop"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 32): ExtMacroTextElement(macro_name="some_cloud_macro"),
        },
    )
    assert actual_res == exp_res


def test_expand_macro_text():
    resources = SimpleAliasedResourceRegistry(
        resources={
            "my_text": AliasedTextResource(body="Some {text: nested_text} Text"),
            "nested_text": AliasedTextResource(body="Nested"),
        }
    )
    expander = MacroExpander(resources=resources)
    text = "qwerty {text: my_text} uiop"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 22): RichText(
                "Some {text: nested_text} Text",
                replacements={
                    MRK(5, 24): RichText("Nested"),
                },
            ),
        },
    )
    assert actual_res == exp_res


def test_expand_macro_link():
    resources = SimpleAliasedResourceRegistry(
        resources={
            "my_link": AliasedLinkResource(url="http://some.url"),
        }
    )
    expander = MacroExpander(resources=resources)
    text = "qwerty {link: my_link: My Link} uiop"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 31): LinkTextElement(url="http://some.url", text="My Link"),
        },
    )
    assert actual_res == exp_res


def test_expand_macro_table():
    resources = SimpleAliasedResourceRegistry(
        resources={
            "my_table": AliasedTableResource(table_body=[["Some {text: nested_text} Text", "Something"]]),
            "nested_text": AliasedTextResource(body="Nested"),
        }
    )
    expander = MacroExpander(resources=resources)
    text = "qwerty {table: my_table} uiop"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 24): TableTextElement(
                table_body=[
                    [
                        RichText("Some {text: nested_text} Text", replacements={MRK(5, 24): RichText("Nested")}),
                        RichText("Something"),
                    ],
                ]
            ),
        },
    )
    assert actual_res == exp_res


def test_expand_macro_arg_argn():
    expander = MacroExpander(
        args=[
            FuncArg(name="first_arg", types={DataType.STRING}, optional_level=0),
            FuncArg(name="second_arg", types={DataType.STRING}, optional_level=0),
        ]
    )
    text = "qwerty {argn: 1} uiop {arg: 0}"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 16): CodeSpanTextElement(text="second_arg", wrap=False),
            MRK(22, 30): CodeSpanTextElement(text="first_arg", wrap=True),
        },
    )
    assert actual_res == exp_res


def test_expand_macro_ref():
    def func_link_provider(func_name: str, category_name: str) -> Tuple[str, str]:
        return (func_name, f'https://{func_name.lower()}{"-win" if category_name == "window" else ""}.funcs.com')

    def cat_link_provider(category_name: str, anchor_name: Optional[str]) -> Tuple[str, str]:
        return (category_name, f"https://{category_name.lower()}.cats.com")

    expander = MacroExpander(
        func_link_provider=func_link_provider,
        cat_link_provider=cat_link_provider,
    )
    text = "qwerty {ref: SUM} uiop {ref: window/AVG} asdf {ref: aggregation/AVG:aggregate function}"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 17): LinkTextElement(url="https://sum.funcs.com", text="SUM"),
            MRK(23, 40): LinkTextElement(url="https://avg-win.funcs.com", text="AVG"),
            MRK(46, 87): LinkTextElement(url="https://avg.funcs.com", text="aggregate function"),
        },
    )
    assert actual_res == exp_res


def test_conditional_block():
    expander = MacroExpander()
    text = "qwerty {if some_cond} uiop{end}"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 31): ConditionalBlock(
                condition="some_cond",
                rich_text=RichText(text=" uiop", replacements={}),
            ),
        },
    )
    assert actual_res == exp_res


def test_nested_conditional_block():
    expander = MacroExpander()
    text = "qwerty {if some_cond} uio{if smth_else}pp{end}p{end}"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 52): ConditionalBlock(
                condition="some_cond",
                rich_text=RichText(
                    text=" uio{if smth_else}pp{end}p",
                    replacements={
                        MRK(4, 25): ConditionalBlock(
                            condition="smth_else",
                            rich_text=RichText(text="pp", replacements={}),
                        ),
                    },
                ),
            ),
        },
    )
    assert actual_res == exp_res


def test_note_block():
    expander = MacroExpander()
    text = "qwerty {note warning} uiop{end}"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 31): NoteBlock(
                level="warning",
                rich_text=RichText(text=" uiop", replacements={}),
            ),
        },
    )
    assert actual_res == exp_res


def test_audience_block():
    expander = MacroExpander()
    text = "qwerty {audience internal} uiop{end}"
    actual_res = expander.expand_text(text)
    exp_res = RichText(
        text=text,
        replacements={
            MRK(7, 36): AudienceBlock(
                audience_types=["internal"],
                rich_text=RichText(text=" uiop", replacements={}),
            ),
        },
    )
    assert actual_res == exp_res
