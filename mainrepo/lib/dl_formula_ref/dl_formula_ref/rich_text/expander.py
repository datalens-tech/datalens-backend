from functools import singledispatchmethod
import re
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

import attr

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import get_dialect_from_str
from dl_formula_ref.registry.aliased_res import (
    AliasedResourceRegistryBase,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.arg_base import FuncArg
from dl_formula_ref.rich_text.elements import (
    AudienceBlock,
    BaseTextElement,
    CodeSpanTextElement,
    ConditionalBlock,
    ExtMacroTextElement,
    LinkTextElement,
    ListTextElement,
    MacroReplacementKey,
    NoteBlock,
    RichText,
    TableTextElement,
    TermTextElement,
)
from dl_formula_ref.rich_text.helpers import get_human_data_type_list
from dl_formula_ref.rich_text.macro import (
    DOUBLE_ARG_MACROS,
    LIST_MACROS,
    SINGLE_ARG_MACROS,
    ArgMacro,
    ArgNMacro,
    BaseMacro,
    CategoryMacro,
    DialectsMacro,
    ExtMacroMacro,
    LinkMacro,
    RefMacro,
    TableMacro,
    TextMacro,
    TypeMacro,
)
from dl_formula_ref.texts import (
    HUMAN_DIALECTS,
    DialectStyle,
)
from dl_i18n.localizer_base import Translatable


_MACRO_CHOICES = "|".join(("ref", "link", "text", "table", "dialects", "arg", "argn", "type", "macro", "category"))
# 3 kinds of macros:
#     {type: single_value}
#     {type: first_value: second_value}
#     {type: value_1 | value_2 | value_3 | ...}
SIMPLE_MACRO_RE = re.compile(
    r"\{\s*"
    r"(?P<type>" + _MACRO_CHOICES + r")"
    r"\s*:\s*"
    r"(?P<first_value>[^{}:|`]+)"
    r"(|:(?P<second_value>[^{}:|`]+)|(?P<list>(\|[^{}:|`]+)+))"
    r"\}"
)
_BLOCK_MACRO_CHOICES = "|".join(
    (
        "if",
        "note",
        "audience",
        "end",
    )
)
END_TAG = "end"
BLOCK_TAG_RE = re.compile(r"\{\s*(?P<tag_name>" + _BLOCK_MACRO_CHOICES + r")(\s+(?P<block_param>[^}]+))?\s*\}")


@attr.s
class MacroExpander:
    _resources: AliasedResourceRegistryBase = attr.ib(kw_only=True, factory=SimpleAliasedResourceRegistry)
    _func_link_provider: Optional[Callable[[str, Optional[str]], Tuple[str, str]]] = attr.ib(kw_only=True, default=None)
    _cat_link_provider: Optional[Callable[[str, Optional[str]], Tuple[str, str]]] = attr.ib(kw_only=True, default=None)
    _args: Optional[List[FuncArg]] = attr.ib(kw_only=True, default=None)
    _translation_callable: Callable[[str | Translatable], str] = attr.ib(kw_only=True, default=lambda s: s)

    def _get_raw_link_url_by_alias(self, alias: str) -> str:
        return self._resources.get_link(alias).url

    def _get_raw_text_body_by_alias(self, alias: str) -> str:
        return self._resources.get_text(alias).body

    def _get_raw_table_body_by_alias(self, alias: str) -> list[list[str | Translatable]]:
        return self._resources.get_table(alias).table_body

    def _get_func_name_and_url(self, func_name: str, category_name: Optional[str] = None) -> Tuple[str, str]:
        assert self._func_link_provider is not None
        return self._func_link_provider(func_name, category_name)

    def _get_cat_name_and_url(self, category_name: str, anchor_name: Optional[str] = None) -> Tuple[str, str]:
        assert self._cat_link_provider is not None
        return self._cat_link_provider(category_name, anchor_name)

    def _translate_text(self, text: str | Translatable) -> str:
        if isinstance(text, str) and not text:
            return text
        return self._translation_callable(text)

    def expand_text(self, text: str | Translatable) -> RichText:
        trans_text = self._translate_text(text)
        replacements: Dict[MacroReplacementKey, BaseTextElement] = {}

        block_replacements = self._get_block_replacements(trans_text)
        replacements.update(block_replacements)
        macro_replacements = self._get_macro_replacements(trans_text, block_replacements)
        replacements.update(macro_replacements)

        return RichText(text=trans_text, replacements=replacements)

    def _make_block_macro(
        self,
        tag_name: str,
        block_param: Optional[str],
        block_text: str,
        nested_replacements: Dict[MacroReplacementKey, BaseTextElement],
    ) -> BaseTextElement:
        element: BaseTextElement

        if tag_name == "if":
            assert block_param is not None
            element = ConditionalBlock(
                condition=block_param,
                rich_text=RichText(text=block_text, replacements=nested_replacements),
            )
        elif tag_name == "note":
            block_param = block_param or "info"
            assert block_param is not None
            element = NoteBlock(
                level=block_param,
                rich_text=RichText(text=block_text, replacements=nested_replacements),
            )
        elif tag_name == "audience":
            block_param = block_param
            assert block_param is not None
            audiences = [item.strip() for item in block_param.split((","))]
            element = AudienceBlock(
                audience_types=audiences,
                rich_text=RichText(text=block_text, replacements=nested_replacements),
            )
        else:
            raise ValueError(f"Unknown block tag: {tag_name}")
        return element

    def _get_block_replacements(self, text: str) -> Dict[MacroReplacementKey, BaseTextElement]:
        block_replacements: Dict[MacroReplacementKey, BaseTextElement] = {}

        block_match: Optional[re.Match] = BLOCK_TAG_RE.search(text)
        while block_match is not None and block_match.group("tag_name") != END_TAG:
            tag_name: str = block_match.group("tag_name")
            block_param: Optional[str] = block_match.group("block_param")
            block_text_start_pos = block_match.end()
            nested_block_replacements = self._get_block_replacements(text[block_text_start_pos:])

            nested_end_pos: int = 0
            if nested_block_replacements:
                nested_end_pos = max([nest_repl_key.end_pos for nest_repl_key in nested_block_replacements])

            block_end_match = BLOCK_TAG_RE.search(text, block_text_start_pos + nested_end_pos)
            assert block_end_match is not None
            assert block_end_match.group("tag_name") == END_TAG
            repl_key = MacroReplacementKey(
                start_pos=block_match.start(),
                end_pos=block_end_match.end(),
            )
            nested_text = text[block_match.end() : block_end_match.start()]
            nested_macro_replacements = self._get_macro_replacements(nested_text, nested_block_replacements)

            block_replacement = self._make_block_macro(
                tag_name=tag_name,
                block_param=block_param,
                block_text=nested_text,
                nested_replacements={
                    **nested_block_replacements,
                    **nested_macro_replacements,
                },
            )
            block_replacements[repl_key] = block_replacement

            block_match = BLOCK_TAG_RE.search(text, block_end_match.end())

        return block_replacements

    def _get_macro_replacements(
        self,
        text: str,
        block_replacements: Dict[MacroReplacementKey, BaseTextElement],
    ) -> Dict[MacroReplacementKey, BaseTextElement]:
        replacements: Dict[MacroReplacementKey, BaseTextElement] = {}

        for macro_match in SIMPLE_MACRO_RE.finditer(text):
            start_pos = macro_match.start()
            for block_repl_key in block_replacements:
                if block_repl_key.start_pos <= start_pos < block_repl_key.end_pos:
                    # skip macros inside blocks that are already being replaced
                    continue
            repl_key = MacroReplacementKey(
                start_pos=macro_match.start(),
                end_pos=macro_match.end(),
            )
            macro = self._make_macro(macro_match)
            expanded_element = self.expand_macro(macro)
            replacements[repl_key] = expanded_element

        return replacements

    def _make_macro(self, macro_match: re.Match) -> BaseMacro:
        macro_type: str = macro_match.group("type").strip()
        first_value: str = macro_match.group("first_value").strip()
        list_str: Optional[str] = macro_match.group("list")
        second_value: Optional[str] = macro_match.group("second_value")

        if macro_type in LIST_MACROS:
            value_list = [first_value]
            if list_str is not None:
                value_list += [s.strip() for s in list_str.split("|")[1:]]
            return LIST_MACROS[macro_type](values=value_list)

        if macro_type in DOUBLE_ARG_MACROS:
            if second_value is not None:
                second_value = second_value.strip()
            return DOUBLE_ARG_MACROS[macro_type](arg=first_value, second_arg=second_value)

        return SINGLE_ARG_MACROS[macro_type](arg=first_value)

    @singledispatchmethod
    def expand_macro(self, macro: BaseMacro) -> BaseTextElement:
        raise TypeError(type(macro))

    @expand_macro.register
    def expand_macro_ref(self, macro: RefMacro) -> LinkTextElement:
        # In theory, we can easily support anchors here just like in category links
        assert macro.arg.count("/") <= 1, 'The format is "[category_name/]function_name", got "{}"'.format(macro.arg)
        if macro.arg.find("/") > -1:
            macro.category_name, macro.arg = macro.arg.split("/")
        func_name, url = self._get_func_name_and_url(func_name=macro.arg, category_name=macro.category_name)
        text: str = func_name
        if macro.second_arg is not None:
            text = self._translate_text(macro.second_arg)
        return LinkTextElement(text=text, url=url)

    @expand_macro.register
    def expand_macro_category(self, macro: CategoryMacro) -> LinkTextElement:
        assert macro.arg.count("#") <= 1, 'The format is "category_name[#anchor_name]", got "{}"'.format(macro.arg)
        if macro.arg.find("#") > -1:
            macro.arg, macro.anchor_name = macro.arg.split("#")
        cat_name, url = self._get_cat_name_and_url(category_name=macro.arg, anchor_name=macro.anchor_name)
        text: str = cat_name
        if macro.second_arg is not None:
            text = self._translate_text(macro.second_arg)
        return LinkTextElement(text=text, url=url)

    @expand_macro.register
    def expand_macro_link(self, macro: LinkMacro) -> LinkTextElement:
        text: str = macro.arg
        if macro.second_arg is not None:
            text = self._translate_text(macro.second_arg)
        return LinkTextElement(
            text=text,
            url=self._translate_text(self._get_raw_link_url_by_alias(macro.arg)),
        )

    @expand_macro.register
    def expand_macro_text(self, macro: TextMacro) -> RichText:
        return self.expand_text(self._get_raw_text_body_by_alias(macro.arg))

    @expand_macro.register
    def expand_macro_table(self, macro: TableMacro) -> TableTextElement:
        raw_table_body = self._get_raw_table_body_by_alias(macro.arg)
        return TableTextElement(table_body=[[self.expand_text(cell) for cell in row] for row in raw_table_body])

    @expand_macro.register
    def expand_macro_dialects(self, macro: DialectsMacro) -> ListTextElement:
        items: list[TermTextElement] = []
        for dialect_name in macro.values:
            dialect = get_dialect_from_str(dialect_name)
            human_dialect = HUMAN_DIALECTS[dialect].for_style(DialectStyle.split_version)
            # FIXME: Temporary hack for removing backticks:
            human_dialect = self._translate_text(human_dialect)[1:-1]
            items.append(TermTextElement(term=human_dialect))

        return ListTextElement(items=items, sep=", ")

    @expand_macro.register
    def expand_macro_type(self, macro: TypeMacro) -> ListTextElement:
        items: List[TermTextElement] = []
        type_list = [DataType[type_name.strip().upper()] for type_name in macro.values]
        for h_type_name in get_human_data_type_list(types=type_list):
            items.append(
                TermTextElement(
                    term=self._translate_text(h_type_name),
                    wrap=False,
                )
            )

        return ListTextElement(items=items, sep=" | ", wrap=True)

    @expand_macro.register
    def expand_macro_arg(self, macro: ArgMacro) -> CodeSpanTextElement:
        assert self._args
        return CodeSpanTextElement(
            text=self._args[int(macro.arg)].name,
            wrap=True,
        )

    @expand_macro.register
    def expand_macro_argn(self, macro: ArgNMacro) -> CodeSpanTextElement:
        assert self._args
        return CodeSpanTextElement(
            text=self._args[int(macro.arg)].name,
            wrap=False,
        )

    @expand_macro.register
    def expand_macro_ext_macro(self, macro: ExtMacroMacro) -> ExtMacroTextElement:
        return ExtMacroTextElement(macro_name=macro.arg)
