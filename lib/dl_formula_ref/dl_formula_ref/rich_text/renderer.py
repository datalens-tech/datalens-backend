from __future__ import annotations

import abc
from functools import singledispatchmethod
from typing import (
    Mapping,
    Optional,
)

import attr
import tabulate

from dl_formula_ref.rich_text.elements import (
    AudienceBlock,
    CodeSpanTextElement,
    ConditionalBlock,
    ExtMacroTextElement,
    LinkTextElement,
    ListTextElement,
    NoteBlock,
    RichText,
    TableTextElement,
    TermTextElement,
)
from dl_formula_ref.rich_text.helpers import escape_cell


@attr.s
class RichTextRenderEnvironment:
    block_conditions: Mapping[str, bool] = attr.ib(kw_only=True, factory=dict)


class RichTextRenderer(abc.ABC):
    @abc.abstractmethod
    def render(self, rtext: RichText, env: Optional[RichTextRenderEnvironment] = None) -> str:
        raise NotImplementedError


class MdRichTextRenderer(RichTextRenderer):
    def render(self, rtext: RichText, env: Optional[RichTextRenderEnvironment] = None) -> str:
        if env is None:
            env = RichTextRenderEnvironment()
        assert env is not None
        return self._render(rtext, env=env)

    @singledispatchmethod
    def _render(self, value, env: RichTextRenderEnvironment) -> str:  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        raise TypeError

    @_render.register
    def _render_rich_text(self, value: RichText, env: RichTextRenderEnvironment) -> str:
        parts: list[str] = []
        last_idx = 0
        for key, element in sorted(value.replacements.items()):
            parts.append(value.text[last_idx : key.start_pos])
            parts.append(self._render(element, env=env))
            last_idx = key.end_pos

        parts.append(value.text[last_idx:])
        return "".join(parts)

    @_render.register
    def _render_term(self, value: TermTextElement, env: RichTextRenderEnvironment) -> str:
        if value.wrap:
            return f"`{value.term}`"
        return value.term

    @_render.register
    def _render_code_span(self, value: CodeSpanTextElement, env: RichTextRenderEnvironment) -> str:
        if value.wrap:
            return f"`{value.text}`"
        return value.text

    @_render.register
    def _render_ext_macro(self, value: ExtMacroTextElement, env: RichTextRenderEnvironment) -> str:
        return f"{{{{ {value.macro_name} }}}}"

    @_render.register
    def _render_list(self, value: ListTextElement, env: RichTextRenderEnvironment) -> str:
        list_str = value.sep.join([self._render(item, env=env) for item in value.items])
        if value.wrap:
            list_str = f"`{list_str}`"
        return list_str

    @_render.register
    def _render_table(self, value: TableTextElement, env: RichTextRenderEnvironment) -> str:
        table_data = value.table_body
        return tabulate.tabulate(
            [[escape_cell(self._render(cell, env=env)) for cell in row] for row in table_data[1:]],
            headers=[escape_cell(self._render(cell, env=env)) for cell in table_data[0]],
            tablefmt="pipe",
        )

    @_render.register
    def _render_link(self, value: LinkTextElement, env: RichTextRenderEnvironment) -> str:
        return f"[{value.text}]({value.url})"

    @_render.register
    def _render_conditional_block(self, value: ConditionalBlock, env: RichTextRenderEnvironment) -> str:
        if env.block_conditions.get(value.condition):
            return self._render(value.rich_text, env=env)
        return ""

    @_render.register
    def _render_note_block(self, value: NoteBlock, env: RichTextRenderEnvironment) -> str:
        content = self._render(value.rich_text, env=env)
        return f"{{% note {value.level} %}}{content}{{% endnote %}}"

    @_render.register
    def _render_audience(self, value: AudienceBlock, env: RichTextRenderEnvironment) -> str:
        content = self._render(value.rich_text, env=env)
        if len(value.audience_types) == 1:
            return f"{{% if audience == '{value.audience_types[0]}' %}}{content}{{% endif %}}"

        audience_types_str = "[{}]".format(", ".join(["'{}'".format(aud) for aud in value.audience_types]))
        return f"{{% if audience in {audience_types_str} %}}{content}{{% endif %}}"
