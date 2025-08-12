from typing import (
    NamedTuple,
    Sequence,
)

import attr


class MacroReplacementKey(NamedTuple):
    start_pos: int
    end_pos: int

    @property
    def pos(self) -> slice:
        return slice(self.start_pos, self.end_pos)


@attr.s(frozen=True)
class BaseTextElement:
    pass


@attr.s(frozen=True)
class RichText(BaseTextElement):
    text: str = attr.ib()
    replacements: dict[MacroReplacementKey, BaseTextElement] = attr.ib(kw_only=True, factory=dict)


@attr.s(frozen=True)
class ConditionalBlock(BaseTextElement):
    condition: str = attr.ib()
    rich_text: RichText = attr.ib()


@attr.s(frozen=True)
class NoteBlock(BaseTextElement):
    level: str = attr.ib()
    rich_text: RichText = attr.ib()


@attr.s(frozen=True)
class AudienceBlock(BaseTextElement):
    audience_types: list[str] = attr.ib()
    rich_text: RichText = attr.ib()


@attr.s(frozen=True)
class LinkTextElement(BaseTextElement):
    text: str = attr.ib(kw_only=True)
    url: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class TableTextElement(BaseTextElement):
    table_body: list[list[RichText]] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ListTextElement(BaseTextElement):
    items: Sequence[BaseTextElement] = attr.ib(kw_only=True)
    sep: str = attr.ib(kw_only=True)
    wrap: bool = attr.ib(kw_only=True, default=False)


@attr.s(frozen=True)
class TermTextElement(BaseTextElement):
    term: str = attr.ib(kw_only=True)
    wrap: bool = attr.ib(kw_only=True, default=True)


@attr.s(frozen=True)
class CodeSpanTextElement(BaseTextElement):
    text: str = attr.ib(kw_only=True)
    wrap: bool = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ExtMacroTextElement(BaseTextElement):
    macro_name: str = attr.ib(kw_only=True)
