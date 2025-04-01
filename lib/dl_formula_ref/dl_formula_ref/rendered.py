from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Iterable,
    NamedTuple,
    Optional,
)

import attr

from dl_formula_ref.primitives import (
    NoteLevel,
    NoteType,
)


if TYPE_CHECKING:
    from dl_formula.core.dialect import DialectCombo
    from dl_formula_ref.audience import Audience
    from dl_formula_ref.registry.arg_base import FuncArg
    from dl_formula_ref.registry.signature_base import FunctionSignatureCollection


class LocalizedFuncArg(NamedTuple):
    arg: FuncArg
    locale: str
    human_type: str

    @property
    def name(self) -> str:
        return self.arg.name


@attr.s
class RenderedNote:
    text: str = attr.ib(kw_only=True)
    level: NoteLevel = attr.ib(kw_only=True, default=NoteLevel.info)
    formatting: bool = attr.ib(kw_only=True, default=True)
    type: NoteType = attr.ib(kw_only=True, default=NoteType.REGULAR)


class RenderedFunc(NamedTuple):
    # The content (e.g. cross-reference links) is path dependent,
    # and so is the rendered function, so tha t is why it contains its own path
    file_path: str
    name: str
    title: str
    short_title: str
    internal_name: str
    category_name: str
    human_category: str
    category_description: str
    category_keywords: list[str]
    args: list[LocalizedFuncArg]
    dialects: set[DialectCombo]
    human_dialects: list[str]
    description: str
    top_notes: list[RenderedNote]
    bottom_notes: list[RenderedNote]
    return_type: str
    examples: list[str]
    signature_coll: FunctionSignatureCollection
    locale: str
    crosslink_note: Optional[RenderedNote]

    @property
    def const_args(self) -> list[LocalizedFuncArg]:
        return [a for a in self.args if a.arg.is_const]

    @property
    def multi_signature(self) -> bool:
        return len(self.signature_coll.signatures) > 1

    @property
    def category_description_short(self) -> str:
        first_sentence = self.category_description.split(".", 1)[0]
        if not first_sentence:
            return ""
        return f"{first_sentence}."


@attr.s(frozen=True)
class RenderedMultiAudienceFunc:
    name: str = attr.ib(kw_only=True)
    category_name: str = attr.ib(kw_only=True)
    category_description: str = attr.ib(kw_only=True)
    category_description_short: str = attr.ib(kw_only=True)
    category_keywords: list[str] = attr.ib(kw_only=True)
    file_path: str = attr.ib(kw_only=True)
    _aud_dict: dict[Audience, RenderedFunc] = attr.ib(kw_only=True)

    def items(self) -> Iterable[tuple[Audience, RenderedFunc]]:
        items: list[tuple[Audience, RenderedFunc]] = list(self._aud_dict.items())
        return sorted(items, key=lambda item: item[0].name)
