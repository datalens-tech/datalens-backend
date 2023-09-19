"""
Generate structures used for creating documentation
"""

from __future__ import annotations

from typing import (
    Callable,
    Collection,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import attr

from dl_formula.core.dialect import DialectCombo
from dl_formula_ref.audience import Audience
from dl_formula_ref.registry.aliased_res import AliasedResourceRegistry
from dl_formula_ref.registry.arg_base import FuncArg
from dl_formula_ref.registry.base import FunctionDocCategory
from dl_formula_ref.registry.example_base import ExampleBase
from dl_formula_ref.registry.note import (
    NoteLevel,
    NoteType,
    ParameterizedNote,
)
from dl_formula_ref.registry.signature_base import FunctionSignatureCollection
from dl_formula_ref.registry.text import ParameterizedText


TOP_NOTE_LEVELS: Collection[Tuple[Optional[NoteType], Optional[NoteLevel]]] = [
    (NoteType.REGULAR, NoteLevel.alert),
    (NoteType.REGULAR, NoteLevel.warning),
]
BOTTOM_NOTE_LEVELS: Collection[Tuple[Optional[NoteType], Optional[NoteLevel]]] = [
    (NoteType.REGULAR, NoteLevel.info),
    (NoteType.ARG_RESTRICTION, None),
]


@attr.s(frozen=True)
class RawFunc:
    _name: str = attr.ib(kw_only=True)
    _title_factory: Callable[[str], str] = attr.ib(kw_only=True)
    _short_title_factory: Callable[[str], str] = attr.ib(kw_only=True)
    _internal_name: str = attr.ib(kw_only=True)
    _description: str = attr.ib(kw_only=True)
    _args: List[FuncArg] = attr.ib(kw_only=True)
    _signature_coll: FunctionSignatureCollection = attr.ib(kw_only=True)
    _notes: List[ParameterizedNote] = attr.ib(kw_only=True)
    _return_type: ParameterizedText = attr.ib(kw_only=True)
    _category: FunctionDocCategory = attr.ib(kw_only=True)
    _resources: AliasedResourceRegistry = attr.ib(kw_only=True)
    _examples: Sequence[ExampleBase] = attr.ib(kw_only=True)
    _dialects: Set[DialectCombo] = attr.ib(kw_only=True)

    @property
    def name(self) -> str:
        return self._name.upper()

    @property
    def description(self) -> str:
        return self._description

    @property
    def category(self) -> FunctionDocCategory:
        return self._category

    @property
    def resources(self) -> AliasedResourceRegistry:
        return self._resources

    @property
    def examples(self) -> Sequence[ExampleBase]:
        return self._examples

    @property
    def args(self) -> List[FuncArg]:
        return self._args

    @property
    def return_type(self) -> ParameterizedText:
        return self._return_type

    @property
    def internal_name(self) -> str:
        return self._internal_name

    def get_title(self, locale: str) -> str:
        return self._title_factory(locale)

    def get_short_title(self, locale: str) -> str:
        return self._short_title_factory(locale)

    @property
    def short_title_factory(self) -> Callable[[str], str]:
        return self._short_title_factory

    @property
    def title_factory(self) -> Callable[[str], str]:
        return self._title_factory

    @property
    def signature_coll(self) -> FunctionSignatureCollection:
        return self._signature_coll

    @property
    def dialects(self) -> Set[DialectCombo]:
        return self._dialects

    def _filter_notes(
        self,
        notes: Iterable[ParameterizedNote],
        levels: Collection[Tuple[Optional[NoteType], Optional[NoteLevel]]],
    ) -> List[ParameterizedNote]:
        result: List[ParameterizedNote] = []
        for note in notes or ():
            for note_type_mask, note_level_mask in levels:
                if (note_type_mask is None or note.type == note_type_mask) and (
                    note_level_mask is None or note.level == note_level_mask
                ):
                    result.append(note)
                    break

        return result

    @property
    def bottom_notes(self) -> List[ParameterizedNote]:
        return self._filter_notes(notes=self._notes, levels=BOTTOM_NOTE_LEVELS)

    @property
    def top_notes(self) -> List[ParameterizedNote]:
        return self._filter_notes(notes=self._notes, levels=TOP_NOTE_LEVELS)


@attr.s(frozen=True)
class RawMultiAudienceFunc:
    name: str = attr.ib(kw_only=True)
    category: FunctionDocCategory = attr.ib(kw_only=True)
    internal_name: str = attr.ib(kw_only=True)
    _title_factory: Callable[[str], str] = attr.ib(kw_only=True)
    _short_title_factory: Callable[[str], str] = attr.ib(kw_only=True)
    _aud_dict: dict[Audience, RawFunc] = attr.ib(kw_only=True)

    def items(self) -> Iterable[tuple[Audience, RawFunc]]:
        items: list[tuple[Audience, RawFunc]] = list(self._aud_dict.items())
        return sorted(items, key=lambda item: item[0].name)

    def keys(self) -> Iterable[Audience]:
        return self._aud_dict.keys()

    def values(self) -> Iterable[RawFunc]:
        return self._aud_dict.values()

    def get(self, audience: Audience, default: Optional[RawFunc] = None) -> Optional[RawFunc]:
        return self._aud_dict.get(audience, default)

    def get_title(self, locale: str) -> str:
        return self._title_factory(locale)

    def get_short_title(self, locale: str) -> str:
        return self._short_title_factory(locale)

    @classmethod
    def from_aud_dict(cls, aud_dict: dict[Audience, RawFunc]) -> RawMultiAudienceFunc:
        names: set[str] = set()
        internal_names: set[str] = set()
        for _audience, raw_func in aud_dict.items():
            names.add(raw_func.name)
            internal_names.add(raw_func.internal_name)

        assert len(names) == 1
        assert len(internal_names) == 1

        one_raw_func = next(iter(aud_dict.values()))

        return cls(
            name=next(iter(names)),
            category=one_raw_func.category,
            internal_name=next(iter(internal_names)),
            aud_dict=aud_dict,
            title_factory=one_raw_func.short_title_factory,
            short_title_factory=one_raw_func.short_title_factory,
        )
