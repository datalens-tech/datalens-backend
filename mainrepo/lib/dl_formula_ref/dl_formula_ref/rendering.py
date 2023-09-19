"""
Generate structures used for creating documentation
"""

from __future__ import annotations

from collections import defaultdict
import os.path
from typing import (
    TYPE_CHECKING,
    Collection,
    Mapping,
    Optional,
    Union,
)

import attr

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D
from dl_formula_ref.audience import Audience
from dl_formula_ref.i18n.registry import get_localizer
from dl_formula_ref.paths import (
    FileLink,
    PathRenderer,
)
from dl_formula_ref.primitives import RawFunc
from dl_formula_ref.reference import FuncReference
from dl_formula_ref.registry.note import (
    CrosslinkNote,
    ParameterizedNote,
)
from dl_formula_ref.registry.registry import RefFunctionKey
from dl_formula_ref.registry.signature_base import (
    FunctionSignature,
    FunctionSignatureCollection,
)
from dl_formula_ref.registry.text import ParameterizedText
from dl_formula_ref.rendered import (
    LocalizedFuncArg,
    RenderedFunc,
    RenderedMultiAudienceFunc,
)
from dl_formula_ref.rich_text.expander import MacroExpander
from dl_formula_ref.rich_text.helpers import get_human_data_type_list
from dl_formula_ref.rich_text.renderer import (
    MdRichTextRenderer,
    RichTextRenderEnvironment,
)
from dl_formula_ref.texts import (
    ALSO_IN_OTHER_CATEGORIES,
    ANY_DIALECTS,
    HIDDEN_DIALECTS,
    HUMAN_CATEGORIES,
    HUMAN_DIALECTS,
    DialectStyle,
)


if TYPE_CHECKING:
    from dl_formula_ref.registry.example_base import DataExampleRendererConfig


def translate(text: str, locale: str) -> str:
    gtext = get_localizer(locale).translate
    return gtext(text) if text else ""


def human_dialect(dialect: Union[DialectCombo, str], locale: str, style: DialectStyle = DialectStyle.simple) -> str:
    if isinstance(dialect, str):
        dialect = D.by_name(dialect.upper())

    return "{}".format(translate(HUMAN_DIALECTS[dialect].for_style(style), locale=locale))


def human_data_type(types: Collection[DataType], locale: str) -> str:
    result_types = get_human_data_type_list(types=types)
    raw_type_str = " | ".join(translate(t, locale=locale) for t in sorted(result_types))
    return f"`{raw_type_str}`"


def human_category(category: str, locale: str) -> str:
    return translate(HUMAN_CATEGORIES.get(category, category), locale=locale)


def human_dialects(dialects: set[DialectCombo], locale: str) -> list[str]:
    dialect_mask = D.EMPTY
    for dialect in dialects:
        if dialect == D.ANY:
            for any_dialect in ANY_DIALECTS:
                dialect_mask |= any_dialect
        else:
            dialect_mask |= dialect

    dialects_by_db = defaultdict(list)
    for dialect in dialect_mask.to_list():
        dialects_by_db[dialect.single_bit.name].append(dialect)

    sorted_dialects = sorted(
        [sorted(dialects_for_db, key=lambda d: d.single_bit.version)[0] for dialects_for_db in dialects_by_db.values()],
        key=lambda d: d.common_name_and_version,
    )

    h_dialects: list[str] = [human_dialect(d, locale=locale) for d in sorted_dialects if d not in HIDDEN_DIALECTS]
    return h_dialects


@attr.s
class FuncRenderer:
    _func_ref: FuncReference = attr.ib(kw_only=True)
    _locale: str = attr.ib(kw_only=True)
    _example_rend_config: DataExampleRendererConfig = attr.attrib(kw_only=True)
    _path_renderer: PathRenderer = attr.ib(kw_only=True)
    _rt_renderer: MdRichTextRenderer = attr.ib(init=False, factory=MdRichTextRenderer)
    _block_conditions: Mapping[str, bool] = attr.ib(kw_only=True, factory=dict)
    _rt_env: RichTextRenderEnvironment = attr.ib(kw_only=True)

    @_rt_env.default
    def _rt_env_default(self) -> RichTextRenderEnvironment:
        return RichTextRenderEnvironment(block_conditions=self._block_conditions)

    @property
    def path_renderer(self) -> PathRenderer:
        return self._path_renderer

    def render_text(self, context_path: str, raw_func: RawFunc, text: Union[str, ParameterizedText]) -> str:
        if not text:
            return ""
        if isinstance(text, ParameterizedText):
            text = ParameterizedText(
                text=translate(text.text, locale=self._locale),
                params=text.params,
            ).format()
        translation_callable = get_localizer(self._locale).translate
        relative_path_renderer = self._path_renderer.child(context_path)
        expander = MacroExpander(
            resources=raw_func.resources,
            args=raw_func.args,
            translation_callable=translation_callable,
            func_link_provider=relative_path_renderer.get_func_link,
            cat_link_provider=relative_path_renderer.get_cat_link,
        )
        rich_text = expander.expand_text(text)
        result = self._rt_renderer.render(rich_text, env=self._rt_env)
        return result

    def get_counterpart_keys(self, func_key: RefFunctionKey) -> list[RefFunctionKey]:
        return [
            RefFunctionKey.normalized(name=key.name, category_name=key.category.name)
            for key in self._func_ref.filter(name=func_key.name.upper())
            if key.category.name != func_key.category_name
        ]

    def get_counterpart_crosslink_note_and_refs(self, func_key: RefFunctionKey) -> Optional[CrosslinkNote]:
        counterpart_keys = self.get_counterpart_keys(func_key=func_key)
        if not counterpart_keys:
            return None

        counterpart_ref = "{{ref:{category_name}/{func_name}:{human_category}}}"
        counterpart_refs_list = [
            counterpart_ref.format(
                category_name=key.category_name,
                func_name=func_key.name.upper(),
                human_category=HUMAN_CATEGORIES[key.category_name],
            )
            for key in counterpart_keys
        ]

        note = CrosslinkNote(
            param_text=ParameterizedText(
                text=ALSO_IN_OTHER_CATEGORIES,
                params={"func_name": func_key.name.upper()},
            ),
            ref_list=counterpart_refs_list,
        )

        return note

    def get_func_link(self, func_name: str, category_name: str, anchor_name: Optional[str] = None) -> FileLink:
        return self._path_renderer.get_func_link(
            func_name=func_name,
            category_name=category_name,
            anchor_name=anchor_name,
        )

    def get_cat_link(self, category_name: str, anchor_name: Optional[str] = None) -> FileLink:
        return self._path_renderer.get_cat_link(
            category_name=category_name,
            anchor_name=anchor_name,
        )

    def render_func(self, func_key: RefFunctionKey, raw_func: RawFunc) -> RenderedFunc:
        gtext = get_localizer(self._locale).translate
        func_file_path = self._path_renderer.get_func_path(func_key=func_key)
        context_path = os.path.dirname(func_file_path)

        def _render_text(text: Union[str, ParameterizedText]) -> str:
            return self.render_text(context_path=context_path, raw_func=raw_func, text=text)

        def _render_note(note: ParameterizedNote) -> ParameterizedNote:
            text = _render_text(note.param_text)
            return note.clone(param_text=ParameterizedText(text=text))

        def _render_crosslink_note(note: CrosslinkNote) -> ParameterizedNote:
            text = _render_text(note.param_text)
            refs_list = ",".join(_render_text(ref) for ref in note.ref_list)
            new_text = text + refs_list + "."
            return note.clone(param_text=ParameterizedText(text=new_text))

        def _make_keywords(keyword_list_str: str) -> list[str]:
            if not keyword_list_str:
                return []
            keyword_list_str = gtext(keyword_list_str)
            return [kw.strip() for kw in keyword_list_str.split(",")]

        crosslink_note = self.get_counterpart_crosslink_note_and_refs(func_key=func_key)

        # Place examples under cut only if there are more than one
        ex_under_cut = len(raw_func.examples) > 1
        examples = [
            ex.render(
                func_name=raw_func.internal_name,
                config=self._example_rend_config,
                locale=self._locale,
                under_cut=ex_under_cut,
            )
            for ex in raw_func.examples
        ]
        signature_coll = FunctionSignatureCollection(
            signatures=[
                FunctionSignature(
                    title=sign.title,
                    body=sign.body,
                    description=[_render_text(desc_part) for desc_part in sign.description],
                )
                for sign in raw_func.signature_coll.signatures
            ],
            placement_mode=raw_func.signature_coll.placement_mode,
        )

        return RenderedFunc(
            file_path=func_file_path,
            name=raw_func.name,
            internal_name=raw_func.internal_name,
            title=raw_func.get_title(locale=self._locale),
            short_title=raw_func.get_short_title(locale=self._locale),
            category_name=raw_func.category.name or "",
            human_category=human_category(category=raw_func.category.name, locale=self._locale),
            category_description=_render_text(raw_func.category.description),
            category_keywords=_make_keywords(raw_func.category.keywords),
            args=[
                LocalizedFuncArg(
                    arg=arg, locale=self._locale, human_type=human_data_type(types=arg.types, locale=self._locale)
                )
                for arg in raw_func.args
            ],
            description=_render_text(raw_func.description),
            dialects=raw_func.dialects,
            human_dialects=human_dialects(dialects=raw_func.dialects, locale=self._locale),
            top_notes=[_render_note(w) for w in raw_func.top_notes],
            bottom_notes=[_render_note(n) for n in raw_func.bottom_notes[:]],
            return_type=_render_text(raw_func.return_type),
            examples=examples,
            signature_coll=signature_coll,
            locale=self._locale,
            crosslink_note=_render_crosslink_note(crosslink_note) if crosslink_note is not None else None,
        )

    def render_multi_func_func(self, func_key: RefFunctionKey) -> RenderedMultiAudienceFunc:
        raw_multi_func = self._func_ref.get_func(func_key=func_key)
        func_file_path = self._path_renderer.get_func_path(func_key=func_key)

        rendered_aud_func_dict: dict[Audience, RenderedFunc] = {}
        for audience, raw_func in raw_multi_func.items():
            rendered_func = self.render_func(func_key=func_key, raw_func=raw_func)
            rendered_aud_func_dict[audience] = rendered_func

        one_rendered_func = next(iter(rendered_aud_func_dict.values()))

        rendered_multi_func = RenderedMultiAudienceFunc(
            aud_dict=rendered_aud_func_dict,
            name=raw_multi_func.name,
            category_name=one_rendered_func.category_name,
            category_description=one_rendered_func.category_description,
            category_description_short=one_rendered_func.category_description_short,
            category_keywords=one_rendered_func.category_keywords,
            file_path=func_file_path,
        )
        return rendered_multi_func
