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
from dl_formula_ref.paths import PathRenderer
from dl_formula_ref.primitives import RawFunc
from dl_formula_ref.reference import FuncReference
from dl_formula_ref.registry.aliased_res import (
    AliasedResourceRegistryBase,
    SimpleAliasedResourceRegistry,
)
from dl_formula_ref.registry.note import ParameterizedNote
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
    RenderedNote,
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
    """Renders of a single RawFunction"""

    _func_key: RefFunctionKey = attr.ib(kw_only=True)
    _raw_func: RawFunc = attr.ib(kw_only=True)
    _resource_overrides: AliasedResourceRegistryBase = attr.ib(kw_only=True, factory=SimpleAliasedResourceRegistry)
    _locale: str = attr.ib(kw_only=True)
    _path_renderer: PathRenderer = attr.ib(kw_only=True)
    _context_path: str = attr.ib(kw_only=True)
    _rt_renderer: MdRichTextRenderer = attr.ib(kw_only=True)
    _rt_env: RichTextRenderEnvironment = attr.ib(kw_only=True)
    _example_rend_config: DataExampleRendererConfig = attr.attrib(kw_only=True)

    def render_text(self, param_text: ParameterizedText) -> str:
        if not param_text:
            return ""
        translation_callable = get_localizer(self._locale).translate
        relative_path_renderer = self._path_renderer.child(self._context_path)
        resources = self._resource_overrides + self._raw_func.resources
        expander = MacroExpander(
            resources=param_text.resources + resources,
            args=self._raw_func.args,
            translation_callable=translation_callable,
            func_link_provider=relative_path_renderer.get_func_link,
            cat_link_provider=relative_path_renderer.get_cat_link,
        )
        rich_text = expander.expand_text(param_text.text)
        result = self._rt_renderer.render(rich_text, env=self._rt_env)
        return result

    def _as_param_text(self, text: str, params: Optional[dict[str, str]] = None) -> ParameterizedText:
        return ParameterizedText.from_str(text=text, params=params or {})

    def render_note(self, note: ParameterizedNote) -> RenderedNote:
        text = self.render_text(note.param_text)
        return RenderedNote(
            text=text,
            formatting=note.formatting,
            level=note.level,
            type=note.type,
        )

    def make_keywords(self) -> list[str]:
        gtext = get_localizer(self._locale).translate
        keyword_list_str = self._raw_func.category.keywords
        if not keyword_list_str:
            return []
        keyword_list_str = gtext(keyword_list_str)
        return [kw.strip() for kw in keyword_list_str.split(",")]

    def make_signature_coll(self) -> FunctionSignatureCollection:
        signature_coll = FunctionSignatureCollection(
            signatures=[
                FunctionSignature(
                    title=sign.title,
                    body=sign.body,
                    description=[
                        self.render_text(self._as_param_text(text=desc_part)) for desc_part in sign.description
                    ],
                )
                for sign in self._raw_func.signature_coll.signatures
            ],
            placement_mode=self._raw_func.signature_coll.placement_mode,
        )
        return signature_coll

    def render_examples(self) -> list[str]:
        # Place examples under cut only if there are more than one
        ex_under_cut = len(self._raw_func.examples) > 1
        examples = [
            ex.render(
                func_name=self._raw_func.internal_name,
                config=self._example_rend_config,
                locale=self._locale,
                under_cut=ex_under_cut,
            )
            for ex in self._raw_func.examples
        ]
        return examples

    def render_args(self) -> list[LocalizedFuncArg]:
        args = [
            LocalizedFuncArg(
                arg=arg, locale=self._locale, human_type=human_data_type(types=arg.types, locale=self._locale)
            )
            for arg in self._raw_func.args
        ]
        return args

    def get_counterpart_keys(self, func_ref: FuncReference) -> list[RefFunctionKey]:
        return [
            RefFunctionKey.normalized(name=key.name, category_name=key.category.name)
            for key in func_ref.filter(name=self._func_key.name.upper())
            if key.category.name != self._func_key.category_name
        ]

    def get_counterpart_crosslink_note_and_refs(self, func_ref: FuncReference) -> Optional[ParameterizedNote]:
        counterpart_keys = self.get_counterpart_keys(func_ref=func_ref)
        if not counterpart_keys:
            return None

        counterpart_ref = "{{ref:{category_name}/{func_name}:{human_category}}}"
        counterpart_refs_list = [
            counterpart_ref.format(
                category_name=key.category_name,
                func_name=self._func_key.name.upper(),
                human_category=HUMAN_CATEGORIES[key.category_name],
            )
            for key in counterpart_keys
        ]
        root_text = "{text:also_in_other_categories}" + ",".join(counterpart_refs_list) + "."

        note = ParameterizedNote(
            param_text=self._as_param_text(
                text=root_text,
                params={
                    "func_name": self._func_key.name.upper(),
                    "also_in_other_categories": ALSO_IN_OTHER_CATEGORIES,
                },
            ),
        )

        return note

    def render(self, func_ref: FuncReference) -> RenderedFunc:
        func_file_path = self._path_renderer.get_func_path(func_key=self._func_key)
        crosslink_note = self.get_counterpart_crosslink_note_and_refs(func_ref=func_ref)
        examples = self.render_examples()
        signature_coll = self.make_signature_coll()
        args = self.render_args()

        return RenderedFunc(
            file_path=func_file_path,
            name=self._raw_func.name,
            internal_name=self._raw_func.internal_name,
            title=self._raw_func.get_title(locale=self._locale),
            short_title=self._raw_func.get_short_title(locale=self._locale),
            category_name=self._raw_func.category.name or "",
            human_category=human_category(category=self._raw_func.category.name, locale=self._locale),
            category_description=self.render_text(self._as_param_text(text=self._raw_func.category.description)),
            category_keywords=self.make_keywords(),
            args=args,
            description=self.render_text(self._as_param_text(text=self._raw_func.description)),
            dialects=self._raw_func.dialects,
            human_dialects=human_dialects(dialects=self._raw_func.dialects, locale=self._locale),
            top_notes=[self.render_note(w) for w in self._raw_func.top_notes],
            bottom_notes=[self.render_note(n) for n in self._raw_func.bottom_notes[:]],
            return_type=self.render_text(self._raw_func.return_type),
            examples=examples,
            signature_coll=signature_coll,
            locale=self._locale,
            crosslink_note=self.render_note(crosslink_note) if crosslink_note is not None else None,
        )


@attr.s
class FuncRefRenderer:
    """Renders the whole FuncReference"""

    _func_ref: FuncReference = attr.ib(kw_only=True)
    _locale: str = attr.ib(kw_only=True)
    _example_rend_config: DataExampleRendererConfig = attr.attrib(kw_only=True)
    _path_renderer: PathRenderer = attr.ib(kw_only=True)
    _rt_renderer: MdRichTextRenderer = attr.ib(init=False, factory=MdRichTextRenderer)
    _block_conditions: Mapping[str, bool] = attr.ib(kw_only=True, factory=dict)
    _rt_env: RichTextRenderEnvironment = attr.ib(kw_only=True)
    _resource_overrides: AliasedResourceRegistryBase = attr.ib(kw_only=True, factory=SimpleAliasedResourceRegistry)

    @_rt_env.default
    def _rt_env_default(self) -> RichTextRenderEnvironment:
        return RichTextRenderEnvironment(block_conditions=self._block_conditions)

    @property
    def path_renderer(self) -> PathRenderer:
        return self._path_renderer

    def render_func(self, func_key: RefFunctionKey, raw_func: RawFunc) -> RenderedFunc:
        func_file_path = self._path_renderer.get_func_path(func_key=func_key)
        context_path = os.path.dirname(func_file_path)

        func_renderer = FuncRenderer(
            func_key=func_key,
            raw_func=raw_func,
            rt_renderer=self._rt_renderer,
            rt_env=self._rt_env,
            context_path=context_path,
            locale=self._locale,
            path_renderer=self._path_renderer,
            resource_overrides=self._resource_overrides,
            example_rend_config=self._example_rend_config,
        )
        return func_renderer.render(func_ref=self._func_ref)

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
