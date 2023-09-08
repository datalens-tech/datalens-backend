from __future__ import annotations

import inspect
import json
import os
from collections import defaultdict
from contextlib import redirect_stdout
from typing import Collection, Dict, Iterable, List, Optional, Sequence, Tuple

import attr
import jinja2
from tabulate import tabulate

import bi_formula.core.exc as exc
from bi_formula.core.dialect import StandardDialect as D, DialectCombo, get_all_basic_dialects, get_dialect_from_str
from bi_formula.definitions.base import MultiVariantTranslation
from bi_formula.definitions.registry import OPERATION_REGISTRY
from bi_formula_testing.database import Db, make_db_from_config, make_db_config

from bi_formula_ref.audience import Audience, DEFAULT_AUDIENCE
from bi_formula_ref.texts import (
    ANY_DIALECTS, DOC_ALL_TITLE, DOC_OVERVIEW_TEXT, DOC_AVAIL_TITLE, DialectStyle,
)
from bi_formula_ref.primitives import RawFunc, RawMultiAudienceFunc
from bi_formula_ref.registry.dialect_extractor import COMPENG_SUPPORT
from bi_formula_ref.rendered import RenderedFunc, RenderedMultiAudienceFunc
from bi_formula_ref.reference import (
    FuncReference, load_func_reference_from_registry
)
from bi_formula_ref.rendering import human_category, human_dialect, FuncRenderer
from bi_formula_ref.registry.registry import RefFunctionKey
from bi_formula_ref.registry.example_base import DataExampleRendererConfig
from bi_formula_ref.registry.example import DataExample, ExampleBase
from bi_formula_ref.i18n.registry import get_localizer
from bi_formula_ref.config import (
    RefDocGeneratorConfig, get_generator_config, ConfigVersion,
    FuncDocConfigVersion, FuncDocTemplateConfig,
    FuncPathTemplate, CatPathTemplate,
)
from bi_formula_ref.paths import PathRenderer

try:
    from bi_formula_ref.examples.data_prep import DataPreparer
except exc.ParserNotFoundError:
    class DataPreparer:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise exc.ParserNotFoundError()


def get_jinja_env(gen_config: RefDocGeneratorConfig) -> jinja2.Environment:
    import bi_formula_ref as top
    return jinja2.Environment(
        loader=jinja2.PackageLoader(top.__name__, gen_config.template_dir_rel),
    )


@attr.s
class ReferenceDocGenerator:
    _locale: str = attr.ib(kw_only=True)
    _config_version: ConfigVersion = attr.ib(kw_only=True)

    _gen_config: RefDocGeneratorConfig = attr.ib(init=False)
    _func_ref: FuncReference = attr.ib(init=False)
    _renderers_by_tmpl: Dict[Tuple[FuncPathTemplate, CatPathTemplate], FuncRenderer] = attr.ib(init=False, factory=dict)
    _jinja_env: jinja2.Environment = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._gen_config = get_generator_config(self._config_version)
        self._func_ref = load_func_reference_from_registry(
            scopes_by_audience=self._gen_config.function_scopes,
            supported_dialects=self._gen_config.supported_dialects
        )
        self._jinja_env = get_jinja_env(self._gen_config)

    def _get_renderer(self, doc_config: FuncDocTemplateConfig) -> FuncRenderer:
        path_renderer = PathRenderer(
            func_ref=self._func_ref,
            func_path_template=doc_config.func_file_path,
            cat_path_template=doc_config.cat_file_path,
        )
        key = (doc_config.func_file_path, doc_config.cat_file_path)
        if key not in self._renderers_by_tmpl:
            self._renderers_by_tmpl[key] = FuncRenderer(
                func_ref=self._func_ref,
                locale=self._locale,
                example_rend_config=DataExampleRendererConfig(
                    jinja_env=self._jinja_env,
                    template_filename=self._gen_config.doc_example_template,
                    example_data_filename=self._gen_config.example_data_file,
                ),
                block_conditions=self._gen_config.block_conditions,
                path_renderer=path_renderer,
            )
        return self._renderers_by_tmpl[key]

    def _get_single_rendered_func(
            self, func_key: RefFunctionKey,
            doc_config: FuncDocTemplateConfig,
    ) -> RenderedMultiAudienceFunc:
        renderer = self._get_renderer(doc_config=doc_config)
        return renderer.render_multi_func_func(func_key=func_key)

    def _render_funcs(
            self, raw_funcs: List[RawMultiAudienceFunc], doc_config: FuncDocTemplateConfig,
    ) -> List[RenderedMultiAudienceFunc]:
        func_doc_structs = []
        for raw_func in raw_funcs:
            func_key = RefFunctionKey.normalized(name=raw_func.name, category_name=raw_func.category.name)
            func_doc_structs.append(
                self._get_single_rendered_func(func_key=func_key, doc_config=doc_config)
            )
        return func_doc_structs

    def generate_doc_func(self, outdir: str):
        raw_funcs = self._func_ref.as_list()
        for doc_config in self._gen_config.func_doc_configs.values():
            template = self._jinja_env.get_template(doc_config.template_file)
            rend_funcs = self._render_funcs(raw_funcs=raw_funcs, doc_config=doc_config)
            for multi_func in rend_funcs:
                text = template.render(multi_func=multi_func, _=get_localizer(self._locale).translate)
                full_path = os.path.join(outdir, multi_func.file_path)
                dirname = os.path.dirname(full_path)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                with open(full_path, mode='w', encoding='utf-8') as outfile:
                    outfile.write(text)
                print(full_path)

    def _group_raw_funcs_by_category(
            self, raw_funcs: List[RawMultiAudienceFunc]
    ) -> Dict[str, List[RawMultiAudienceFunc]]:

        funcs_by_category = defaultdict(list)
        for func in raw_funcs:
            funcs_by_category[func.category.name].append(func)
        return funcs_by_category

    def generate_doc_toc(self, list_funcs: bool = False) -> None:
        trans = get_localizer(self._locale).translate
        doc_config = self._gen_config.func_doc_configs[FuncDocConfigVersion.overview_shortcut]
        raw_funcs = self._func_ref.as_list()
        renderer = self._get_renderer(doc_config=doc_config)
        funcs_by_category = self._group_raw_funcs_by_category(raw_funcs=raw_funcs)
        global_audiences = set(self._gen_config.function_scopes.keys())

        print('items:')
        print('  - name: {}\n    href: {}'.format(
            trans(DOC_ALL_TITLE), self._gen_config.doc_all_filename))

        localized_categories = {
            category: human_category(category=funcs_by_category[category][0].category.name, locale=self._locale)
            for category in funcs_by_category.keys()
        }
        locale_sorted_categories = sorted(
            funcs_by_category.keys(),
            key=lambda category: localized_categories[category]
        )
        for category in locale_sorted_categories:
            print(f'  - name: {localized_categories[category]}')
            print('    items:')
            print('      - name: {}'.format(trans(DOC_OVERVIEW_TEXT)))
            print('        href: {}'.format(renderer.path_renderer.get_cat_path(category_name=category)))
            if list_funcs:
                for multi_func in sorted(
                        funcs_by_category[category],
                        key=lambda func: func.get_title(locale=self._locale)
                ):
                    func_key = RefFunctionKey.normalized(
                        name=multi_func.name, category_name=multi_func.category.name,
                    )
                    audiences = set(multi_func.keys())
                    items: Iterable[tuple[Audience, RawFunc]]
                    if audiences == global_audiences:
                        # Consolidate entries if all audiences are present
                        # even if they have different content
                        items = [(DEFAULT_AUDIENCE, next(iter(multi_func.values())))]
                    else:
                        items = multi_func.items()

                    for audience, raw_func in items:
                        print('      - name: {}'.format(raw_func.get_short_title(locale=self._locale)))
                        print('        href: {}'.format(
                            renderer.path_renderer.get_func_path(func_key=func_key)))
                        if not audience.default:
                            print(f'        when: audience == "{audience.name}"')

        if self._gen_config.gen_availability_table:
            print('  - name: {}\n    href: {}'.format(trans(DOC_AVAIL_TITLE), self._gen_config.doc_avail_filename))

    def _generate_doc_list(
            self,
            context_path: str,
            title: str,
            description: str,
            rend_funcs: List[RenderedMultiAudienceFunc],
            in_category: bool,
            meta_title: str = '',
            meta_description: str = '',
            meta_keywords: Sequence[str] = (),
    ) -> None:
        trans = get_localizer(self._locale).translate
        template = self._jinja_env.get_template(self._gen_config.doc_list_template)
        doc_config = self._gen_config.func_doc_configs[FuncDocConfigVersion.overview_shortcut]
        renderer = self._get_renderer(doc_config=doc_config)
        relative_path_renderer = renderer.path_renderer.child(context_path)

        def make_link(func: RenderedFunc) -> str:
            func_key = RefFunctionKey.normalized(name=func.name, category_name=func.category_name)
            return relative_path_renderer.get_func_path(func_key=func_key)

        text = template.render(
            _=trans,
            functions=rend_funcs,
            make_link=make_link,
            title=trans(title),
            description=description,
            in_category=in_category,
            meta_title=meta_title,
            meta_description=meta_description,
            meta_keywords=meta_keywords,
        )
        print(text)

    def generate_doc_list_all(self, context_path: str):
        doc_config = self._gen_config.func_doc_configs[FuncDocConfigVersion.overview_shortcut]
        raw_funcs = self._func_ref.as_list()
        rend_funcs = self._render_funcs(raw_funcs=raw_funcs, doc_config=doc_config)
        self._generate_doc_list(
            context_path=context_path,
            rend_funcs=rend_funcs,
            title=DOC_ALL_TITLE,
            description='',
            in_category=False,
        )

    def generate_doc_list_category(self, category: str, context_path: str):
        doc_config = self._gen_config.func_doc_configs[FuncDocConfigVersion.overview_shortcut]
        raw_funcs = self._func_ref.filter(category=category)
        rend_funcs = self._render_funcs(raw_funcs=raw_funcs, doc_config=doc_config)
        rend_first_func = [
            # Some funcs have double categories, so find one whose primary category is the one we need
            # TODO: refactor this
            func for func in rend_funcs if func.category_name == category
        ][0]
        title = human_category(category=category, locale=self._locale)
        self._generate_doc_list(
            context_path=context_path,
            rend_funcs=rend_funcs,
            title=title,
            description=rend_first_func.category_description,
            in_category=True,
            meta_title=title,
            meta_description=rend_first_func.category_description_short,
            meta_keywords=rend_first_func.category_keywords,
        )

    def generate_doc_availability_list(self, context_path: str, audience: Audience) -> str:
        priority = {'==', 'NEG', '*', '/', '%', '^', '+', '-'}
        raw_funcs = self._func_ref.as_list()
        raw_funcs = sorted(
            raw_funcs,
            # move symbols to the top of the list
            key=lambda func: (
                ' ' + func.get_title(locale=self._locale)
                if func.name in priority
                else func.get_title(locale=self._locale)
            )
        )
        doc_config = self._gen_config.func_doc_configs[FuncDocConfigVersion.overview_shortcut]
        renderer = self._get_renderer(doc_config=doc_config)
        relative_path_renderer = renderer.path_renderer.child(context_path)

        scopes = self._gen_config.function_scopes.get(
            audience, self._gen_config.function_scopes.get(DEFAULT_AUDIENCE),
        )
        assert scopes is not None
        dialects = [
            d for d in get_all_basic_dialects()
            if d in self._gen_config.supported_dialects
        ]
        table_data = []
        for multi_func in raw_funcs:
            func = multi_func.get(audience, multi_func.get(DEFAULT_AUDIENCE))
            if func is None:
                continue
            func_key = RefFunctionKey.normalized(name=func.name, category_name=func.category.name)
            supported_dialects: Collection[DialectCombo]
            if func.category.name == 'window':  # FIXME CATEGORY_WINDOW.name
                supported_dialects = COMPENG_SUPPORT
            else:
                supported_dialects = func.dialects
                if len(supported_dialects) == 1 and next(iter(supported_dialects)) == D.ANY:
                    supported_dialects = ANY_DIALECTS
            func_data = [
                '[{}]({})'.format(
                    func.get_title(locale=self._locale),
                    relative_path_renderer.get_func_path(func_key=func_key),
                ),
                *[
                    ('X' if dialect in supported_dialects else '')
                    for dialect in dialects
                ]
            ]
            table_data.append(func_data)
        headers = ['Function', *[
            human_dialect(dialect, locale=self._locale, style=DialectStyle.multiline)
            for dialect in dialects
        ]]
        table = tabulate(table_data, headers=headers, tablefmt="pipe")
        # now hack column alignment to be centered for all columns except the first one
        first_match = table.find('-|')
        table = table[:first_match+2] + table[first_match+2:].replace('-|', ':|')
        return table

    def generate_doc_availability_list_for_all_audiences(self, context_path: str) -> None:
        table_by_audience: dict[Audience, str] = {}
        for audience in sorted(self._gen_config.function_scopes.keys()):
            avail_tbl = self.generate_doc_availability_list(context_path=context_path, audience=audience)
            table_by_audience[audience] = avail_tbl

        all_tables = set(table_by_audience.values())
        if len(all_tables) == 1:
            table_by_audience = {DEFAULT_AUDIENCE: next(iter(all_tables))}

        template = self._jinja_env.get_template(self._gen_config.doc_avail_template)
        text = template.render(
            table_by_audience=table_by_audience,
            _=get_localizer(self._locale).translate,
        )
        print(text)

    def generate_doc_full_dir(self, outdir: str):
        raw_funcs = self._func_ref.as_list()
        funcs_by_category = self._group_raw_funcs_by_category(raw_funcs=raw_funcs)

        func_dir = outdir
        self.generate_doc_func(outdir=func_dir)
        all_funcs_path = os.path.join(func_dir, self._gen_config.doc_all_filename)
        with open(all_funcs_path, 'w') as outfile:
            with redirect_stdout(outfile):
                self.generate_doc_list_all(context_path=os.path.dirname(all_funcs_path))
        for doc_config in self._gen_config.func_doc_configs.values():
            for category in sorted(funcs_by_category):
                category_path = os.path.join(func_dir, doc_config.cat_file_path.format(category_name=category))
                with open(category_path, 'w') as outfile:
                    with redirect_stdout(outfile):
                        self.generate_doc_list_category(context_path=os.path.dirname(category_path), category=category)
        if self._gen_config.gen_availability_table:
            availability_path = os.path.join(func_dir, self._gen_config.doc_avail_filename)
            with open(availability_path, 'w') as outfile:
                with redirect_stdout(outfile):
                    self.generate_doc_availability_list_for_all_audiences(
                        context_path=os.path.dirname(availability_path),
                    )
        with open(os.path.join(outdir, self._gen_config.doc_toc_filename), 'w') as outfile:
            with redirect_stdout(outfile):
                self.generate_doc_toc(list_funcs=True)

    @staticmethod
    def _get_func_base_class(name: str) -> Optional[type]:
        """Find the first (base) class for the given function"""
        name = name.lower()
        for i, definition in OPERATION_REGISTRY.items():
            func_name = definition.name
            assert func_name is not None
            if func_name.lower() == name:
                def_cls = type(definition)
                break
        else:
            return None

        mro = inspect.getmro(def_cls)  # `def_cls` itself plus all its bases
        for earliest_super_cls in reversed(mro):
            if (
                    issubclass(earliest_super_cls, MultiVariantTranslation)
                    and (earliest_super_cls.name or '').lower() == name
            ):
                return earliest_super_cls

        return None

    def _get_func_source_info(self, name: str) -> Tuple[str, int]:
        """Get file name and line number for given function"""
        func_cls = self._get_func_base_class(name)
        assert func_cls is not None
        filename = inspect.getsourcefile(func_cls)
        assert filename is not None
        lineno = inspect.getsourcelines(func_cls)[1]
        return filename, lineno

    def _load_db_config(self) -> Dict[DialectCombo, Db]:
        with open(self._gen_config.db_config_file) as config_file:
            raw_config: dict = json.load(config_file)
        return {
            get_dialect_from_str(d_name): make_db_from_config(
                make_db_config(dialect=get_dialect_from_str(d_name), url=d_url)
            )
            for d_name, d_url in raw_config.items()
        }

    def generate_example_data(self) -> None:
        raw_funcs = self._func_ref.as_list()
        db_by_dialect = self._load_db_config()
        preparer = DataPreparer(
            storage_filename=self._gen_config.example_data_file,
            db_by_dialect=db_by_dialect,
            default_example_dialect=self._gen_config.default_example_dialect,
        )
        examples: list[ExampleBase]
        for multi_func in raw_funcs:
            examples = []
            for raw_func in multi_func.values():
                for example in raw_func.examples:
                    if example not in examples:
                        examples.append(example)
            for example in examples:
                if isinstance(example, DataExample):
                    preparer.generate_example_data(
                        func_name=multi_func.internal_name,
                        example=example.example_config,
                    )

        preparer.save()
