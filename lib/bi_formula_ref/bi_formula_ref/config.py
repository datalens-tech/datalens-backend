from __future__ import annotations

import os
from enum import Enum
from typing import Any, Mapping, NamedTuple

import attr
from dynamic_enum import DynamicEnum, AutoEnumValue

from bi_formula.definitions.scope import Scope

from bi_formula_ref.audience import Audience, DEFAULT_AUDIENCE
from bi_formula_ref.paths import FuncPathTemplate, CatPathTemplate


class FuncDocTemplateConfig(NamedTuple):
    template_file: str
    func_file_path: FuncPathTemplate  # where the doc file should be generated
    cat_file_path: CatPathTemplate


class FuncDocConfigVersion(Enum):
    overview_shortcut = 'overview_shortcut'
    overview = 'overview'
    summary = 'summary'


class ConfigVersion(DynamicEnum):
    default = AutoEnumValue()
    yacloud = AutoEnumValue()
    nebius = AutoEnumValue()
    doublecloud = AutoEnumValue()


@attr.s
class RefDocGeneratorConfig:
    template_dir_rel: str = attr.ib(kw_only=True, default=os.path.join('templates'))  # rel to bi_formula_ref pkg
    func_doc_configs: dict[FuncDocConfigVersion, FuncDocTemplateConfig] = attr.ib(kw_only=True)

    # Templates
    doc_list_template: str = attr.ib(kw_only=True, default='doc_list.md.jinja')
    doc_avail_template: str = attr.ib(kw_only=True, default='doc_availability.md.jinja')
    doc_example_template: str = attr.ib(kw_only=True, default='doc_example.md.jinja')

    # Destination paths
    doc_toc_filename: str = attr.ib(kw_only=True)
    doc_all_filename: str = attr.ib(kw_only=True)
    doc_avail_filename: str = attr.ib(kw_only=True)

    db_config_file: str = attr.ib(kw_only=True, default=os.path.join(os.path.dirname(__file__), 'db_config.json'))  # FIXME: move somewhere else
    example_data_file: str = attr.ib(kw_only=True, default=os.path.join(os.path.dirname(__file__), 'example_data.json'))
    gen_availability_table: bool = attr.ib(kw_only=True, default=True)

    function_scopes: dict[Audience, int] = attr.ib(kw_only=True, default={DEFAULT_AUDIENCE: Scope.DOCUMENTED})
    block_conditions: Mapping[str, bool] = attr.ib(kw_only=True, factory=dict)

    def clone(self, **kwargs: Any) -> RefDocGeneratorConfig:
        return attr.evolve(self, **kwargs)


DOC_GEN_CONFIG_DEFAULT = RefDocGeneratorConfig(
    func_doc_configs={
        FuncDocConfigVersion.overview_shortcut: FuncDocTemplateConfig(
            template_file='doc_func_long.md.jinja',
            func_file_path=FuncPathTemplate('function-ref/{func_name}.md'),
            cat_file_path=CatPathTemplate('function-ref/{category_name}-functions.md'),
        ),
    },
    doc_toc_filename='toc.yaml',
    doc_all_filename='function-ref/all.md',
    doc_avail_filename='function-ref/availability.md',
)


_CONFIGS_BY_VERSION: dict[ConfigVersion, RefDocGeneratorConfig] = {}


def register_config_version(version: ConfigVersion, config: RefDocGeneratorConfig) -> None:
    try:
        assert _CONFIGS_BY_VERSION[version] is config
    except KeyError:
        _CONFIGS_BY_VERSION[version] = config


def get_generator_config(version: ConfigVersion) -> RefDocGeneratorConfig:
    return _CONFIGS_BY_VERSION[version]


register_config_version(ConfigVersion.default, DOC_GEN_CONFIG_DEFAULT)
