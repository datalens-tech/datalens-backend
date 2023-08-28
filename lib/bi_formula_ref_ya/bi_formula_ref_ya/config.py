from bi_formula.definitions.scope import Scope

from bi_formula_ref.paths import FuncPathTemplate, CatPathTemplate
from bi_formula_ref.audience import Audience
from bi_formula_ref.config import (
    RefDocGeneratorConfig, FuncDocConfigVersion, FuncDocTemplateConfig
)


DOC_GEN_CONFIG_YC = RefDocGeneratorConfig(
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
    function_scopes={
        Audience(name='internal'): Scope.DOCUMENTED | Scope.INTERNAL,
        Audience(name='external'): Scope.DOCUMENTED | Scope.YACLOUD,
    },
    block_conditions={'ycloud': True},
)
