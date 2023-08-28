from bi_formula.definitions.scope import Scope

from bi_formula_ref.paths import FuncPathTemplate, CatPathTemplate
from bi_formula_ref.audience import DEFAULT_AUDIENCE
from bi_formula_ref.config import (
    RefDocGeneratorConfig, FuncDocConfigVersion, FuncDocTemplateConfig
)


DOC_GEN_CONFIG_DC = RefDocGeneratorConfig(
    func_doc_configs={
        FuncDocConfigVersion.overview_shortcut: FuncDocTemplateConfig(
            template_file='doc_func_long.md.jinja',
            func_file_path=FuncPathTemplate('{func_name}.md'),
            cat_file_path=CatPathTemplate('{category_name}-functions.md'),
        ),
    },
    doc_toc_filename='toc.yaml',
    doc_all_filename='all.md',
    doc_avail_filename='availability.md',
    gen_availability_table=False,
    function_scopes={
        DEFAULT_AUDIENCE: Scope.DOCUMENTED | Scope.DOUBLECLOUD,
    },
    block_conditions={'doublecloud': True},
)
