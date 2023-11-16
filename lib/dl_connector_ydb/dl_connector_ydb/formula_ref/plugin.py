from dl_formula_ref.functions.date import FUNCTION_NOW
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin
from dl_formula_ref.registry.note import Note

from dl_connector_ydb.formula.constants import YqlDialect
from dl_connector_ydb.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_ydb.formula_ref.i18n import (
    CONFIGS,
    Translatable,
)


class YQLFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*YqlDialect.YDB.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
    function_extensions = [
        FUNCTION_NOW.extend(
            dialect=YqlDialect.YDB,
            notes=(
                Note(Translatable("On {dialects:YQL}, the function always returns the UTC date and time.")),
            ),
        ),
    ]
