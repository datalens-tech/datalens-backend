from dl_formula_ref.functions.operator import FUNCTION_OP_PLUS
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin
from dl_formula_ref.registry.note import (
    Note,
    NoteLevel,
)

from dl_connector_metrica.formula.constants import MetricaDialect
from dl_connector_metrica.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_metrica.formula_ref.i18n import (
    CONFIGS,
    Translatable,
)


class MetricaFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*MetricaDialect.METRIKAAPI.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
    function_extensions = [
        FUNCTION_OP_PLUS.extend(
            dialect=MetricaDialect.METRIKAAPI,
            notes=[  # type: ignore  # 2024-01-24 # TODO: Argument "notes" to "extend" of "FunctionDocRegistryItem" has incompatible type "list[Note]"; expected "tuple[Note, ...]"  [arg-type]
                Note(
                    level=NoteLevel.warning,
                    text=Translatable("{dialects: METRIKAAPI} does not support string concatenation."),
                )
            ],
        ),
    ]
