from dl_formula_ref.functions.date import FUNCTION_DATETRUNC
from dl_formula_ref.functions.operator import FUNCTION_OP_COMPARISON
from dl_formula_ref.functions.type_conversion import (
    FUNCTION_DATE,
    DbCastExtension,
)
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin
from dl_formula_ref.registry.note import (
    Note,
    NoteLevel,
)

from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_connector_clickhouse.formula.definitions.functions_type import FuncDbCastClickHouseBase
from dl_connector_clickhouse.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_clickhouse.formula_ref.i18n import (
    CONFIGS,
    Translatable,
)


class ClickHouseFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*ClickHouseDialect.CLICKHOUSE.to_list(),))
    compeng_support_dialects = frozenset((*ClickHouseDialect.CLICKHOUSE.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
    db_cast_extension = DbCastExtension(
        type_whitelists=FuncDbCastClickHouseBase.WHITELISTS,
    )
    function_extensions = [
        FUNCTION_DATETRUNC.extend(
            dialect=ClickHouseDialect.CLICKHOUSE,
            notes=(
                Note(
                    Translatable(
                        "The function with three arguments is only available for the sources "
                        "{dialects:CLICKHOUSE_21_8} or higher."
                    ),
                ),
            ),
        ),
        FUNCTION_DATE.extend(
            dialect=ClickHouseDialect.CLICKHOUSE,
            notes=(
                Note(
                    Translatable(
                        "For {dialects:CLICKHOUSE} data sources, numeric {arg:0} values less than or "
                        "equal to `65535` are interpreted as the number of days (not seconds, like in "
                        "all other cases) since January 1st 1970. This is the result of the behavior "
                        "of available {dialects:CLICKHOUSE} functions.\n"
                        "\n"
                        "One way to surpass this is to use the following formula: "
                        "`DATE(DATETIME([value]))`. The result is more consistent, but is likely to "
                        "be much slower."
                    ),
                    level=NoteLevel.warning,
                ),
            ),
        ),
        FUNCTION_OP_COMPARISON.extend(
            dialect=ClickHouseDialect.CLICKHOUSE,
            notes=(
                Note(
                    Translatable(
                        "Due to implementation details of the {type:FLOAT} type in {dialects:CLICKHOUSE} sources "
                        "it is recommended to use the {ref:COMPARE} function instead of comparison operators for this type."
                    ),
                ),
            ),
        ),
    ]
