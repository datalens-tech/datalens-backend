from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_connector_clickhouse.formula.definitions.functions_type import FuncDbCastClickHouseBase
from dl_connector_clickhouse.formula_ref.human_dialects import HUMAN_DIALECTS
from dl_connector_clickhouse.formula_ref.i18n import CONFIGS
from dl_formula_ref.functions.type_conversion import DbCastExtension
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin


class ClickHouseFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((*ClickHouseDialect.CLICKHOUSE.to_list(),))
    compeng_support_dialects = frozenset((*ClickHouseDialect.CLICKHOUSE.to_list(),))
    human_dialects = HUMAN_DIALECTS
    translation_configs = frozenset(CONFIGS)
    db_cast_extension = DbCastExtension(
        type_whitelists=FuncDbCastClickHouseBase.WHITELISTS,
    )
