from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin

from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_clickhouse.formula_ref.human_dialects import HUMAN_DIALECTS


class ClickHouseFormulaRefPlugin(FormulaRefPlugin):
    any_dialects = frozenset((
        *ClickHouseDialect.CLICKHOUSE.to_list(),
    ))
    human_dialects = HUMAN_DIALECTS
