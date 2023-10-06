from dl_formula_ref.texts import StyledDialect

from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_connector_clickhouse.formula_ref.i18n import Translatable


HUMAN_DIALECTS = {
    ClickHouseDialect.CLICKHOUSE: StyledDialect(
        "`ClickHouse`",
        "`ClickHouse`",
        "`ClickHouse`",
    ),
    ClickHouseDialect.CLICKHOUSE_21_8: StyledDialect(
        "`ClickHouse 21.8`",
        "`ClickHouse`<br/>`21.8`",
        Translatable("`ClickHouse` version `21.8`"),
    ),
    ClickHouseDialect.CLICKHOUSE_22_10: StyledDialect(
        "`ClickHouse 22.10`",
        "`ClickHouse`<br/>`22.10`",
        Translatable("`ClickHouse` version `22.10`"),
    ),
}
