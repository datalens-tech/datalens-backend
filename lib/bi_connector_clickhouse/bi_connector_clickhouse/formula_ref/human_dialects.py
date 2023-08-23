from bi_formula_ref.texts import StyledDialect
from bi_formula_ref.localization import get_gettext

from bi_connector_clickhouse.formula.constants import ClickHouseDialect


_ = get_gettext()


HUMAN_DIALECTS = {
    ClickHouseDialect.CLICKHOUSE: StyledDialect(
        '`ClickHouse`',
        '`ClickHouse`',
        '`ClickHouse`',
    ),
    ClickHouseDialect.CLICKHOUSE_21_8: StyledDialect(
        '`ClickHouse 21.8`',
        '`ClickHouse`<br/>`21.8`',
        _('`ClickHouse` version `21.8`'),
    ),
    ClickHouseDialect.CLICKHOUSE_22_10: StyledDialect(
        '`ClickHouse 22.10`',
        '`ClickHouse`<br/>`22.10`',
        _('`ClickHouse` version `22.10`'),
    ),
}
