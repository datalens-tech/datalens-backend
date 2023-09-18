from dl_formula_ref.texts import StyledDialect

from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_mysql.formula_ref.i18n import Translatable

HUMAN_DIALECTS = {
    MySQLDialect.MYSQL: StyledDialect(
        "`MySQL`",
        "`MySQL`",
        "`MySQL`",
    ),
    MySQLDialect.MYSQL_5_6: StyledDialect(
        "`MySQL 5.6`",
        "`MySQL`<br/>`5.6`",
        Translatable("`MySQL` version `5.6`"),
    ),
    MySQLDialect.MYSQL_5_7: StyledDialect(
        "`MySQL 5.7`",
        "`MySQL`<br/>`5.7`",
        Translatable("`MySQL` version `5.7`"),
    ),
    MySQLDialect.MYSQL_8_0_12: StyledDialect(
        "`MySQL 8.0.12`",
        "`MySQL`<br/>`8.0.12`",
        Translatable("`MySQL` version `8.0.12`"),
    ),
}
