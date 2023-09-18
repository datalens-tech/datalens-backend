from dl_formula_ref.texts import StyledDialect

from dl_connector_postgresql.formula.constants import PostgreSQLDialect
from dl_connector_postgresql.formula_ref.i18n import Translatable

HUMAN_DIALECTS = {
    PostgreSQLDialect.POSTGRESQL: StyledDialect(
        "`PostgreSQL`",
        "`PostgreSQL`",
        "`PostgreSQL`",
    ),
    PostgreSQLDialect.POSTGRESQL_9_3: StyledDialect(
        "`PostgreSQL 9.3`",
        "`PostgreSQL`<br/>`9.3`",
        Translatable("`PostgreSQL` version `9.3`"),
    ),
    PostgreSQLDialect.POSTGRESQL_9_4: StyledDialect(
        "`PostgreSQL 9.4`",
        "`PostgreSQL`<br/>`9.4`",
        Translatable("`PostgreSQL` version `9.4`"),
    ),
}
