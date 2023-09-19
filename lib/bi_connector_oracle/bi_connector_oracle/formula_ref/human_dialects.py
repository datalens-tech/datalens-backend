from dl_formula_ref.texts import StyledDialect

from bi_connector_oracle.formula.constants import OracleDialect
from bi_connector_oracle.formula_ref.i18n import Translatable


HUMAN_DIALECTS = {
    OracleDialect.ORACLE_12_0: StyledDialect(
        "`Oracle Database 12c (12.1)`",
        "`Oracle`<br/>`Database 12c`<br/>`(12.1)`",
        Translatable("`Oracle Database` version `12c (12.1)`"),
    ),
}
