from dl_formula_ref.texts import StyledDialect

from dl_connector_bundle_chs3.file.formula.constants import FileS3Dialect
from dl_connector_bundle_chs3.file.formula_ref.i18n import Translatable


HUMAN_DIALECTS = {
    FileS3Dialect.FILE: StyledDialect(
        Translatable("`Files`"),
        Translatable("`Files`"),
        Translatable("`Files`"),
    ),
}
