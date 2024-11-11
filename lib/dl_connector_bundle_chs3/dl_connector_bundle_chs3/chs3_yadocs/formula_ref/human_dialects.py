from dl_formula_ref.texts import StyledDialect

from dl_connector_bundle_chs3.chs3_yadocs.formula.constants import YaDocsFileS3Dialect
from dl_connector_bundle_chs3.chs3_yadocs.formula_ref.i18n import Translatable


HUMAN_DIALECTS = {
    YaDocsFileS3Dialect.YADOCS: StyledDialect(
        Translatable("`Yandex Documents`"),
        Translatable("`Yandex Documents`"),
        Translatable("`Yandex Documents`"),
    ),
}
