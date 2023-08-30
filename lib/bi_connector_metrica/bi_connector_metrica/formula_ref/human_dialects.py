from bi_formula_ref.texts import StyledDialect
from bi_formula_ref.localization import get_gettext

from bi_connector_metrica.formula.constants import MetricaDialect


_ = get_gettext()


HUMAN_DIALECTS = {
    MetricaDialect.METRIKAAPI: StyledDialect(
        '`Yandex Metrica`',
        '`Yandex Metrica`',
        '`Yandex Metrica`',
    ),
}
