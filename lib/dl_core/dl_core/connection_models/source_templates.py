from typing import NamedTuple

from dl_core.i18n.localizer import Translatable


class DataSourceTemplateDisabledText(NamedTuple):
    """Localized explanation of a ``DataSourceTemplate``'s availability.

    Holds untranslated ``Translatable`` messages; the helpers that build
    ``DataSourceTemplate`` translate them via the request localizer into the
    translated message stored on the template.
    """

    title: Translatable
    description: Translatable
