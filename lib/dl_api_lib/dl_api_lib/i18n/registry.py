from typing import Iterable

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIG
from dl_api_lib.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS
from dl_core.i18n.localizer import CONFIGS as BI_CORE_CONFIGS
from dl_i18n.localizer_base import TranslationConfig

LOCALIZATION_CONFIGS: set[TranslationConfig] = set()


def register_translation_configs(config: Iterable[TranslationConfig]) -> None:
    LOCALIZATION_CONFIGS.update(config)


register_translation_configs(BI_API_LIB_CONFIGS)
register_translation_configs(BI_CORE_CONFIGS)
register_translation_configs(BI_API_CONNECTOR_CONFIG)
