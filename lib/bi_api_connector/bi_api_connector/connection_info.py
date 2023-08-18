from typing import Optional, ClassVar, final

from bi_core.i18n.localizer_base import Translatable, Localizer


class ConnectionInfoProvider:
    title_translatable: ClassVar[Translatable]
    alias: ClassVar[Optional[str]] = None

    @classmethod
    @final
    def get_title(cls, localizer: Localizer) -> str:
        return localizer.translate(cls.title_translatable)
