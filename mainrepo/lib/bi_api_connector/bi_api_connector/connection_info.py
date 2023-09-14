from typing import (
    ClassVar,
    Optional,
    final,
)

from bi_i18n.localizer_base import (
    Localizer,
    Translatable,
)


class ConnectionInfoProvider:
    title_translatable: ClassVar[Translatable]
    alias: ClassVar[Optional[str]] = None

    @classmethod
    @final
    def get_title(cls, localizer: Localizer) -> str:
        return localizer.translate(cls.title_translatable)
