import base64
from importlib.abc import Traversable
from importlib.resources import as_file
import logging
from typing import (
    ClassVar,
    Optional,
    final,
)

import attr

from dl_i18n.localizer_base import (
    Localizer,
    Translatable,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class ConnectionInfoProvider:
    title_translatable: ClassVar[Translatable]
    alias: ClassVar[Optional[str]] = None
    icon_data_standard: Optional[bytes] = attr.ib(default=None)
    icon_data_nav: Optional[bytes] = attr.ib(default=None)
    icon_data_standard_filepath: Optional[Traversable] = attr.ib(default=None)
    icon_data_nav_filepath: Optional[Traversable] = attr.ib(default=None)

    @classmethod
    @final
    def get_title(cls, localizer: Localizer) -> str:
        return localizer.translate(cls.title_translatable)

    def __attrs_post_init__(self) -> None:
        self.icon_data_standard = self.get_icon_file(self.icon_data_standard_filepath)
        self.icon_data_nav = self.get_icon_file(self.icon_data_nav_filepath)

    def get_icon_file(self, filepath: Traversable | None) -> bytes | None:
        if not filepath:
            return None
        try:
            with as_file(filepath) as file:
                with open(file, "rb") as icon_file:
                    return base64.b64encode(icon_file.read())
        except Exception:
            LOGGER.info(f"Connector icon reading by path {str(filepath)} failed")
            return None
