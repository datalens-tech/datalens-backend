from __future__ import annotations

import logging
from typing import ClassVar

from dl_s3.s3_service import S3Service


LOGGER = logging.getLogger(__name__)


class InternalS3Service(S3Service):
    """
    The only scenario when we may need to use virtual host addressing is Presigned URLs, but in every
    other case we can use automatic addressing.
    Addressing style is a part of client initialization, so here we create a specialized S3Service that
    ignores addressing settings.

    This is a bit of a kludge to avoid adding an extra set of S3 settings to the app just to have an S3 client with
    different addressing style.
    """

    APP_KEY: ClassVar[str] = "S3_SERVICE_INTERNAL"

    @property
    def _s3_addressing_style(self) -> str:
        return "auto"
