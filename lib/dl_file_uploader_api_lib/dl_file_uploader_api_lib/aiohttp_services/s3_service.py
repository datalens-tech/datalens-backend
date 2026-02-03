from __future__ import annotations

import logging
from typing import ClassVar

from dl_s3.s3_service import S3Service


LOGGER = logging.getLogger(__name__)


class InternalS3Service(S3Service):
    APP_KEY: ClassVar[str] = "S3_SERVICE_INTERNAL"
