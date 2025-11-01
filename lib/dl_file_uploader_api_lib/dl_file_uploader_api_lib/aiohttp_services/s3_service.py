from __future__ import annotations

import logging
from typing import ClassVar

from aiobotocore.config import AioConfig

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

    def _init_client_config(self) -> None:
        super()._init_client_config()

        new_config = AioConfig(s3={"addressing_style": "auto"})

        assert isinstance(self._client_init_params["config"], AioConfig)

        self._client_init_params["config"] = self._client_init_params["config"].merge(new_config)
