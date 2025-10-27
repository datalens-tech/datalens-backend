import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
)

from aiobotocore.client import AioBaseClient
from aiobotocore.config import AioConfig
from aiobotocore.session import (
    AioSession,
    get_session,
)
import attr


if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AsyncS3Client

from dl_s3.s3_service import S3Service


LOGGER = logging.getLogger(__name__)


class FileUploaderAPIS3Service(S3Service):
    """
    The only scenario when we need to use virtual host addressing is Presigned URLs,
    but addressing style is a part of client initialization, so here we create a specialized S3Service that
    creates a separate client for Presigned URLs and applies addressing settings only to it and not the basic client.

    All of this is done to avoid adding a second S3Service to the app along with a second set of settings.
    """

    APP_KEY: ClassVar[str] = "S3_SERVICE_FILE_UPLOADER_API"

    _client_for_presigned: AioBaseClient = attr.ib(init=False, repr=False, hash=False, cmp=False)
    _client_init_params_for_presigned: dict[str, Any] = attr.ib(init=False, repr=False, hash=False, cmp=False)

    async def _initialize_basic_client(self, session: AioSession) -> None:
        self._client_init_params = dict(
            service_name="s3",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            endpoint_url=self._endpoint_url,
            config=AioConfig(
                signature_version="s3v4",  # v4 signature is required to generate presigned URLs with restriction policies
            ),
        )
        client = session.create_client(**self._client_init_params)
        self._client = await client.__aenter__()

    async def _initialize_client_for_presigned(self, session: AioSession) -> None:
        self._client_init_params_for_presigned = dict(
            service_name="s3",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            endpoint_url=self._endpoint_url,
            config=AioConfig(
                signature_version="s3v4",  # v4 signature is required to generate presigned URLs with restriction policies
                s3={"addressing_style": "virtual" if self._use_virtual_host_addressing else "auto"},
            ),
        )
        client_for_presigned = session.create_client(**self._client_init_params_for_presigned)
        self._client_for_presigned = await client_for_presigned.__aenter__()

    async def initialize(self) -> None:
        LOGGER.info("Initializing S3 service")
        session = get_session()
        await self._initialize_basic_client(session=session)
        await self._initialize_client_for_presigned(session=session)

    async def get_client_for_presigned(self) -> AsyncS3Client:
        return self._client_for_presigned
