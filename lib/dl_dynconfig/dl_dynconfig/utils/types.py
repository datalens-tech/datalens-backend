import typing

import aiobotocore.client


if typing.TYPE_CHECKING:
    import types_aiobotocore_s3

    AiobotocoreS3Client = types_aiobotocore_s3.S3Client
else:
    AiobotocoreS3Client = aiobotocore.client.AioBaseClient
