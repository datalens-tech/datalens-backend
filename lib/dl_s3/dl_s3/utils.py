from __future__ import annotations

from typing import TYPE_CHECKING

import botocore.exceptions

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AsyncS3Client


async def s3_file_exists(s3_client: AsyncS3Client, bucket: str, key: str) -> bool:
    try:
        s3_resp = await s3_client.head_object(
            Bucket=bucket,
            Key=key,
        )
    except botocore.exceptions.ClientError as ex:
        if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise
    return s3_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
