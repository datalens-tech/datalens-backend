from __future__ import annotations

import typing

import aiohttp
import botocore.exceptions


if typing.TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AsyncS3Client


async def upload_to_s3_by_presigned(presigned_url: dict[str, typing.Any], data: str) -> aiohttp.ClientResponse:
    upload_url = presigned_url["url"]
    upload_url_fields = presigned_url["fields"]

    async with aiohttp.ClientSession() as session:
        with aiohttp.MultipartWriter("form-data") as mpwriter:
            for key, value in upload_url_fields.items():
                mpwriter.append(value, {"Content-Disposition": f'form-data; name="{key}"'})

            mpwriter.append(data, {"Content-Disposition": 'form-data; name=file; filename="mydata"'})

            async with session.post(
                url=upload_url,
                data=mpwriter,
            ) as resp:
                resp.raise_for_status()
                return resp


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
