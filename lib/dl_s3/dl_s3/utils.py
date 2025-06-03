from __future__ import annotations

import typing

import aiohttp
import botocore.exceptions


if typing.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as SyncS3Client
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


class S3Object(typing.NamedTuple):
    bucket: str
    key: str


class S3Exception(Exception):
    pass


class S3NoSuchKey(S3Exception):
    pass


class S3AccessDenied(S3Exception):
    pass


def write_json_to_s3(
    s3_sync_cli: SyncS3Client,
    file: S3Object,
    json_data: str,
) -> None:
    try:
        s3_sync_cli.put_object(
            Body=json_data.encode("utf-8"),
            Bucket=file.bucket,
            Key=file.key,
        )
    except s3_sync_cli.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            raise S3AccessDenied() from e
        raise S3Exception() from e


def read_json_from_s3(
    s3_sync_cli: SyncS3Client,
    file: S3Object,
) -> str:
    try:
        object = s3_sync_cli.get_object(
            Bucket=file.bucket,
            Key=file.key,
        )
    except s3_sync_cli.exceptions.NoSuchKey as e:
        raise S3NoSuchKey() from e
    except s3_sync_cli.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            raise S3AccessDenied() from e
        raise S3Exception() from e

    return object["Body"].read().decode("utf-8")


def delete_json_from_s3(
    s3_sync_cli: SyncS3Client,
    file: S3Object,
) -> None:
    try:
        s3_sync_cli.delete_object(
            Bucket=file.bucket,
            Key=file.key,
        )
    except s3_sync_cli.exceptions.NoSuchKey as e:
        raise S3NoSuchKey() from e
    except s3_sync_cli.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            raise S3AccessDenied() from e
        raise S3Exception() from e


def read_one_of_objects(
    s3_sync_cli: SyncS3Client,
    files: list[S3Object],
) -> typing.Any:
    """Read one of files if possible"""

    for idx, file in enumerate(files):
        try:
            return read_json_from_s3(
                s3_sync_cli=s3_sync_cli,
                file=file,
            )
        except s3_sync_cli.exceptions.NoSuchKey:
            if idx == len(files) - 1:
                raise
