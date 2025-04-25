from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    List,
    NamedTuple,
)


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as SyncS3Client


class S3Object(NamedTuple):
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
    files: List[S3Object],
) -> Any:
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
