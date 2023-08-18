from __future__ import annotations

import asyncio
import logging

import aiobotocore.client
import aiobotocore.session
import botocore.exceptions
import botocore.client
import botocore.session


LOGGER = logging.getLogger(__name__)


def create_s3_client(s3_settings) -> aiobotocore.client.AioBaseClient:
    session = aiobotocore.session.get_session()
    return session.create_client(
        service_name='s3',
        aws_access_key_id=s3_settings.ACCESS_KEY_ID,
        aws_secret_access_key=s3_settings.SECRET_ACCESS_KEY,
        endpoint_url=s3_settings.ENDPOINT_URL,
    )


def create_sync_s3_client(s3_settings) -> botocore.client.BaseClient:
    session = botocore.session.get_session()
    return session.create_client(
        service_name='s3',
        aws_access_key_id=s3_settings.ACCESS_KEY_ID,
        aws_secret_access_key=s3_settings.SECRET_ACCESS_KEY,
        endpoint_url=s3_settings.ENDPOINT_URL,
    )


async def create_s3_bucket(
        s3_client: aiobotocore.client.AioBaseClient,
        bucket_name: str,
        max_attempts: int = 10,
) -> None:
    attempt = 1
    while True:
        try:
            resp = await s3_client.create_bucket(Bucket=bucket_name)
            LOGGER.info(resp)
            break
        except botocore.exceptions.ClientError as ex:
            if ex.response.get('Error', {}).get('Code') != 'BucketAlreadyOwnedByYou':
                raise
            break
        except botocore.exceptions.HTTPClientError:
            LOGGER.warning(
                f'HTTPClientError during creating S3 bucket. Attempt {attempt} from {max_attempts}. '
                'Retrying after 5 seconds...',
                exc_info=True,
            )
            if attempt > max_attempts:
                raise
            await asyncio.sleep(5)
        attempt += 1


async def get_lc_rules_number(s3_client: aiobotocore.client.AioBaseClient, bucket: str) -> int:
    try:
        lc_config = await s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
    except botocore.exceptions.ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            lc_config = {'Rules': []}
        else:
            raise
    return len(lc_config['Rules'])


async def s3_file_exists(s3_client: aiobotocore.client.AioBaseClient, bucket: str, key: str) -> bool:
    try:
        s3_resp = await s3_client.head_object(
            Bucket=bucket,
            Key=key,
        )
    except botocore.exceptions.ClientError as ex:
        if ex.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return False
        raise
    return s3_resp['ResponseMetadata']['HTTPStatusCode'] == 200


S3_TBL_FUNC_TEMPLATE = '''s3(
'{s3_endpoint}/{bucket}/{filename}',
'{key_id}',
'{secret_key}',
'{file_fmt}',
'{schema_line}')'''


def s3_tbl_func_maker(s3_settings):
    def table_function(
        for_: str,
        conn_dto,
        filename: str,
        file_fmt: str,
        schema_line: str,
    ) -> str:
        if for_ == 'db':  # secret are filled
            tbl_func = S3_TBL_FUNC_TEMPLATE.format(
                s3_endpoint=conn_dto.s3_endpoint,
                bucket=conn_dto.bucket,
                filename=filename,
                key_id=s3_settings.ACCESS_KEY_ID,
                secret_key=s3_settings.SECRET_ACCESS_KEY,
                file_fmt=file_fmt,
                schema_line=schema_line,
            )
        elif for_ == 'dba':  # secrets will be filled by the adapter
            tbl_func = S3_TBL_FUNC_TEMPLATE.format(
                s3_endpoint=conn_dto.s3_endpoint,
                bucket=conn_dto.bucket,
                filename=filename,
                key_id=f'key_id_{conn_dto.replace_secret}',
                secret_key=f'secret_key_{conn_dto.replace_secret}',
                file_fmt=file_fmt,
                schema_line=schema_line,
            )
        else:
            raise ValueError(f"Unknown mode '{for_}'")
        return tbl_func

    return table_function
