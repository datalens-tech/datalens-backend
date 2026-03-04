from typing import AsyncGenerator

import aiobotocore.session
import pytest
import pytest_asyncio

import dl_dynconfig.utils.types as types_utils
import dl_testing


@pytest.fixture(name="s3_hostport")
def fixture_s3_hostport() -> dl_testing.HostPort:
    return dl_testing.get_test_container_hostport("s3-storage")


@pytest.fixture(name="s3_access_key")
def fixture_s3_access_key() -> str:
    return "accessKey1"


@pytest.fixture(name="s3_secret_key")
def fixture_s3_secret_key() -> str:
    return "verySecretKey1"


@pytest_asyncio.fixture(name="s3_client")
async def fixture_s3_client(
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
) -> AsyncGenerator[types_utils.AiobotocoreS3Client, None]:
    session = aiobotocore.session.get_session()
    async with session.create_client(
        service_name="s3",
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        endpoint_url=f"http://{s3_hostport.as_pair()}",
    ) as client:
        yield client


@pytest_asyncio.fixture(name="s3_bucket")
async def fixture_s3_bucket(s3_client: types_utils.AiobotocoreS3Client) -> str:
    name = "dl-dynconfig-test"
    try:
        await s3_client.create_bucket(Bucket=name)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except s3_client.exceptions.BucketAlreadyExists:
        pass

    resp = await s3_client.list_objects_v2(Bucket=name)
    for obj in resp.get("Contents", []):
        await s3_client.delete_object(Bucket=name, Key=obj["Key"])

    return name
