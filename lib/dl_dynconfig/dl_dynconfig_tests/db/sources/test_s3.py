import datetime

import pytest

import dl_dynconfig
import dl_dynconfig.utils.types as types_utils
import dl_testing


@pytest.mark.asyncio
async def test_s3_source_store_and_fetch(
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
    s3_bucket: str,
) -> None:
    s3_key = "test_s3_source_store_and_fetch"
    settings = dl_dynconfig.BaseSourceSettings.factory(
        {
            "TYPE": "s3",
            "ENDPOINT_URL": f"http://{s3_hostport.as_pair()}",
            "ACCESS_KEY_ID": s3_access_key,
            "SECRET_ACCESS_KEY": s3_secret_key,
            "BUCKET": s3_bucket,
            "KEY": s3_key,
        }
    )
    assert isinstance(settings, dl_dynconfig.S3SourceSettings)
    source = dl_dynconfig.S3Source.from_settings(settings)

    data = {"key": "value", "nested": {"a": 1}}
    await source.store(data)
    result = await source.fetch()
    assert result == data


@pytest.mark.asyncio
async def test_s3_source_fetch_yaml(
    s3_client: types_utils.AiobotocoreS3Client,
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
    s3_bucket: str,
) -> None:
    s3_key = "test_s3_source_fetch_yaml"
    yaml_content = b"key: value\nlist:\n  - a\n  - b"
    await s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=yaml_content)

    source = dl_dynconfig.S3Source.from_settings(
        dl_dynconfig.S3SourceSettings(
            ENDPOINT_URL=f"http://{s3_hostport.as_pair()}",
            ACCESS_KEY_ID=s3_access_key,
            SECRET_ACCESS_KEY=s3_secret_key,
            BUCKET=s3_bucket,
            KEY=s3_key,
        ),
    )

    result = await source.fetch()
    assert result == {"key": "value", "list": ["a", "b"]}


@pytest.mark.asyncio
async def test_s3_source_check_readiness(
    s3_client: types_utils.AiobotocoreS3Client,
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
    s3_bucket: str,
) -> None:
    s3_key = "test_s3_source_check_readiness"
    source = dl_dynconfig.S3Source.from_settings(
        settings=dl_dynconfig.S3SourceSettings(
            ENDPOINT_URL=f"http://{s3_hostport.as_pair()}",
            ACCESS_KEY_ID=s3_access_key,
            SECRET_ACCESS_KEY=s3_secret_key,
            BUCKET=s3_bucket,
            KEY=s3_key,
        ),
    )
    assert not await source.check_readiness()

    await s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=b"")
    assert await source.check_readiness()

    await s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    assert not await source.check_readiness()


@pytest.mark.asyncio
async def test_cached_s3_source_caching(
    s3_client: types_utils.AiobotocoreS3Client,
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
    s3_bucket: str,
) -> None:
    s3_key = "test_cached_s3_source_caching"
    settings = dl_dynconfig.BaseSourceSettings.factory(
        {
            "TYPE": "cached_s3",
            "ENDPOINT_URL": f"http://{s3_hostport.as_pair()}",
            "ACCESS_KEY_ID": s3_access_key,
            "SECRET_ACCESS_KEY": s3_secret_key,
            "BUCKET": s3_bucket,
            "KEY": s3_key,
            "TTL": datetime.timedelta(minutes=5),
        }
    )
    assert isinstance(settings, dl_dynconfig.CachedS3SourceSettings)
    source = dl_dynconfig.CachedS3Source.from_settings(settings=settings)

    await source.store({"version": 1})
    result1 = await source.fetch()
    assert result1 == {"version": 1}

    # Update directly in S3, bypassing cache
    await s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=b"version: 2")

    # Should still return cached version
    result2 = await source.fetch()
    assert result2 == {"version": 1}


@pytest.mark.asyncio
async def test_cached_s3_source_cache_expired(
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
    s3_bucket: str,
) -> None:
    s3_key = "test_cached_s3_source_cache_expired"
    settings = dl_dynconfig.BaseSourceSettings.factory(
        {
            "TYPE": "cached_s3",
            "ENDPOINT_URL": f"http://{s3_hostport.as_pair()}",
            "ACCESS_KEY_ID": s3_access_key,
            "SECRET_ACCESS_KEY": s3_secret_key,
            "BUCKET": s3_bucket,
            "KEY": s3_key,
            "TTL": datetime.timedelta(seconds=0),
        }
    )
    assert isinstance(settings, dl_dynconfig.CachedS3SourceSettings)
    source = dl_dynconfig.CachedS3Source.from_settings(settings)

    await source.store({"version": 1})
    result1 = await source.fetch()
    assert result1 == {"version": 1}

    await source.store({"version": 2})
    result2 = await source.fetch()
    assert result2 == {"version": 2}


@pytest.mark.asyncio
async def test_cached_s3_source_store_updates_cache(
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
    s3_bucket: str,
) -> None:
    s3_key = "test_cached_s3_source_store_updates_cache"
    settings = dl_dynconfig.BaseSourceSettings.factory(
        {
            "TYPE": "cached_s3",
            "ENDPOINT_URL": f"http://{s3_hostport.as_pair()}",
            "ACCESS_KEY_ID": s3_access_key,
            "SECRET_ACCESS_KEY": s3_secret_key,
            "BUCKET": s3_bucket,
            "KEY": s3_key,
            "TTL": datetime.timedelta(minutes=5),
        }
    )
    assert isinstance(settings, dl_dynconfig.CachedS3SourceSettings)
    source = dl_dynconfig.CachedS3Source.from_settings(settings)

    await source.store({"version": 1})
    result = await source.fetch()
    assert result == {"version": 1}

    await source.store({"version": 2})
    result = await source.fetch()
    assert result == {"version": 2}


@pytest.mark.asyncio
async def test_cached_s3_source_check_readiness(
    s3_client: types_utils.AiobotocoreS3Client,
    s3_hostport: dl_testing.HostPort,
    s3_access_key: str,
    s3_secret_key: str,
    s3_bucket: str,
) -> None:
    s3_key = "test_cached_s3_source_check_readiness"
    settings = dl_dynconfig.BaseSourceSettings.factory(
        {
            "TYPE": "cached_s3",
            "ENDPOINT_URL": f"http://{s3_hostport.as_pair()}",
            "ACCESS_KEY_ID": s3_access_key,
            "SECRET_ACCESS_KEY": s3_secret_key,
            "BUCKET": s3_bucket,
            "KEY": s3_key,
            "TTL": datetime.timedelta(minutes=5),
        }
    )
    assert isinstance(settings, dl_dynconfig.CachedS3SourceSettings)
    source = dl_dynconfig.CachedS3Source.from_settings(settings)
    assert not await source.check_readiness()

    await s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=b"test")
    await source.fetch()
    assert await source.check_readiness()

    await s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    assert await source.check_readiness()
