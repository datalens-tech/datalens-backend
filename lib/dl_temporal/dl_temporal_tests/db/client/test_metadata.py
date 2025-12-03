import asyncio
import datetime

import pytest
import pytest_mock

import dl_temporal


@pytest.mark.asyncio
async def test_ttl(
    mocker: pytest_mock.MockerFixture,
) -> None:
    ttl = datetime.timedelta(microseconds=5000)

    metadata_provider = mocker.Mock(spec=dl_temporal.MetadataProvider)
    metadata_provider.get_metadata.return_value = {"test": "test_before"}
    metadata_provider.ttl = ttl

    temporal_client = await dl_temporal.TemporalClient.from_dependencies(
        dependencies=dl_temporal.TemporalClientDependencies(
            host="test-host",
            port=1234,
            tls=False,
            namespace="test-namespace",
            metadata_provider=metadata_provider,
        ),
    )
    assert temporal_client.base_client.rpc_metadata == {"test": "test_before"}

    metadata_provider.get_metadata.return_value = {"test": "test_after"}
    await asyncio.sleep(ttl.total_seconds() * 2)

    assert temporal_client.base_client.rpc_metadata == {"test": "test_after"}

    await temporal_client.close()
