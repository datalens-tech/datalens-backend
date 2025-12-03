import pytest

import dl_temporal


@pytest.mark.asyncio
async def test_default(temporal_client: dl_temporal.TemporalClient) -> None:
    result = await temporal_client.check_health()
    assert result


@pytest.mark.asyncio
async def test_unavailable() -> None:
    temporal_client = await dl_temporal.TemporalClient.from_dependencies(
        dependencies=dl_temporal.TemporalClientDependencies(
            host="unavailable_host",
            port=8080,
            tls=False,
            namespace="dl_temporal_tests",
        ),
    )
    result = await temporal_client.check_health()
    assert not result

    await temporal_client.close()
