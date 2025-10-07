import pytest

import dl_temporal


@pytest.mark.asyncio
async def test_default(temporal_client: dl_temporal.TemporalClient) -> None:
    result = await temporal_client.check_health()
    assert result


@pytest.mark.asyncio
async def test_unavailable() -> None:
    temporal_client = await dl_temporal.TemporalClient.from_settings(
        settings=dl_temporal.TemporalClientSettings(
            host="unavailable_host",
            port=8080,
            namespace="dl_temporal_tests",
        ),
    )
    result = await temporal_client.check_health()
    assert not result

    await temporal_client.close()
