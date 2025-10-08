import pytest

import dl_temporal


@pytest.mark.asyncio
async def test_check_auth(temporal_client: dl_temporal.TemporalClient) -> None:
    assert await temporal_client.check_auth()
