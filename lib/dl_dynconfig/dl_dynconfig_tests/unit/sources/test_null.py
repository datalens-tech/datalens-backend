import pytest

import dl_dynconfig


@pytest.mark.asyncio
async def test_null_source() -> None:
    source = dl_dynconfig.NullSource.from_settings(dl_dynconfig.NullSourceSettings())
    assert await source.fetch() == {}
    await source.store({"key": "value"})
    assert await source.fetch() == {}
