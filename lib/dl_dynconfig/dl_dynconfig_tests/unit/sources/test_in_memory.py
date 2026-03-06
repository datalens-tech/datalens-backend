import pytest

import dl_dynconfig


@pytest.mark.asyncio
async def test_in_memory_source_fetch() -> None:
    data = {"key": "value"}
    source = dl_dynconfig.InMemorySource(data=data)
    result = await source.fetch()
    assert result == data


@pytest.mark.asyncio
async def test_in_memory_source_returns_copy() -> None:
    data = {"key": "value"}
    source = dl_dynconfig.InMemorySource(data=data)
    result = await source.fetch()
    result["key"] = "modified"
    assert await source.fetch() == {"key": "value"}
