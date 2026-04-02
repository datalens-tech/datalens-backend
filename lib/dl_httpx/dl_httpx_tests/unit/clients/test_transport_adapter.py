import httpx
import pytest
import pytest_mock

import dl_httpx


def test_no_transport_adapter_context_yields_input(mocker: pytest_mock.MockFixture) -> None:
    transport = mocker.Mock(spec=httpx.BaseTransport)
    request = mocker.Mock(spec=httpx.Request)
    adapter = dl_httpx.NoTransportAdapter()

    with adapter.context(transport, request) as result:
        assert result is transport


def test_no_transport_adapter_context_does_not_close_transport(mocker: pytest_mock.MockFixture) -> None:
    transport = mocker.Mock(spec=httpx.BaseTransport)
    request = mocker.Mock(spec=httpx.Request)
    adapter = dl_httpx.NoTransportAdapter()

    with adapter.context(transport, request):
        pass

    transport.close.assert_not_called()


@pytest.mark.asyncio
async def test_no_transport_adapter_context_async_yields_input(mocker: pytest_mock.MockFixture) -> None:
    transport = mocker.Mock(spec=httpx.AsyncBaseTransport)
    request = mocker.Mock(spec=httpx.Request)
    adapter = dl_httpx.NoTransportAdapter()

    async with adapter.context_async(transport, request) as result:
        assert result is transport


@pytest.mark.asyncio
async def test_no_transport_adapter_context_async_does_not_close_transport(mocker: pytest_mock.MockFixture) -> None:
    transport = mocker.Mock(spec=httpx.AsyncBaseTransport)
    request = mocker.Mock(spec=httpx.Request)
    adapter = dl_httpx.NoTransportAdapter()

    async with adapter.context_async(transport, request):
        pass

    transport.aclose.assert_not_called()
