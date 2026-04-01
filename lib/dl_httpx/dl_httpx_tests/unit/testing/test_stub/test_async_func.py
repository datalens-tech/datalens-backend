import warnings

import pytest

import dl_httpx.testing as dl_httpx_testing


@pytest.mark.asyncio
async def test_result() -> None:
    @dl_httpx_testing.tracked
    async def add(a: int, b: int) -> int:
        return a + b

    assert await add(1, 2) == 3


@pytest.mark.asyncio
async def test_calls_registered() -> None:
    @dl_httpx_testing.tracked
    async def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    await func(1, "y")
    await func(2, b="z")
    assert func.calls == [
        dl_httpx_testing.Call(args=(1, "y"), kwargs={}),
        dl_httpx_testing.Call(args=(2,), kwargs={"b": "z"}),
    ]


@pytest.mark.asyncio
async def test_reset() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    await func()
    func.reset()
    assert func.calls == []


@pytest.mark.asyncio
async def test_assert_called_once_passes() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    await func()
    func.assert_called_once()


@pytest.mark.asyncio
async def test_assert_called_once_not_called() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    with pytest.raises(AssertionError, match="'func' to be called once"):
        func.assert_called_once()


@pytest.mark.asyncio
async def test_assert_not_called_passes() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    func.assert_not_called()


@pytest.mark.asyncio
async def test_assert_not_called_fails() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    await func()
    with pytest.raises(AssertionError, match="'func' not to be called, but it was called 1 time"):
        func.assert_not_called()


@pytest.mark.asyncio
async def test_assert_awaited_once_passes() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    await func()
    assert func.calls[0].awaited
    func.assert_awaited_once()


@pytest.mark.asyncio
async def test_assert_awaited_once_fails() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    with pytest.raises(AssertionError, match="'func' to be awaited once"):
        func.assert_awaited_once()


@pytest.mark.asyncio
async def test_assert_not_awaited_passes() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    func.assert_not_awaited()


@pytest.mark.asyncio
async def test_assert_not_awaited_fails() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    await func()
    with pytest.raises(AssertionError, match="'func' not to be awaited, but it was awaited 1 time"):
        func.assert_not_awaited()


@pytest.mark.asyncio
async def test_assert_called_once_with_passes() -> None:
    @dl_httpx_testing.tracked
    async def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    await func(1, b="y")
    func.assert_called_once_with(1, b="y")


@pytest.mark.asyncio
async def test_assert_called_once_with_fails_not_called() -> None:
    @dl_httpx_testing.tracked
    async def func(a: int) -> int:
        return a

    with pytest.raises(AssertionError, match="'func' to be called once"):
        func.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_assert_called_once_with_fails_wrong_args() -> None:
    @dl_httpx_testing.tracked
    async def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    await func(1, b="y")
    with pytest.raises(AssertionError, match="'func' to be called with"):
        func.assert_called_once_with(2, b="y")


@pytest.mark.asyncio
async def test_assert_awaited_once_with_passes() -> None:
    @dl_httpx_testing.tracked
    async def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    await func(1, b="y")
    func.assert_awaited_once_with(1, b="y")


@pytest.mark.asyncio
async def test_assert_awaited_once_with_fails_not_awaited() -> None:
    @dl_httpx_testing.tracked
    async def func(a: int) -> int:
        return a

    with pytest.raises(AssertionError, match="'func' to be awaited once"):
        func.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_assert_awaited_once_with_fails_wrong_args() -> None:
    @dl_httpx_testing.tracked
    async def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    await func(1, b="y")
    with pytest.raises(AssertionError, match="'func' to be awaited with"):
        func.assert_awaited_once_with(2, b="y")


@pytest.mark.asyncio
async def test_assert_awaited_once_fails_if_called_not_awaited() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        func().close()

    with pytest.raises(AssertionError, match="'func' to be awaited once"):
        func.assert_awaited_once()


@pytest.mark.asyncio
async def test_assert_not_awaited_passes_if_called_not_awaited() -> None:
    @dl_httpx_testing.tracked
    async def func() -> None:
        pass

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        func().close()

    func.assert_not_awaited()
