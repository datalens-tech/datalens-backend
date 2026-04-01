import warnings

import pytest

import dl_httpx.testing as dl_httpx_testing


@pytest.mark.asyncio
async def test_result() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def multiply(self, x: int) -> int:
            return x * 2

    assert await MyClass().multiply(5) == 10


@pytest.mark.asyncio
async def test_calls_registered() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    await obj.method(1, "y")
    await obj.method(2, b="z")
    assert obj.method.calls == [
        dl_httpx_testing.Call(args=(1, "y"), kwargs={}),
        dl_httpx_testing.Call(args=(2,), kwargs={"b": "z"}),
    ]


@pytest.mark.asyncio
async def test_per_instance() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj1 = MyClass()
    obj2 = MyClass()

    await obj1.method()
    assert obj1.method.calls == [dl_httpx_testing.Call(args=(), kwargs={})]
    assert obj2.method.calls == []


@pytest.mark.asyncio
async def test_reset() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    await obj.method()
    obj.method.reset()
    assert obj.method.calls == []


@pytest.mark.asyncio
async def test_assert_called_once_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    await obj.method()
    obj.method.assert_called_once()


@pytest.mark.asyncio
async def test_assert_called_once_not_called() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    with pytest.raises(AssertionError, match="'method' to be called once"):
        obj.method.assert_called_once()


@pytest.mark.asyncio
async def test_assert_not_called_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    obj.method.assert_not_called()


@pytest.mark.asyncio
async def test_assert_not_called_fails() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    await obj.method()
    with pytest.raises(AssertionError, match="'method' not to be called, but it was called 1 time"):
        obj.method.assert_not_called()


@pytest.mark.asyncio
async def test_assert_awaited_once_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    await obj.method()
    assert obj.method.calls[0].awaited
    obj.method.assert_awaited_once()


@pytest.mark.asyncio
async def test_assert_awaited_once_fails() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    with pytest.raises(AssertionError, match="'method' to be awaited once"):
        obj.method.assert_awaited_once()


@pytest.mark.asyncio
async def test_assert_not_awaited_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    obj.method.assert_not_awaited()


@pytest.mark.asyncio
async def test_assert_not_awaited_fails() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    await obj.method()
    with pytest.raises(AssertionError, match="'method' not to be awaited, but it was awaited 1 time"):
        obj.method.assert_not_awaited()


@pytest.mark.asyncio
async def test_assert_called_once_with_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    await obj.method(1, b="y")
    obj.method.assert_called_once_with(1, b="y")


@pytest.mark.asyncio
async def test_assert_called_once_with_fails_not_called() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self, a: int) -> int:
            return a

    obj = MyClass()
    with pytest.raises(AssertionError, match="'method' to be called once"):
        obj.method.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_assert_called_once_with_fails_wrong_args() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    await obj.method(1, b="y")
    with pytest.raises(AssertionError, match="'method' to be called with"):
        obj.method.assert_called_once_with(2, b="y")


@pytest.mark.asyncio
async def test_assert_awaited_once_with_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    await obj.method(1, b="y")
    obj.method.assert_awaited_once_with(1, b="y")


@pytest.mark.asyncio
async def test_assert_awaited_once_with_fails_not_awaited() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self, a: int) -> int:
            return a

    obj = MyClass()
    with pytest.raises(AssertionError, match="'method' to be awaited once"):
        obj.method.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_assert_awaited_once_with_fails_wrong_args() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    await obj.method(1, b="y")
    with pytest.raises(AssertionError, match="'method' to be awaited with"):
        obj.method.assert_awaited_once_with(2, b="y")


@pytest.mark.asyncio
async def test_assert_awaited_once_fails_if_called_not_awaited() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        obj.method().close()

    with pytest.raises(AssertionError, match="'method' to be awaited once"):
        obj.method.assert_awaited_once()


@pytest.mark.asyncio
async def test_assert_not_awaited_passes_if_called_not_awaited() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        async def method(self) -> None:
            pass

    obj = MyClass()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        obj.method().close()

    obj.method.assert_not_awaited()
