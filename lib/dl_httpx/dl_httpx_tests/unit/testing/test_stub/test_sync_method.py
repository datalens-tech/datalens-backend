import pytest

import dl_httpx.testing as dl_httpx_testing


def test_result() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def multiply(self, x: int) -> int:
            return x * 2

    assert MyClass().multiply(5) == 10


def test_calls_registered() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    obj.method(1, "y")
    obj.method(2, b="z")
    assert obj.method.calls == [
        dl_httpx_testing.Call(args=(1, "y"), kwargs={}),
        dl_httpx_testing.Call(args=(2,), kwargs={"b": "z"}),
    ]


def test_per_instance() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self) -> None:
            pass

    obj1 = MyClass()
    obj2 = MyClass()

    obj1.method()
    assert obj1.method.calls == [dl_httpx_testing.Call(args=(), kwargs={})]
    assert obj2.method.calls == []


def test_reset() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self) -> None:
            pass

    obj = MyClass()
    obj.method()
    obj.method.reset()
    assert obj.method.calls == []


def test_assert_called_once_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self) -> None:
            pass

    obj = MyClass()
    obj.method()
    obj.method.assert_called_once()


def test_assert_called_once_not_called() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self) -> None:
            pass

    obj = MyClass()
    with pytest.raises(AssertionError, match="'method' to be called once"):
        obj.method.assert_called_once()


def test_assert_not_called_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self) -> None:
            pass

    obj = MyClass()
    obj.method.assert_not_called()


def test_assert_not_called_fails() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self) -> None:
            pass

    obj = MyClass()
    obj.method()
    with pytest.raises(AssertionError, match="'method' not to be called, but it was called 1 time"):
        obj.method.assert_not_called()


def test_assert_called_once_with_passes() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    obj.method(1, b="y")
    obj.method.assert_called_once_with(1, b="y")


def test_assert_called_once_with_fails_not_called() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self, a: int) -> int:
            return a

    obj = MyClass()
    with pytest.raises(AssertionError, match="'method' to be called once"):
        obj.method.assert_called_once_with(1)


def test_assert_called_once_with_fails_wrong_args() -> None:
    class MyClass:
        @dl_httpx_testing.tracked
        def method(self, a: int, b: str = "x") -> str:
            return f"{a}{b}"

    obj = MyClass()
    obj.method(1, b="y")
    with pytest.raises(AssertionError, match="'method' to be called with"):
        obj.method.assert_called_once_with(2, b="y")
