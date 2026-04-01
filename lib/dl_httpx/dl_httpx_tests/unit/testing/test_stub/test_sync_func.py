import pytest

import dl_httpx.testing as dl_httpx_testing


def test_result() -> None:
    @dl_httpx_testing.tracked
    def add(a: int, b: int) -> int:
        return a + b

    assert add(1, 2) == 3


def test_calls_registered() -> None:
    @dl_httpx_testing.tracked
    def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    func(1, "y")
    func(2, b="z")
    assert func.calls == [
        dl_httpx_testing.Call(args=(1, "y"), kwargs={}),
        dl_httpx_testing.Call(args=(2,), kwargs={"b": "z"}),
    ]


def test_reset() -> None:
    @dl_httpx_testing.tracked
    def func() -> None:
        pass

    func()
    func.reset()
    assert func.calls == []


def test_assert_called_once_passes() -> None:
    @dl_httpx_testing.tracked
    def func() -> None:
        pass

    func()
    func.assert_called_once()


def test_assert_called_once_not_called() -> None:
    @dl_httpx_testing.tracked
    def func() -> None:
        pass

    with pytest.raises(AssertionError, match="'func' to be called once"):
        func.assert_called_once()


def test_assert_not_called_passes() -> None:
    @dl_httpx_testing.tracked
    def func() -> None:
        pass

    func.assert_not_called()


def test_assert_not_called_fails() -> None:
    @dl_httpx_testing.tracked
    def func() -> None:
        pass

    func()
    with pytest.raises(AssertionError, match="'func' not to be called, but it was called 1 time"):
        func.assert_not_called()


def test_assert_called_once_with_passes() -> None:
    @dl_httpx_testing.tracked
    def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    func(1, b="y")
    func.assert_called_once_with(1, b="y")


def test_assert_called_once_with_fails_not_called() -> None:
    @dl_httpx_testing.tracked
    def func(a: int) -> int:
        return a

    with pytest.raises(AssertionError, match="'func' to be called once"):
        func.assert_called_once_with(1)


def test_assert_called_once_with_fails_wrong_args() -> None:
    @dl_httpx_testing.tracked
    def func(a: int, b: str = "x") -> str:
        return f"{a}{b}"

    func(1, b="y")
    with pytest.raises(AssertionError, match="'func' to be called with"):
        func.assert_called_once_with(2, b="y")
