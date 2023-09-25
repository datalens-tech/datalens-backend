from __future__ import annotations

import pytest

from dl_formula.utils.caching import multi_cached_with_errors


def test_cached_with_errors():
    counter = 0

    @multi_cached_with_errors(maxsize=3, cache_exceptions=(TypeError,))
    def my_func(a: int, b: int) -> int:
        nonlocal counter
        counter += 1
        return a // b

    assert my_func(2, 2) == 1
    assert counter == 1
    assert my_func(2, 2) == 1
    assert counter == 1

    assert my_func(4, 2) == 2
    assert my_func(6, 2) == 3
    assert my_func(8, 2) == 4
    assert counter == 4

    assert my_func(2, 2) == 1  # cache overflow
    assert counter == 5
    assert my_func(6, 2) == 3
    assert counter == 5

    # cached exception
    assert pytest.raises(TypeError, my_func, None, None)
    assert counter == 6
    assert pytest.raises(TypeError, my_func, None, None)
    assert counter == 6
    # noncached exception
    assert pytest.raises(ZeroDivisionError, my_func, 2, 0)
    assert counter == 7
    assert pytest.raises(ZeroDivisionError, my_func, 2, 0)
    assert counter == 8

    my_func.cache_clear()
    assert pytest.raises(TypeError, my_func, None, None)
    assert counter == 9


def test_multi_cache():
    counter = 0

    def cache_qualifier(value: str) -> str:
        return value[0].lower()

    @multi_cached_with_errors(maxsize=2, cache_qualifier=cache_qualifier)
    def my_func(value: str) -> str:
        nonlocal counter
        counter += 1
        return value.upper()

    assert my_func("panda") == "PANDA"  # miss
    assert my_func("puma") == "PUMA"  # miss
    assert my_func("tiger") == "TIGER"  # miss
    assert my_func("parrot") == "PARROT"  # miss, overflow -> "panda" ejected
    assert my_func("tiger") == "TIGER"  # hit
    assert my_func("panda") == "PANDA"  # miss because of "parrot"

    assert counter == 5  # the number of misses

    cache_info = my_func.cache_info()
    assert set(cache_info.keys()) == {"p", "t"}
    assert cache_info["p"].misses == 4
    assert cache_info["p"].hits == 0
    assert cache_info["t"].misses == 1
    assert cache_info["t"].hits == 1
