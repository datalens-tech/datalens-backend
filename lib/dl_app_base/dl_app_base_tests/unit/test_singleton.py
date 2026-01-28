import pytest
from typing_extensions import override

import dl_app_base


def test_sync_function_same_result() -> None:
    @dl_app_base.singleton_function_result
    def test_func() -> object:
        return object()

    result1 = test_func()
    result2 = test_func()

    assert result1 is result2


def test_sync_function_result() -> None:
    value = object()

    @dl_app_base.singleton_function_result
    def test_func() -> object:
        return value

    assert test_func() is value


def test_sync_function_with_args() -> None:
    @dl_app_base.singleton_function_result
    def test_func(arg: int) -> int:
        return arg

    assert test_func(1) == 1
    assert test_func(2) == 1


def test_sync_function_with_kwargs() -> None:
    @dl_app_base.singleton_function_result
    def test_func(arg: int) -> int:
        return arg

    assert test_func(arg=1) == 1
    assert test_func(arg=2) == 1


def test_sync_function_recursive_call() -> None:
    @dl_app_base.singleton_function_result
    def test_func() -> object:
        return test_func()

    with pytest.raises(dl_app_base.LockedAndUnsetError):
        test_func()


@pytest.mark.asyncio
async def test_async_function_same_result() -> None:
    @dl_app_base.singleton_function_result
    async def test_func() -> object:
        return object()

    result1 = await test_func()
    result2 = await test_func()

    assert result1 is result2


@pytest.mark.asyncio
async def test_async_function_result() -> None:
    value = object()

    @dl_app_base.singleton_function_result
    async def test_func() -> object:
        return value

    assert await test_func() is value


@pytest.mark.asyncio
async def test_async_function_with_args() -> None:
    @dl_app_base.singleton_function_result
    async def test_func(arg: int) -> int:
        return arg

    assert await test_func(1) == 1
    assert await test_func(2) == 1


@pytest.mark.asyncio
async def test_async_function_with_kwargs() -> None:
    @dl_app_base.singleton_function_result
    async def test_func(arg: int) -> int:
        return arg

    assert await test_func(arg=1) == 1
    assert await test_func(arg=2) == 1


@pytest.mark.asyncio
async def test_async_function_recursive_call() -> None:
    @dl_app_base.singleton_function_result
    async def test_func() -> object:
        return await test_func()

    with pytest.raises(dl_app_base.LockedAndUnsetError):
        await test_func()


def test_sync_class_method_same_result() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        def test_func(self) -> object:
            return object()

    instance = TestClass()
    result1 = instance.test_func()
    result2 = instance.test_func()

    assert result1 is result2


def test_sync_class_method_unique_per_instance() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        def test_func(self) -> object:
            return object()

    instance1 = TestClass()
    instance2 = TestClass()

    result1 = instance1.test_func()
    result2 = instance2.test_func()

    assert result1 is not result2


def test_sync_class_method_result() -> None:
    value = object()

    class TestClass:
        @dl_app_base.singleton_class_method_result
        def test_func(self) -> object:
            return value

    instance = TestClass()

    assert instance.test_func() is value


def test_sync_class_method_with_args() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        def test_func(self, arg: int) -> int:
            return arg

    instance = TestClass()

    assert instance.test_func(1) == 1
    assert instance.test_func(2) == 1


def test_sync_class_method_with_kwargs() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        def test_func(self, arg: int) -> int:
            return arg

    instance = TestClass()

    assert instance.test_func(arg=1) == 1
    assert instance.test_func(arg=2) == 1


def test_sync_class_method_recursive_call() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        def test_func(self) -> object:
            return self.test_func()

    instance = TestClass()
    with pytest.raises(dl_app_base.LockedAndUnsetError):
        instance.test_func()


def test_sync_class_child_class_result() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        def test_func(self) -> object:
            return object()

    class TestChildClass(TestClass):
        @override
        @dl_app_base.singleton_class_method_result
        def test_func(self) -> object:
            return super().test_func()

    instance = TestChildClass()
    result1 = instance.test_func()
    result2 = instance.test_func()

    assert result1 is result2


@pytest.mark.asyncio
async def test_async_class_method_same_result() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        async def test_func(self) -> object:
            return object()

    instance = TestClass()
    result1 = await instance.test_func()
    result2 = await instance.test_func()

    assert result1 is result2


@pytest.mark.asyncio
async def test_async_class_method_unique_per_instance() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        async def test_func(self) -> object:
            return object()

    instance1 = TestClass()
    instance2 = TestClass()

    result1 = await instance1.test_func()
    result2 = await instance2.test_func()

    assert result1 is not result2


@pytest.mark.asyncio
async def test_async_class_method_result() -> None:
    value = object()

    class TestClass:
        @dl_app_base.singleton_class_method_result
        async def test_func(self) -> object:
            return value

    instance = TestClass()

    assert await instance.test_func() is value


@pytest.mark.asyncio
async def test_async_class_method_with_args() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        async def test_func(self, arg: int) -> int:
            return arg

    instance = TestClass()

    assert await instance.test_func(1) == 1
    assert await instance.test_func(2) == 1


@pytest.mark.asyncio
async def test_async_class_method_with_kwargs() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        async def test_func(self, arg: int) -> int:
            return arg

    instance = TestClass()

    assert await instance.test_func(arg=1) == 1
    assert await instance.test_func(arg=2) == 1


@pytest.mark.asyncio
async def test_async_class_method_recursive_call() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        async def test_func(self) -> object:
            return await self.test_func()

    instance = TestClass()
    with pytest.raises(dl_app_base.LockedAndUnsetError):
        await instance.test_func()


@pytest.mark.asyncio
async def test_async_class_child_class_result() -> None:
    class TestClass:
        @dl_app_base.singleton_class_method_result
        async def test_func(self) -> object:
            return object()

    class TestChildClass(TestClass):
        @override
        @dl_app_base.singleton_class_method_result
        async def test_func(self) -> object:
            return await super().test_func()

    instance = TestChildClass()
    result1 = await instance.test_func()
    result2 = await instance.test_func()

    assert result1 is result2
