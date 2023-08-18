import abc
import attr


@attr.s
class BaseContext(metaclass=abc.ABCMeta):
    pass


class BaseContextFabric(metaclass=abc.ABCMeta):
    async def make(self) -> BaseContext:
        pass

    async def tear_down(self, inst: BaseContext):
        pass
