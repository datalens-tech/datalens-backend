import abc

import attr


@attr.s
class BaseContext(metaclass=abc.ABCMeta):  # noqa: B024
    pass


class BaseContextFabric(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def make(self) -> BaseContext:
        pass

    @abc.abstractmethod
    async def tear_down(self, inst: BaseContext) -> None:
        pass
