import abc

from ..context import ObfuscationContext


class BaseObfuscator(abc.ABC):
    """
    Base class for custom obfuscators that can be plugged into the obfuscation engine
    """

    @abc.abstractmethod
    def obfuscate(self, text: str, context: ObfuscationContext) -> str:
        raise NotImplementedError
