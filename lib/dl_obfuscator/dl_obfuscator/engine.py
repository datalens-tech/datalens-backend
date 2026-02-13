from typing import overload

import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator
from dl_obfuscator.obfuscators.regex import RegexObfuscator
from dl_obfuscator.obfuscators.secret import SecretObfuscator
from dl_obfuscator.secret_keeper import SecretKeeper


ObfuscatableData = str | None | dict[str, "ObfuscatableData"] | list["ObfuscatableData"]


@attr.s
class ObfuscationEngine:
    """Core engine responsible for applying obfuscation rules"""

    _base_obfuscators: list[BaseObfuscator] = attr.ib(factory=list)
    _request_obfuscators: list[BaseObfuscator] = attr.ib(factory=list)

    def add_base_obfuscator(self, obfuscator: BaseObfuscator) -> None:
        self._base_obfuscators.append(obfuscator)

    def add_request_obfuscator(self, obfuscator: BaseObfuscator) -> None:
        self._request_obfuscators.append(obfuscator)

    def _obfuscate_text(self, text: str, context: ObfuscationContext) -> str:
        for obfuscator in self._request_obfuscators:
            text = obfuscator.obfuscate(text, context)

        for obfuscator in self._base_obfuscators:
            text = obfuscator.obfuscate(text, context)

        return text

    @overload
    def obfuscate(self, data: str, context: ObfuscationContext) -> str:
        ...

    @overload
    def obfuscate(self, data: None, context: ObfuscationContext) -> None:
        ...

    @overload
    def obfuscate(self, data: dict[str, ObfuscatableData], context: ObfuscationContext) -> dict[str, ObfuscatableData]:
        ...

    @overload
    def obfuscate(self, data: list[ObfuscatableData], context: ObfuscationContext) -> list[ObfuscatableData]:
        ...

    def obfuscate(self, data: ObfuscatableData, context: ObfuscationContext) -> ObfuscatableData:
        if data is None:
            return None
        if isinstance(data, str):
            return self._obfuscate_text(data, context)
        if isinstance(data, dict):
            return {key: self.obfuscate(value, context) for key, value in data.items()}
        if isinstance(data, list):
            return [self.obfuscate(item, context) for item in data]
        raise TypeError(
            f"Cannot obfuscate type {type(data).__name__}. Only str, None, dict[str, ...], list are supported."
        )


def create_base_obfuscators(
    global_keeper: SecretKeeper | None = None,
) -> tuple[BaseObfuscator, ...]:
    obfuscators: list[BaseObfuscator] = []
    if global_keeper is None:
        global_keeper = SecretKeeper()
    obfuscators.append(SecretObfuscator(keeper=global_keeper))
    obfuscators.append(RegexObfuscator())
    return tuple(obfuscators)


def create_request_engine(
    base_obfuscators: tuple[BaseObfuscator, ...],
    secret_keeper: SecretKeeper | None = None,
) -> ObfuscationEngine:
    engine = ObfuscationEngine(base_obfuscators=list(base_obfuscators))
    if secret_keeper is not None:
        engine.add_request_obfuscator(SecretObfuscator(keeper=secret_keeper))
    return engine
