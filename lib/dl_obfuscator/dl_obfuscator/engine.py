from __future__ import annotations

from contextvars import ContextVar
from typing import overload

import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator
from dl_obfuscator.obfuscators.regex import RegexObfuscator
from dl_obfuscator.obfuscators.secret import SecretObfuscator
from dl_obfuscator.secret_keeper import SecretKeeper


ObfuscatableData = str | None | dict[str, "ObfuscatableData"] | list["ObfuscatableData"]

_request_obfuscators: ContextVar[list[BaseObfuscator] | None] = ContextVar("request_obfuscators", default=None)


@attr.s
class ObfuscationEngine:
    """Core engine responsible for applying obfuscation rules"""

    _base_obfuscators: list[BaseObfuscator] = attr.ib(factory=list)

    def add_base_obfuscator(self, obfuscator: BaseObfuscator) -> None:
        self._base_obfuscators.append(obfuscator)

    def add_request_obfuscator(self, obfuscator: BaseObfuscator) -> None:
        current = _request_obfuscators.get()
        if current is None:
            _request_obfuscators.set([obfuscator])
        else:
            current.append(obfuscator)

    def clear_request_obfuscators(self) -> None:
        _request_obfuscators.set(None)

    def _obfuscate_text(self, text: str, context: ObfuscationContext) -> str:
        for obfuscator in self._base_obfuscators:
            text = obfuscator.obfuscate(text, context)

        request_obfuscators = _request_obfuscators.get()
        if request_obfuscators is not None:
            for obfuscator in request_obfuscators:
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
        if not data:
            return data
        if isinstance(data, str):
            return self._obfuscate_text(data, context)
        if isinstance(data, dict):
            return {key: self.obfuscate(value, context) for key, value in data.items()}
        if isinstance(data, list):
            return [self.obfuscate(item, context) for item in data]
        raise TypeError(
            f"Cannot obfuscate type {type(data).__name__}. Only str, None, dict[str, ...], list are supported."
        )


def create_obfuscation_engine(
    global_keeper: SecretKeeper | None = None,
) -> ObfuscationEngine:
    engine = ObfuscationEngine()
    if global_keeper is None:
        global_keeper = SecretKeeper()
    engine.add_base_obfuscator(SecretObfuscator(keeper=global_keeper))
    engine.add_base_obfuscator(RegexObfuscator())
    return engine
