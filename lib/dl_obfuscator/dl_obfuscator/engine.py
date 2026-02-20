from typing import (
    TypeVar,
    cast,
)

import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator
from dl_obfuscator.obfuscators.secret import SecretObfuscator
from dl_obfuscator.secret_keeper import SecretKeeper


ObfuscatableData = str | None | dict[str, "ObfuscatableData"] | list["ObfuscatableData"]

_ObfuscatableT = TypeVar("_ObfuscatableT", bound=ObfuscatableData)


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

    def obfuscate(self, data: _ObfuscatableT, context: ObfuscationContext) -> _ObfuscatableT:
        if data is None:
            return None
        if isinstance(data, str):
            return cast(_ObfuscatableT, self._obfuscate_text(data, context))
        if isinstance(data, dict):
            return cast(_ObfuscatableT, {key: self.obfuscate(value, context) for key, value in data.items()})
        if isinstance(data, list):
            return cast(_ObfuscatableT, [self.obfuscate(item, context) for item in data])
        raise TypeError(
            f"Cannot obfuscate type {type(data).__name__}. Only str, None, dict[str, ...], list are supported."
        )


def create_base_obfuscators(
    global_keeper: SecretKeeper | None = None,
    extra_regex_patterns: tuple[str, ...] | None = None,
) -> tuple[BaseObfuscator, ...]:
    obfuscators: list[BaseObfuscator] = []
    if global_keeper is None:
        global_keeper = SecretKeeper()
    obfuscators.append(SecretObfuscator(keeper=global_keeper))
    # obfuscators.append(RegexObfuscator())
    # TODO: BI-6831
    return tuple(obfuscators)


def create_request_engine(
    base_obfuscators: tuple[BaseObfuscator, ...],
    secret_keeper: SecretKeeper | None = None,
) -> ObfuscationEngine:
    engine = ObfuscationEngine(base_obfuscators=list(base_obfuscators))
    if secret_keeper is not None:
        engine.add_request_obfuscator(SecretObfuscator(keeper=secret_keeper))
    return engine
