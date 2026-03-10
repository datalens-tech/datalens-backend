import enum
from typing import (
    TypeVar,
    cast,
)

import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator
from dl_obfuscator.obfuscators.regex import (
    DEFAULT_PATTERNS,
    RegexObfuscator,
)
from dl_obfuscator.obfuscators.secret import SecretObfuscator
from dl_obfuscator.secret_keeper import SecretKeeper


ObfuscatableData = str | None | dict[str, "ObfuscatableData"] | list["ObfuscatableData"]

_ObfuscatableT = TypeVar("_ObfuscatableT", bound=ObfuscatableData)


@enum.unique
class OnObfuscationError(enum.Enum):
    FAIL = "FAIL"
    SKIP = "SKIP"
    RETURN_ORIGINAL = "RETURN_ORIGINAL"


@attr.s
class ObfuscationEngine:
    """Core engine responsible for applying obfuscation rules"""

    _base_obfuscators: list[BaseObfuscator] = attr.ib(factory=list)
    _request_obfuscators: list[BaseObfuscator] = attr.ib(factory=list)
    _obfuscation_error_message: str = attr.ib(default="!OBFUSCATION ERROR!")

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

    def obfuscate(
        self,
        data: _ObfuscatableT,
        context: ObfuscationContext,
        on_error: OnObfuscationError = OnObfuscationError.FAIL,
    ) -> _ObfuscatableT:
        if data is None:
            return None
        if isinstance(data, str):
            try:
                return cast(_ObfuscatableT, self._obfuscate_text(data, context))
            except Exception:
                if on_error is OnObfuscationError.SKIP:
                    return cast(_ObfuscatableT, self._obfuscation_error_message)
                if on_error is OnObfuscationError.RETURN_ORIGINAL:
                    return cast(_ObfuscatableT, data)
                raise
        if isinstance(data, dict):
            return cast(_ObfuscatableT, {key: self.obfuscate(value, context, on_error) for key, value in data.items()})
        if isinstance(data, list):
            return cast(_ObfuscatableT, [self.obfuscate(item, context, on_error) for item in data])

        if on_error is OnObfuscationError.SKIP:
            return cast(_ObfuscatableT, self._obfuscation_error_message)
        if on_error is OnObfuscationError.RETURN_ORIGINAL:
            return data
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
    regex_patterns = DEFAULT_PATTERNS
    if extra_regex_patterns is not None:
        regex_patterns = regex_patterns + extra_regex_patterns
    obfuscators.append(RegexObfuscator(patterns=regex_patterns))
    return tuple(obfuscators)


def create_request_engine(
    base_obfuscators: tuple[BaseObfuscator, ...],
    secret_keeper: SecretKeeper | None = None,
) -> ObfuscationEngine:
    engine = ObfuscationEngine(base_obfuscators=list(base_obfuscators))
    if secret_keeper is not None:
        engine.add_request_obfuscator(SecretObfuscator(keeper=secret_keeper))
    return engine
