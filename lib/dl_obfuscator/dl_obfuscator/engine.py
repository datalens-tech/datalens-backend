from functools import reduce
import re
from typing import Any

import attr

from .context import ObfuscationContext
from .obfuscators.base import BaseObfuscator
from .secret_keeper import SecretKeeper


@attr.s
class ObfuscationEngine:
    """Core engine responsible for applying obfuscation rules"""

    secret_keeper: SecretKeeper = attr.ib(repr=False)
    _obfuscators: list[BaseObfuscator] = attr.ib(factory=list, init=False)

    def __init__(self, secret_keeper: SecretKeeper, obfuscators: list[BaseObfuscator] | None = None):
        self.secret_keeper = secret_keeper
        self._obfuscators = obfuscators or []

    def add_obfuscator(self, obfuscator: BaseObfuscator) -> None:
        self._obfuscators.append(obfuscator)

    def obfuscate_text(self, text: str, context: ObfuscationContext) -> str:
        def apply_replacement(text: str, secret_items: tuple[str, str]) -> str:
            secret, replacement = secret_items
            escaped_value = re.escape(secret)
            pattern = rf"\b{escaped_value}\b"
            replacement = replacement or "hidden"
            replacement = f"***{replacement}***"
            return re.sub(pattern, replacement, text)

        secrets = list(self.secret_keeper.get_secrets().items())
        if context != ObfuscationContext.INSPECTOR:
            secrets.extend(self.secret_keeper.get_params().items())

        result = reduce(apply_replacement, secrets, text)

        for obfuscator in self._obfuscators:
            result = obfuscator.obfuscate(result, context)

        return result

    def obfuscate_dict(self, data: dict[str, Any], context: ObfuscationContext) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, str):
                result[key] = self.obfuscate_text(value, context)
            elif isinstance(value, dict):
                result[key] = self.obfuscate_dict(value, context)
            else:
                result[key] = value
        return result

    def obfuscate(self, data: Any, context: ObfuscationContext) -> Any:
        if isinstance(data, str):
            return self.obfuscate_text(data, context)
        elif isinstance(data, dict):
            return self.obfuscate_dict(data, context)
        else:
            return data
