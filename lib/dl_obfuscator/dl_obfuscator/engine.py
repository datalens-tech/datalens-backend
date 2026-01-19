import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator


@attr.s
class ObfuscationEngine:
    """Core engine responsible for applying obfuscation rules"""

    _obfuscators: list[BaseObfuscator] = attr.ib(factory=list)

    def add_obfuscator(self, obfuscator: BaseObfuscator) -> None:
        self._obfuscators.append(obfuscator)

    def obfuscate_text(self, text: str, context: ObfuscationContext) -> str:
        for obfuscator in self._obfuscators:
            text = obfuscator.obfuscate(text, context)
        return text

    def obfuscate(self, data: str, context: ObfuscationContext) -> str:
        """Main method. Extension point for obfuscating other data types (e.g. dict)"""
        return self.obfuscate_text(data, context)
