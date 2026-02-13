import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator


@attr.s
class RegexObfuscator(BaseObfuscator):
    # TODO: dummy obfuscator - fix in BI-6831
    patterns: list[str] = attr.ib(factory=list)

    def obfuscate(self, text: str, context: ObfuscationContext) -> str:
        return text
