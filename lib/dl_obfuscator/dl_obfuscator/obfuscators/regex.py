import re
from typing import ClassVar

import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator


@attr.s
class RegexObfuscator(BaseObfuscator):
    DEFAULT_PATTERNS: ClassVar[tuple[str, ...]] = (
        r"ey[JKL][\w\\-]{13,}\.ey[JKL][\w\\-]{8,}\.[\w\\-]{20,}",  # JWT
        r"[a-z0-9]{3,20}://[a-zA-Z0-9_\-\\]{3,20}:[^$][^:@/\s]{6,40}@[a-z0-9.\-]{10,}",  # Credentials in URL (user:pass@host)
        r"Authorization:\s*\S+(?:\s+\S+)?",
        r"[B|b][A|a][S|s][I|i][C|c] [A-Za-z0-9+/=]{16,}",
        r"[B|b][E|e][A|a][R|r][E|e][R|r] \S{20,}",
        r"[O|o][A|a][U|u][T|t][H|h] \S{20,}",
    )

    patterns: tuple[str, ...] = attr.ib()
    _compiled_patterns: tuple[re.Pattern[str], ...] = attr.ib(repr=False)
    _replacement: str = attr.ib(default="***")

    @classmethod
    def create(
        cls,
        patterns: tuple[str, ...] | None = None,
        replacement: str = "***",
    ) -> "RegexObfuscator":
        if patterns is None:
            patterns = cls.DEFAULT_PATTERNS
        compiled = tuple(re.compile(p) for p in patterns)
        return cls(patterns=patterns, compiled_patterns=compiled, replacement=replacement)

    def obfuscate(self, text: str, context: ObfuscationContext) -> str:
        for pattern in self._compiled_patterns:
            text = pattern.sub(self._replacement, text)
        return text
