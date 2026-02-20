import re
from typing import ClassVar

import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator


@attr.s
class RegexObfuscator(BaseObfuscator):
    DEFAULT_PATTERNS: ClassVar[tuple[str, ...]] = (
        r"ey[JKL][\w-]{13,}\.ey[JKL][\w-]{8,}\.[\w-]{20,}",  # JWT
        r"[a-z0-9]{3,20}://[a-zA-Z0-9_-]{3,20}:[^$][^:@/\s]{6,40}@[a-z0-9.-]{10,}",  # Credentials in URL
        r"Authorization:\s*\S+(?:\s+\S+)?",
        r"[Bb][Aa][Ss][Ii][Cc] [A-Za-z0-9+/=]{16,}",
        r"[Bb][Ee][Aa][Rr][Ee][Rr] \S{20,}",
        r"[Oo][Aa][Uu][Tt][Hh] \S{20,}",
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
