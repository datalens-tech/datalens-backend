import re

import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator


JWT_TOKEN_REGEX = r"ey[JKL][\w-]{13,}\.ey[JKL][\w-]{8,}\.[\w-]{20,}"
URL_CREDENTIALS_REGEX = r"[a-z0-9]{3,20}://[a-zA-Z0-9_-]{3,20}:[^$][^:@/\s]{6,40}@[a-z0-9.-]{10,}"
AUTHORIZATION_HEADER_REGEX = r"Authorization:\s*\S+(?:\s+\S+)?"
BASIC_AUTH_REGEX = r"[Bb][Aa][Ss][Ii][Cc] [A-Za-z0-9+/=]{16,}"
BEARER_TOKEN_REGEX = r"[Bb][Ee][Aa][Rr][Ee][Rr] \S{20,}"
OAUTH_TOKEN_REGEX = r"[Oo][Aa][Uu][Tt][Hh] \S{20,}"

DEFAULT_PATTERNS: tuple[str, ...] = (
    JWT_TOKEN_REGEX,
    URL_CREDENTIALS_REGEX,
    AUTHORIZATION_HEADER_REGEX,
    BASIC_AUTH_REGEX,
    BEARER_TOKEN_REGEX,
    OAUTH_TOKEN_REGEX,
)


@attr.s
class RegexObfuscator(BaseObfuscator):
    patterns: tuple[str, ...] = attr.ib(default=DEFAULT_PATTERNS)
    _replacement: str = attr.ib(default="***")
    _compiled_patterns: tuple[re.Pattern[str], ...] = attr.ib(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._compiled_patterns = tuple(re.compile(p) for p in self.patterns)

    def obfuscate(self, text: str, context: ObfuscationContext) -> str:
        for pattern in self._compiled_patterns:
            text = pattern.sub(self._replacement, text)
        return text
