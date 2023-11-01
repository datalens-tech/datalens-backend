from __future__ import annotations

from typing import Any

import attr


@attr.s(frozen=True)
class ParameterizedText:
    text: str = attr.ib(kw_only=True)
    params: dict[str, str] = attr.ib(kw_only=True, factory=dict)

    def format(self) -> str:
        if not self.params:
            return self.text
        return self.text.format(**self.params)

    def __str__(self) -> str:
        return self.format()

    def __bool__(self) -> bool:
        return bool(self.text)

    def clone(self, **kwargs: Any) -> ParameterizedText:
        return attr.evolve(self, **kwargs)
