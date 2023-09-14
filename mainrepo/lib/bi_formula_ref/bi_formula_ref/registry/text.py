from __future__ import annotations

from typing import (
    Any,
    Dict,
)

import attr


@attr.s(frozen=True)
class ParameterizedText:
    text: str = attr.ib(kw_only=True)
    params: Dict[str, str] = attr.ib(kw_only=True, factory=dict)

    def format(self) -> str:
        if not self.params:
            return self.text
        return self.text.format(**self.params)

    def __str__(self) -> str:
        return self.format()

    def clone(self, **kwargs: Any) -> ParameterizedText:
        return attr.evolve(self, **kwargs)
