from __future__ import annotations

import abc
from typing import Dict, Any

from marshmallow import Schema, post_load


class BaseQEAPISchema(Schema):
    @post_load(pass_many=False)
    def post_load(self, data: Dict, **_: Any) -> Any:
        return self.to_object(data)

    @abc.abstractmethod
    def to_object(self, data: Dict[str, Any]) -> Any:
        raise NotImplementedError()
