from __future__ import annotations

import abc
from typing import Any

from marshmallow import (
    EXCLUDE,
    Schema,
    post_load,
)


class BaseQEAPISchema(Schema):
    class Meta:
        # Forward-compatibility: the producer (data-api) and the consumer (RQE) are deployed
        # independently, so during a rolling deploy the producer may send fields that an older
        # consumer does not know yet. Dropping unknown fields instead of raising keeps
        # deserialization working across version skew.
        unknown = EXCLUDE

    @post_load(pass_many=False)
    def post_load(self, data: dict, **_: Any) -> Any:
        return self.to_object(data)

    @abc.abstractmethod
    def to_object(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError()
