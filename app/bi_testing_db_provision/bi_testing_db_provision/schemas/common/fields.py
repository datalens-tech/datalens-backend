from __future__ import annotations

from typing import Any, Tuple, Optional, List

from marshmallow import fields


class TupleField(fields.List):
    def _deserialize(self, value, attr, data, **kwargs) -> Tuple[Any, ...]:  # type: ignore  # TODO: fix
        return tuple(super()._deserialize(value, attr, data, **kwargs))

    def _serialize(self, value, attr, obj, **kwargs) -> Optional[List[Any]]:  # type: ignore  # TODO: fix
        return super()._serialize(list(value), attr, obj, **kwargs)
