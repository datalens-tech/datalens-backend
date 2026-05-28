from __future__ import annotations

from typing import (
    Any,
    Mapping,
)

from marshmallow.fields import Field

from dl_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
)


class ConnectionRefField(Field):
    def _serialize(self, value: ConnectionRef | None, attr: str | None, obj: Any, **kwargs: Any) -> str | None:
        if value is None:
            raise ValueError("Can not serialize data source without connection reference")
        if isinstance(value, DefaultConnectionRef):
            return value.conn_id
        return None

    def _deserialize(
        self, value: str | None, attr: str | None, data: Mapping[str, Any] | None, **kwargs: Any
    ) -> ConnectionRef:
        """
        Wraps connection ID with ConnRef object
        `None` value will not be passed here, so we should handle it in `.to_object()`
        """
        return DefaultConnectionRef(conn_id=value)  # type: ignore  # 2024-01-30 # TODO: Argument "conn_id" to "DefaultConnectionRef" has incompatible type "str | None"; expected "str"  [arg-type]
