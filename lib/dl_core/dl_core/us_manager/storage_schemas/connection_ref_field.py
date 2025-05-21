from __future__ import annotations

from typing import (
    Any,
    Mapping,
    Optional,
)

from marshmallow.fields import Field

from dl_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
)


class ConnectionRefField(Field):
    def _serialize(self, value: Optional[ConnectionRef], attr: Optional[str], obj: Any, **kwargs: Any) -> Optional[str]:
        if value is None:
            raise ValueError("Can not serialize data source without connection reference")
        if isinstance(value, DefaultConnectionRef):
            return value.conn_id
        return None

    def _deserialize(
        self, value: Optional[str], attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any
    ) -> ConnectionRef:
        """ Wraps connection ID with ConnRef object """
        return DefaultConnectionRef(conn_id=value)
