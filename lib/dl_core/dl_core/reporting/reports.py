from __future__ import annotations

import enum
from typing import (
    ClassVar,
    Dict,
    Optional,
    Union,
)

import attr
from typing_extensions import Literal

from dl_constants.enums import (
    ConnectionType,
    QueryType,
)


# TODO FIX: Use as intermediate entity (at this moment is not used)
@attr.s(frozen=True, auto_attribs=True)
class DbQueryExecutionReport:
    event_code: ClassVar[str] = "profile_db_request"

    query_id: str
    dataset_id: str
    user_id: Optional[str]
    billing_folder_id: Optional[str]
    connection_id: str
    connection_type: ConnectionType
    source: Optional[str]
    username: Optional[str]
    execution_time: int
    query: Optional[str]
    status: Union[Literal["success"], Literal["error"]]
    error: Optional[str]
    host: str

    cache_used: bool
    cache_full_hit: bool

    # not in action fields:
    query_type: QueryType
    is_public: bool

    def convert_for_logging_extras(self, value) -> Union[str, int, bool, None]:  # type: ignore  # TODO: fix
        if value is None:
            return None
        elif isinstance(value, (str, int, bool)):
            return value
        elif isinstance(value, enum.Enum):
            return value.name
        else:
            return repr(value)

    def to_logging_extras(self) -> Dict[str, Union[str, int, bool]]:
        return dict(
            {  # type: ignore  # TODO: fix
                k: self.convert_for_logging_extras(v) for k, v in attr.asdict(self)  # type: ignore  # TODO: fix
            },
            event_code=self.event_code,
        )
