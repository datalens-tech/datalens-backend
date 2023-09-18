from typing import (
    Any,
    ClassVar,
    Iterable,
    List,
    Optional,
    Tuple,
)

import attr
import grpc


class GHeaders(dict):
    @property
    def as_tuples(self) -> List[Tuple[str, Any]]:
        return [(k, v) for k, v in self.items()]

    def to_metadata(self) -> List[Tuple[str, str]]:
        return [(str(k).lower(), str(v)) for k, v in self.items()]


@attr.s(frozen=True)
class GAuthorizationData:
    """Just a wrapper for authorization header to prevent display in repr()"""

    authorization_header: Optional[str] = attr.ib(repr=False)

    @classmethod
    def from_invocation_metadata(cls, meta: Iterable[Tuple[str, str]]) -> "GAuthorizationData":
        return cls(authorization_header=next((val for key, val in meta if key.lower() == "authorization"), None))


class GRequestData:
    data_attr_name: ClassVar[str] = "_grpc_req_app_data"

    key_req_id: ClassVar[str] = "request_id"
    key_auth_data: ClassVar[str] = "auth_data"

    @classmethod
    def get_ctx_data(cls, ctx: grpc.ServicerContext) -> dict[str, Any]:
        data: dict[str, Any]

        if hasattr(ctx, cls.data_attr_name):
            data = getattr(ctx, cls.data_attr_name)  # type: ignore
            assert isinstance(data, dict)
            return data

        data = {}
        setattr(ctx, cls.data_attr_name, data)
        return data

    @classmethod
    def set_request_id(cls, ctx: grpc.ServicerContext, req_id: str) -> None:
        cls.get_ctx_data(ctx)[cls.key_req_id] = req_id

    @classmethod
    def set_auth_data(cls, ctx: grpc.ServicerContext, auth_data: GAuthorizationData) -> None:
        cls.get_ctx_data(ctx)[cls.key_auth_data] = auth_data

    @classmethod
    def get_request_id(cls, ctx: grpc.ServicerContext) -> str:
        ret = cls.get_ctx_data(ctx)[cls.key_req_id]
        assert isinstance(ret, str), "Unexpected type of request ID in GRPC service context."
        return ret

    @classmethod
    def get_auth_data(cls, ctx: grpc.ServicerContext) -> GAuthorizationData:
        ret = cls.get_ctx_data(ctx)[cls.key_auth_data]
        assert isinstance(ret, GAuthorizationData), "Unexpected type of GAuthorizationData in GRPC service context."
        return ret
