"""
RDL JSON - Redis DataLens JSON

Serialization with support for custom objects like ``date`` & ``datetime``.
"""

from __future__ import annotations

import abc
import base64
import datetime
import decimal
import ipaddress
import json
from typing import (
    Any,
    ClassVar,
    Generic,
    Type,
    TypeVar,
    Union,
    get_args,
)
import uuid

from dl_constants.types import (
    TJSONExt,
    TJSONLike,
)


_TS_TV = TypeVar("_TS_TV")


class TypeSerializer(Generic[_TS_TV]):
    typename: ClassVar[str]

    @classmethod
    def typeobj(cls) -> Type[_TS_TV]:
        # https://github.com/python/typeshed/issues/7811#issuecomment-1120840824
        # TODO: replace with types.get_original_bases after switching to Python 3.12
        return get_args(cls.__orig_bases__[0])[0]  # type: ignore

    @staticmethod
    @abc.abstractmethod
    def to_jsonable(value: _TS_TV) -> TJSONLike:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def from_jsonable(value: TJSONLike) -> _TS_TV:
        raise NotImplementedError


class DateSerializer(TypeSerializer[datetime.date]):
    typename = "date"

    @staticmethod
    def to_jsonable(obj: datetime.date) -> TJSONLike:
        return obj.isoformat()

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.date:
        assert isinstance(value, str)
        return datetime.date.fromisoformat(value)


class DatetimeSerializer(TypeSerializer[datetime.datetime]):
    typename = "datetime"

    @staticmethod
    def to_jsonable(obj: datetime.datetime) -> TJSONLike:
        utcoffset = obj.utcoffset()
        tzinfo = utcoffset.total_seconds() if utcoffset is not None else None
        return (
            [obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second, obj.microsecond],
            tzinfo,
        )

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.datetime:
        assert isinstance(value, (list, tuple))
        assert len(value) == 2
        parts, offset_sec = value
        assert isinstance(parts, (list, tuple))
        assert len(parts) == 7
        assert all(isinstance(elem, int) for elem in parts)

        assert offset_sec is None or isinstance(offset_sec, float)
        tzinfo = datetime.timezone(datetime.timedelta(seconds=offset_sec)) if offset_sec is not None else None
        return datetime.datetime(*parts, tzinfo=tzinfo)  # type: ignore  # https://github.com/python/mypy/issues/6799


class TimeSerializer(TypeSerializer[datetime.time]):
    typename = "time"

    @staticmethod
    def to_jsonable(obj: datetime.time) -> TJSONLike:
        # Not expecting this to be valid for non-constant time tzinfos.
        utcoffset = obj.tzinfo.utcoffset(datetime.datetime(1970, 1, 1)) if obj.tzinfo is not None else None
        tzinfo = utcoffset.total_seconds() if utcoffset is not None else None
        return [obj.hour, obj.minute, obj.second, obj.microsecond], tzinfo

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.time:
        assert isinstance(value, (list, tuple))
        assert len(value) == 2, value
        parts, offset_sec = value
        assert isinstance(parts, (list, tuple))
        assert len(parts) == 4, parts
        assert all(isinstance(elem, int) for elem in parts)

        assert offset_sec is None or isinstance(offset_sec, (int, float)), offset_sec
        tzinfo = datetime.timezone(datetime.timedelta(seconds=offset_sec)) if offset_sec is not None else None
        return datetime.time(*parts, tzinfo=tzinfo)  # type: ignore  # https://github.com/python/mypy/issues/6799


class TimedeltaSerializer(TypeSerializer[datetime.timedelta]):
    typename = "timedelta"

    @staticmethod
    def to_jsonable(obj: datetime.timedelta) -> TJSONLike:
        return obj.total_seconds()

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.timedelta:
        assert isinstance(value, float)
        return datetime.timedelta(seconds=value)


class DecimalSerializer(TypeSerializer[decimal.Decimal]):
    typename = "decimal"

    @staticmethod
    def to_jsonable(obj: decimal.Decimal) -> TJSONLike:
        return str(obj)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> decimal.Decimal:
        assert isinstance(value, str)
        return decimal.Decimal(value)


class UUIDSerializer(TypeSerializer[uuid.UUID]):
    typename = "uuid"

    @staticmethod
    def to_jsonable(obj: uuid.UUID) -> TJSONLike:
        return str(obj)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> uuid.UUID:
        assert isinstance(value, str)
        return uuid.UUID(value)


class BytesSerializer(TypeSerializer[bytes]):
    typename = "bytes"

    @staticmethod
    def to_jsonable(value: bytes) -> TJSONLike:
        return base64.b64encode(value).decode("ascii")

    @staticmethod
    def from_jsonable(value: TJSONLike) -> bytes:
        assert isinstance(value, str)
        return base64.b64decode(value, validate=True)


class IPv4AddressSerializer(TypeSerializer[ipaddress.IPv4Address]):
    typename = "ipv4_address"

    @staticmethod
    def to_jsonable(value: ipaddress.IPv4Address) -> TJSONLike:
        return str(value)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> ipaddress.IPv4Address:
        assert isinstance(value, str)
        return ipaddress.IPv4Address(value)


class IPv6AddressSerializer(TypeSerializer[ipaddress.IPv6Address]):
    typename = "ipv6_address"

    @staticmethod
    def to_jsonable(value: ipaddress.IPv6Address) -> TJSONLike:
        return str(value)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> ipaddress.IPv6Address:
        assert isinstance(value, str)
        return ipaddress.IPv6Address(value)


class IPv4NetworkSerializer(TypeSerializer[ipaddress.IPv4Network]):
    typename = "ipv4_network"

    @staticmethod
    def to_jsonable(value: ipaddress.IPv4Network) -> TJSONLike:
        return str(value)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> ipaddress.IPv4Network:
        assert isinstance(value, str)
        return ipaddress.IPv4Network(value)


class IPv6NetworkSerializer(TypeSerializer[ipaddress.IPv6Network]):
    typename = "ipv6_network"

    @staticmethod
    def to_jsonable(value: ipaddress.IPv6Network) -> TJSONLike:
        return str(value)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> ipaddress.IPv6Network:
        assert isinstance(value, str)
        return ipaddress.IPv6Network(value)


class IPv4InterfaceSerializer(TypeSerializer[ipaddress.IPv4Interface]):
    typename = "ipv4_interface"

    @staticmethod
    def to_jsonable(value: ipaddress.IPv4Interface) -> TJSONLike:
        return str(value)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> ipaddress.IPv4Interface:
        assert isinstance(value, str)
        return ipaddress.IPv4Interface(value)


class IPv6InterfaceSerializer(TypeSerializer[ipaddress.IPv6Interface]):
    typename = "ipv6_interface"

    @staticmethod
    def to_jsonable(value: ipaddress.IPv6Interface) -> TJSONLike:
        return str(value)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> ipaddress.IPv6Interface:
        assert isinstance(value, str)
        return ipaddress.IPv6Interface(value)


COMMON_SERIALIZERS: list[Type[TypeSerializer]] = [
    DateSerializer,
    DatetimeSerializer,
    TimeSerializer,
    TimedeltaSerializer,
    DecimalSerializer,
    UUIDSerializer,
    BytesSerializer,
    IPv4AddressSerializer,
    IPv6AddressSerializer,
    IPv4NetworkSerializer,
    IPv6NetworkSerializer,
    IPv4InterfaceSerializer,
    IPv6InterfaceSerializer,
]
assert len(set(cls.typename for cls in COMMON_SERIALIZERS)) == len(COMMON_SERIALIZERS), "uniqueness check"


class RedisDatalensDataJSONEncoder(json.JSONEncoder):
    JSONABLERS_MAP = {cls.typeobj(): cls for cls in COMMON_SERIALIZERS}

    def default(self, obj: Any) -> Any:
        typeobj = type(obj)
        preprocessor = self.JSONABLERS_MAP.get(typeobj)
        if preprocessor is not None:
            return dict(__dl_type__=preprocessor.typename, value=preprocessor.to_jsonable(obj))

        return super().default(obj)  # effectively, raises `TypeError`


class RedisDatalensDataJSONDecoder(json.JSONDecoder):
    DEJSONABLERS_MAP = {cls.typename: cls for cls in COMMON_SERIALIZERS}

    def __init__(self, *args, **kwargs) -> None:  # type: ignore  # TODO: fix
        super().__init__(*args, object_hook=self.object_hook, **kwargs)

    def object_hook(self, obj: dict[str, Any]) -> Any:
        # WARNING: this might collide with some unexpected objects that have a `__dl_type__` key.
        # A correct roundtrip way would be to wrap all objects with a `__dl_type__` key into another layer.
        dl_type = obj.get("__dl_type__")
        if dl_type is not None:
            postprocessor = self.DEJSONABLERS_MAP.get(dl_type)
            if postprocessor is not None:
                return postprocessor.from_jsonable(obj["value"])
        return obj


def common_dumps(value: TJSONExt, **kwargs: Any) -> bytes:
    return json.dumps(
        value,
        cls=RedisDatalensDataJSONEncoder,
        separators=(",", ":"),
        ensure_ascii=False,
        check_circular=False,  # dangerous but faster
        **kwargs,
    ).encode("utf-8")


def hashable_dumps(value: TJSONExt, sort_keys: bool = True, check_circular: bool = False, **kwargs: Any) -> str:
    return json.dumps(
        value,
        cls=RedisDatalensDataJSONEncoder,
        separators=(",", ":"),
        ensure_ascii=False,
        check_circular=check_circular,
        sort_keys=sort_keys,
        **kwargs,
    )


def common_loads(value: Union[bytes, str], **kwargs: Any) -> TJSONExt:
    return json.loads(
        value,
        cls=RedisDatalensDataJSONDecoder,
        **kwargs,
    )


class CacheMetadataSerialization:
    r"""
    Serialization/deserialization of metadata+data pair.
    Avoids touching/copying of most of the data
    (e.g. if the metadata shows it is not necessary).

    Similar solutions: msgpack streaming; protobufs.

    >>> metadata = dict(success=True)
    >>> data = b'\nzxc'
    >>> full_data = CacheMetadataSerialization.serialize(metadata, data)
    >>> ser_obj = CacheMetadataSerialization(full_data)
    >>> assert ser_obj.metadata == metadata
    >>> assert ser_obj.data_bytes == data
    """

    _sep = b"\n"

    @classmethod
    def serialize(cls, metadata: TJSONExt, data_bytes: bytes) -> bytes:
        metadata_bytes = common_dumps(metadata)
        assert cls._sep not in metadata_bytes
        return metadata_bytes + cls._sep + data_bytes

    def __init__(self, full_data: bytes):
        self.full_data = full_data
        sep_pos = full_data.index(self._sep)
        self.sep_pos = sep_pos
        self.metadata: TJSONExt = common_loads(full_data[:sep_pos])

    @property
    def data_bytes(self) -> bytes:
        return self.full_data[self.sep_pos + len(self._sep) :]
