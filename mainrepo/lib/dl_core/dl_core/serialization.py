"""
RDL JSON - Redis DataLens JSON

Serialization with support for custom objects like ``date`` & ``datetime``.

>>> data = dict(some_dt=datetime.datetime(2019, 6, 17, 7, 1, 41, 79585, tzinfo=datetime.timezone(datetime.timedelta(seconds=-1320))))
>>> dumped = json.dumps(data, cls=RedisDatalensDataJSONEncoder)
>>> json.loads(dumped)['some_dt']
{'__dl_type__': 'datetime', 'value': [[2019, 6, 17, 7, 1, 41, 79585], -1320.0]}
>>> json.loads(dumped, cls=RedisDatalensDataJSONDecoder)['some_dt']
datetime.datetime(2019, 6, 17, 7, 1, 41, 79585, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=85080)))
>>> # ^ negative timedeltas' repr is actually this weird.
>>> tricky_data = dict(normal=data, abnormal=json.loads(dumped))
>>> tricky_data_dumped = json.dumps(tricky_data, cls=RedisDatalensDataJSONEncoder)
>>> tricky_roundtrip = json.loads(tricky_data_dumped, cls=RedisDatalensDataJSONDecoder)
>>> # WARNING: not currently viable: `assert tricky_roundtrip == tricky_data, tricky_roundtrip`
"""

from __future__ import annotations

import abc
import datetime
import decimal
import json
from typing import (
    Any,
    ClassVar,
    Generic,
    Type,
    TypeVar,
    Union,
)
import uuid

from dl_constants.types import (
    TJSONExt,
    TJSONLike,
)


_TS_TV = TypeVar("_TS_TV")


class TypeSerializer(Generic[_TS_TV]):
    typeobj: ClassVar[Type[_TS_TV]]  # type: ignore  # TODO: fix
    typename: ClassVar[str]

    @staticmethod
    @abc.abstractmethod
    def to_jsonable(value: _TS_TV) -> TJSONLike:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def from_jsonable(value: TJSONLike) -> _TS_TV:
        raise NotImplementedError


class DateSerializer(TypeSerializer[datetime.date]):
    typeobj = datetime.date
    typename = "date"

    @staticmethod
    def to_jsonable(obj: datetime.date) -> TJSONLike:
        return obj.isoformat()

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.date:
        assert isinstance(value, str)
        return datetime.date.fromisoformat(value)


class DatetimeSerializer(TypeSerializer[datetime.datetime]):
    typeobj = datetime.datetime
    typename = "datetime"

    @staticmethod
    def to_jsonable(obj: datetime.datetime) -> TJSONLike:
        tzinfo = (
            obj.tzinfo.utcoffset(obj).total_seconds() if obj.tzinfo is not None else None  # type: ignore  # TODO: fix
        )
        return (
            [obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second, obj.microsecond],
            tzinfo,
        )

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.datetime:
        assert isinstance(value, (list, tuple))
        assert len(value) == 2
        parts, offset_sec = value
        assert len(parts) == 7  # type: ignore  # TODO: fix
        tzinfo = (
            datetime.timezone(datetime.timedelta(seconds=offset_sec))  # type: ignore  # TODO: fix
            if offset_sec is not None
            else None
        )
        return datetime.datetime(*parts, tzinfo=tzinfo)  # type: ignore  # TODO: fix


class TimeSerializer(TypeSerializer[datetime.time]):
    typeobj = datetime.time
    typename = "time"

    @staticmethod
    def to_jsonable(obj: datetime.time) -> TJSONLike:
        # Not expecting this to be valid for non-constant time tzinfos.
        tzinfo = (
            obj.tzinfo.utcoffset(datetime.datetime(1970, 1, 1)).total_seconds()  # type: ignore  # TODO: fix
            if obj.tzinfo is not None
            else None
        )
        return ([obj.hour, obj.minute, obj.second, obj.microsecond], tzinfo)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.time:
        assert isinstance(value, (list, tuple))
        assert len(value) == 2, value
        parts, offset_sec = value
        assert len(parts) == 4, parts  # type: ignore  # TODO: fix
        assert isinstance(offset_sec, (int, float)), offset_sec
        tzinfo = datetime.timezone(datetime.timedelta(seconds=offset_sec)) if offset_sec is not None else None
        return datetime.time(*parts, tzinfo=tzinfo)  # type: ignore  # TODO: fix


class TimedeltaSerializer(TypeSerializer[datetime.timedelta]):
    typeobj = datetime.timedelta
    typename = "timedelta"

    @staticmethod
    def to_jsonable(obj: datetime.timedelta) -> TJSONLike:
        return obj.total_seconds()

    @staticmethod
    def from_jsonable(value: TJSONLike) -> datetime.timedelta:
        assert isinstance(value, float)
        return datetime.timedelta(seconds=value)


class DecimalSerializer(TypeSerializer[decimal.Decimal]):
    typeobj = decimal.Decimal
    typename = "decimal"

    @staticmethod
    def to_jsonable(obj: decimal.Decimal) -> TJSONLike:
        return str(obj)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> decimal.Decimal:
        assert isinstance(value, str)
        return decimal.Decimal(value)


class UUIDSerializer(TypeSerializer[uuid.UUID]):
    typeobj = uuid.UUID
    typename = "uuid"

    @staticmethod
    def to_jsonable(obj: uuid.UUID) -> TJSONLike:
        return str(obj)

    @staticmethod
    def from_jsonable(value: TJSONLike) -> uuid.UUID:
        assert isinstance(value, str)
        return uuid.UUID(value)


COMMON_SERIALIZERS = [
    DateSerializer,
    DatetimeSerializer,
    TimeSerializer,
    TimedeltaSerializer,
    DecimalSerializer,
    UUIDSerializer,
]
assert len(set(cls.typename for cls in COMMON_SERIALIZERS)) == len(COMMON_SERIALIZERS), "uniqueness check"


class RedisDatalensDataJSONEncoder(json.JSONEncoder):
    JSONABLERS_MAP = {cls.typeobj: cls for cls in COMMON_SERIALIZERS}

    def default(self, obj):  # type: ignore  # TODO: fix
        typeobj = type(obj)
        preprocessor = self.JSONABLERS_MAP.get(typeobj)
        if preprocessor is not None:
            return dict(__dl_type__=preprocessor.typename, value=preprocessor.to_jsonable(obj))

        return super().default(obj)  # effectively, raises `TypeError`


class RedisDatalensDataJSONDecoder(json.JSONDecoder):
    DEJSONABLERS_MAP = {cls.typename: cls for cls in COMMON_SERIALIZERS}
    # Transition
    COMPAT_CONVERTERS_MAP = {
        "date": lambda o: datetime.date(*[int(p) for p in o["value"].split("-")]),
        "datetime": lambda o: datetime.datetime(  # type: ignore  # TODO: fix
            *o["parts"],
            tzinfo=None if o["offset_sec"] is None else datetime.timezone(datetime.timedelta(seconds=o["offset_sec"])),
        ),
    }

    def __init__(self, *args, **kwargs):  # type: ignore  # TODO: fix
        super().__init__(*args, object_hook=self.object_hook, **kwargs)

    def object_hook(self, obj):  # type: ignore  # TODO: fix
        # WARNING: this might collide with some unexpected objects that have a `__dl_type__` key.
        # A correct roundtrip way would be to wrap all objects with a `__dl_type__` key into another layer.
        dl_type = obj.get("__dl_type__")
        if dl_type is not None:
            postprocessor = self.DEJSONABLERS_MAP.get(dl_type)
            if postprocessor is not None:
                return postprocessor.from_jsonable(obj["value"])

        # Transition
        compat_dl_type = obj.get("dl_type")
        if compat_dl_type is not None:
            compat_converter = self.COMPAT_CONVERTERS_MAP.get(compat_dl_type)
            if compat_converter is not None:
                return compat_converter(obj)

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
    >>> assert set_obj.data_bytes == data
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
