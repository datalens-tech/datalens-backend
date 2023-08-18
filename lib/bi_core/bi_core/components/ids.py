from __future__ import annotations

from typing import Any, Dict, Optional, Set, Type, TYPE_CHECKING

import abc
import attr
import random
import string
import uuid

from enum import Enum, unique
from anyascii import anyascii

if TYPE_CHECKING:
    from bi_core.us_dataset import Dataset


ID_LENGTH = 36
ID_VALID_SYMBOLS = string.ascii_lowercase + string.digits + '_-'


FieldId = str
SourceId = str
AvatarId = str
RelationId = str


def generate_random_str(length: int = 4) -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def make_field_id() -> FieldId:
    return str(uuid.uuid4())


def make_avatar_id() -> AvatarId:
    return str(uuid.uuid4())


def make_source_id() -> SourceId:
    return str(uuid.uuid4())


def resolve_id_collisions(item: str, existing_items: Set[str], formatter: str = '{} ({})') -> str:
    idx = 1
    orig_item = item
    while item in existing_items:
        item = formatter.format(orig_item, idx)
        idx += 1
    return item


@attr.s
class FieldIdValidator:
    _id_valid_symbols: str = ID_VALID_SYMBOLS
    _id_length: int = ID_LENGTH

    def is_valid(self, field_id: FieldId) -> bool:
        if not len(field_id) or len(field_id) > self._id_length:
            return False
        if any((symbol not in self._id_valid_symbols for symbol in field_id)):
            return False
        return True


@attr.s
class FieldIdGenerator(metaclass=abc.ABCMeta):
    dataset: Dataset = attr.ib(kw_only=True)

    @abc.abstractmethod
    def make_field_id(self, *args: Any, **kwargs: Any) -> FieldId:
        raise NotImplementedError()


@attr.s
class DefaultFieldIdGenerator(FieldIdGenerator):
    def make_field_id(self, *args: Any, **kwargs: Any) -> FieldId:
        return str(uuid.uuid4())


def make_readable_field_id(title: str, valid_symbols: str, max_length: int) -> str:
    field_id = ''.join(
        symbol
        for symbol in '_'.join(anyascii(title).lower().split())
        if symbol in valid_symbols
    )
    field_id = field_id[:max_length]
    return field_id


@attr.s
class ReadableFieldIdGenerator(FieldIdGenerator):
    _id_valid_symbols: str = ID_VALID_SYMBOLS
    _id_length: int = ID_LENGTH
    _id_formatter: str = '{}_{}'

    def make_field_id(self, *args: Any, title: Optional[str] = None, **kwargs: Any) -> FieldId:
        if title is None:
            return str(uuid.uuid4())
        field_id = make_readable_field_id(title, self._id_valid_symbols, self._id_length)
        field_id = self._resolve_id_collisions(field_id)
        return field_id

    def _resolve_id_collisions(self, item: FieldId) -> FieldId:
        existing_items = {f.guid for f in self.dataset.result_schema.fields}
        idx = 1
        orig_item = item
        while item in existing_items:
            item = self._id_formatter.format(orig_item, idx)
            idx += 1
        return item


@attr.s
class ReadableFieldIdGeneratorWithPrefix(ReadableFieldIdGenerator):
    def make_field_id(self, *args: Any, title: Optional[str] = None, **kwargs: Any) -> FieldId:
        field_id = super().make_field_id(title=title)
        return '_'.join([generate_random_str(), field_id])


@attr.s
class ReadableFieldIdGeneratorWithSuffix(ReadableFieldIdGenerator):
    def make_field_id(self, *args: Any, title: Optional[str] = None, **kwargs: Any) -> FieldId:
        field_id = super().make_field_id(title=title)
        return '_'.join([field_id, generate_random_str()])


@unique
class FieldIdGeneratorType(Enum):
    default = 'default'
    readable = 'readable'
    prefix = 'prefix'
    suffix = 'suffix'


DEFAULT_FIELD_ID_GENERATOR_TYPE: FieldIdGeneratorType = FieldIdGeneratorType.readable


FIELD_ID_GENERATOR_MAP: Dict[FieldIdGeneratorType, Type[FieldIdGenerator]] = {
    FieldIdGeneratorType.default: DefaultFieldIdGenerator,
    FieldIdGeneratorType.readable: ReadableFieldIdGenerator,
    FieldIdGeneratorType.prefix: ReadableFieldIdGeneratorWithPrefix,
    FieldIdGeneratorType.suffix: ReadableFieldIdGeneratorWithSuffix,
}
