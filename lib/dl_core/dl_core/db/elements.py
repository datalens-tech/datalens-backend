from __future__ import annotations

from collections import namedtuple
from typing import (
    Any,
    Sequence,
)

import attr

from dl_constants.enums import (
    IndexKind,
    UserDataType,
)
from dl_type_transformer.native_type import GenericNativeType
from dl_utils.utils import get_type_full_name


_SchemaColumn = namedtuple(
    "_SchemaColumn",
    (
        "name",
        "title",
        "user_type",
        "nullable",  # Deprecated in favor of `native_type.nullable`
        "native_type",
        "source_id",
        "has_auto_aggregation",
        "lock_aggregation",
        "description",
    ),
)


class SchemaColumn(_SchemaColumn):
    def __new__(  # type: ignore  # TODO: fix
        cls,
        name: str,
        title: str | None = None,
        user_type: UserDataType | str | None = None,
        nullable: bool | None = True,
        native_type: GenericNativeType | None = None,
        source_id: Any = None,
        has_auto_aggregation: bool | None = None,
        lock_aggregation: bool = False,
        description: str | None = None,
    ):
        title = title or name
        if isinstance(user_type, str):
            user_type = UserDataType[user_type]
        has_auto_aggregation = has_auto_aggregation if has_auto_aggregation is not None else False
        return super().__new__(
            cls,
            name=name,
            title=title,
            user_type=user_type,
            nullable=nullable,
            native_type=native_type,
            source_id=source_id,
            has_auto_aggregation=has_auto_aggregation,
            lock_aggregation=lock_aggregation,
            description=description or "",
        )

    def clone(self, **kwargs) -> "SchemaColumn":  # type: ignore  # TODO: fix
        return SchemaColumn(**dict(self._asdict(), **kwargs))


@attr.s(slots=True, frozen=True)
class IndexInfo:
    columns: Sequence[str] = attr.ib()
    kind: IndexKind | None = attr.ib()


@attr.s(slots=True)
class SchemaInfo:
    # TODO FIX: May be make a Tuple/Sequence instead of list
    schema: list[SchemaColumn] = attr.ib()
    skipped: list[SchemaColumn] = attr.ib(default=None)
    converted: list[SchemaColumn] = attr.ib(default=None)

    # None value should be treated as "unknown/no info"
    indexes: frozenset[IndexInfo] | None = attr.ib(default=None)

    @indexes.validator
    def check_indexes(self, attribute: Any, value: Any) -> None:
        if value is None or isinstance(value, frozenset):
            pass
        else:
            raise TypeError(f"Indexes must be a frozen set or non, not {get_type_full_name(type(value))!r}")

    def clone(self, **kwargs) -> SchemaInfo:  # type: ignore  # TODO: fix
        return attr.evolve(self, **kwargs)

    @classmethod
    def from_schema(cls, schema: Sequence[SchemaColumn]) -> SchemaInfo:
        # TODO FIX: May be make a Tuple/Sequence instead of list
        return SchemaInfo(schema=list(schema))
