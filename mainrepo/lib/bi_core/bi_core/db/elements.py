from __future__ import annotations

from collections import namedtuple
from typing import (
    Any,
    FrozenSet,
    List,
    Optional,
    Sequence,
    Union,
)

import attr

from bi_constants.enums import (
    BIType,
    IndexKind,
)
from bi_core.db.native_type import GenericNativeType
from bi_utils.utils import get_type_full_name

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
        title: Optional[str] = None,
        user_type: Optional[Union[BIType, str]] = None,
        nullable: Optional[bool] = True,
        native_type: Optional[GenericNativeType] = None,
        source_id: Any = None,
        has_auto_aggregation: Optional[bool] = None,
        lock_aggregation: bool = False,
        description: Optional[str] = None,
    ):
        title = title or name
        if isinstance(user_type, str):
            user_type = BIType[user_type]
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
    kind: Optional[IndexKind] = attr.ib()


@attr.s(slots=True)
class SchemaInfo:
    # TODO FIX: May be make a Tuple/Sequence instead of list
    schema: List[SchemaColumn] = attr.ib()
    skipped: List[SchemaColumn] = attr.ib(default=None)
    converted: List[SchemaColumn] = attr.ib(default=None)

    # None value should be treated as "unknown/no info"
    indexes: Optional[FrozenSet[IndexInfo]] = attr.ib(default=None)

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
