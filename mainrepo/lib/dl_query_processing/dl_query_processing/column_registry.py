from __future__ import annotations

from itertools import chain
import logging
from typing import (
    Callable,
    Collection,
    Dict,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

from dl_core.components.ids import (
    AvatarId,
    SourceId,
)
from dl_core.db.elements import SchemaColumn
from dl_core.utils import make_id
from dl_formula.core.datatype import DataType
import dl_formula.core.exc as formula_exc
import dl_formula.core.nodes as formula_nodes
from dl_formula.inspect.expression import used_fields
from dl_query_processing.compilation.type_mapping import BI_TO_FORMULA_TYPES

LOGGER = logging.getLogger(__name__)


ColumnId = str


class AvatarColumn(NamedTuple):
    id: ColumnId
    avatar_id: AvatarId
    column: SchemaColumn


class ColumnRegistry:
    def __init__(
        self,
        db_columns: Optional[Collection[SchemaColumn]] = None,
        avatar_source_map: Optional[Dict[AvatarId, SourceId]] = None,
    ):
        self._db_columns = list(db_columns) if db_columns is not None else []
        self._avatar_source_map = avatar_source_map.copy() if avatar_source_map is not None else {}

        self._column_ids: Dict[Tuple[AvatarId, str], ColumnId] = {}
        self._columns: Dict[ColumnId, AvatarColumn] = {}

        for avatar_id, source_id in self._avatar_source_map.items():
            self.register_avatar(avatar_id=avatar_id, source_id=source_id)

    def __contains__(self, item: str) -> bool:
        return item in self._columns

    def __add__(self, other: ColumnRegistry) -> ColumnRegistry:
        if other is self:
            return self

        if not self._db_columns and not self._avatar_source_map:
            return other
        if not other._db_columns and not other._avatar_source_map:
            return self

        result = ColumnRegistry(
            avatar_source_map={**self._avatar_source_map, **other._avatar_source_map},
        )
        for column_id, avatar_column in chain(self._columns.items(), other._columns.items()):
            result.register_column(
                column_id=column_id, db_column=avatar_column.column, avatar_id=avatar_column.avatar_id
            )
        return result

    def register_avatar(self, avatar_id: AvatarId, source_id: SourceId) -> None:
        self._avatar_source_map[avatar_id] = source_id
        for col in self._db_columns:
            if col.source_id == source_id:
                column_id = make_id()
                self._column_ids[(avatar_id, col.name)] = column_id
                self._columns[column_id] = AvatarColumn(
                    id=column_id,
                    avatar_id=avatar_id,
                    column=col,
                )

    def register_column(self, column_id: ColumnId, avatar_id: AvatarId, db_column: SchemaColumn) -> None:
        if db_column not in self._db_columns:
            self._db_columns.append(db_column)
        avatar_column = AvatarColumn(
            id=column_id,
            avatar_id=avatar_id,
            column=db_column,
        )
        self._column_ids[(avatar_id, db_column.name)] = column_id
        self._columns[column_id] = avatar_column

    def unregister_avatar(self, avatar_id: AvatarId) -> None:
        source_id = self._avatar_source_map[avatar_id]
        for col in self._db_columns:
            if col.source_id == source_id:
                column_id = self._column_ids[(avatar_id, col.name)]
                del self._columns[column_id]
                del self._column_ids[(avatar_id, col.name)]
        del self._avatar_source_map[avatar_id]

    def get_used_avatar_ids_for_formula_obj(self, formula_obj: formula_nodes.Formula) -> Set[str]:
        column_ids = {field_node.name for field_node in used_fields(formula_obj)}
        return {self._columns.get(column_id).avatar_id for column_id in column_ids}  # type: ignore  # TODO: fix

    def get_multipart_column_names(self, avatar_alias_mapper: Callable[[AvatarId], str]) -> Dict[str, Tuple[str, str]]:
        return {
            column_id: (avatar_alias_mapper(av_column.avatar_id), av_column.column.name)
            for column_id, av_column in self._columns.items()
        }

    def get_column_formula_types(self) -> Dict[str, DataType]:
        return {
            column_id: BI_TO_FORMULA_TYPES[av_column.column.user_type] for column_id, av_column in self._columns.items()
        }

    def get(self, column_id: str) -> AvatarColumn:
        return self._columns.get(column_id)  # type: ignore  # TODO: fix

    def get_avatar_column(self, avatar_id: str, name: str) -> AvatarColumn:
        try:
            column_id = self._column_ids[(avatar_id, name)]
            return self._columns[column_id]
        except KeyError:
            raise formula_exc.FormulaError(
                f"Unknown referenced source column: {name}",
                code=formula_exc.UnknownSourceColumnError.default_code,
            )

    def get_column_ids(self) -> Dict[Tuple[str, str], str]:
        return {
            (av_column.avatar_id, av_column.column.name): column_id for column_id, av_column in self._columns.items()
        }

    def get_column_avatar_ids(self) -> Dict[Tuple[str, str], str]:
        return {
            column_id: av_column.avatar_id  # type: ignore  # TODO: fix
            for column_id, av_column in self._columns.items()
        }

    def get_columns_for_avatar(self, avatar_id: str) -> list[AvatarColumn]:
        return sorted(
            [av_column for av_column in self._columns.values() if av_column.avatar_id == avatar_id],
            key=lambda col: col.id,
        )
