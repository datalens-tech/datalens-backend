from __future__ import annotations

import abc
import logging
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Optional,
    Tuple,
)

from sqlalchemy.sql.type_api import TypeEngine

from dl_constants.enums import ConnectionType
from dl_core.connection_executors.models.db_adapter_data import ExecutionStepCursorInfo
from dl_type_transformer.native_type import (
    CommonNativeType,
    SATypeSpec,
)


LOGGER = logging.getLogger(__name__)


class SAColumnTypeNormalizer:
    def normalize_sa_col_type(self, sa_col_type: TypeEngine) -> TypeEngine:
        return sa_col_type


class SATypeTransformer(SAColumnTypeNormalizer):
    _type_code_to_sa: Optional[Dict[Any, SATypeSpec]] = None
    conn_type: ClassVar[ConnectionType]

    @staticmethod
    def _cursor_type_to_str(value: Any) -> str:
        if isinstance(value, type):
            return value.__name__.lower()
        if isinstance(value, int):
            return value  # type: ignore  # TODO: fix
        return str(value).lower()

    def _cursor_column_to_name(self, cursor_col: Tuple[Any, ...], dialect: Any = None) -> str:
        return cursor_col[0]

    def _cursor_column_to_sa(self, cursor_col: Tuple[Any, ...], require: bool = True) -> Optional[SATypeSpec]:
        type_code_to_sa = self._type_code_to_sa
        if not type_code_to_sa:
            if not require:
                return None
            raise Exception(f"No defined `self._type_code_to_sa` on {self.__class__!r}")

        type_code = cursor_col[1]
        sa_type = type_code_to_sa.get(type_code)
        return sa_type

    def _cursor_column_to_nullable(self, cursor_col: Tuple[Any, ...]) -> Optional[bool]:
        # No known `nullable=False` cases for subselects in PG and MySQL and
        # Oracle. But that might change.
        return True

    def _cursor_column_to_native_type(
        self, cursor_col: Tuple[Any, ...], require: bool = True
    ) -> Optional[CommonNativeType]:
        sa_type = self._cursor_column_to_sa(cursor_col, require=require)
        if sa_type is None:
            if not require:
                return None
            raise ValueError(f"Unknown/unsupported type_code for {self}, column info {cursor_col}", self, cursor_col)

        nullable = self._cursor_column_to_nullable(cursor_col)
        if nullable is None:
            nullable = True

        return CommonNativeType.normalize_name_and_create(
            name=self.normalize_sa_col_type(sa_type),  # type: ignore  # TODO: fix
            # no idea whether it can be False here; `create view` version
            # returns `nullable=True` for all known cases (for PG and MySQL).
            nullable=nullable,
        )


class WithCursorInfo:
    def _make_cursor_info(self, cursor, db_session=None) -> dict:  # type: ignore  # TODO: fix
        return {}


class WithMinimalCursorInfo(WithCursorInfo, SATypeTransformer):
    def _make_cursor_info(self, cursor, db_session=None) -> dict:  # type: ignore  # TODO: fix
        """
        Minimal information from the cursor,
        will likely get processed through JSON at some point,
        therefore should only contain jsonable primitives.
        """
        return dict(
            super()._make_cursor_info(cursor, db_session),
            names=[str(cursor_col[0]) for cursor_col in cursor.description],
            driver_types=[self._cursor_type_to_str(cursor_col[1]) for cursor_col in cursor.description],
            db_types=[
                self._cursor_column_to_native_type(cursor_col, require=False) for cursor_col in cursor.description
            ],
        )


class WithDatabaseNameOverride:
    warn_on_default_db_name_override: ClassVar[bool] = True

    @abc.abstractmethod
    def get_default_db_name(self) -> Optional[str]:
        pass

    def get_db_name_for_query(self, db_name_from_query: Optional[str]) -> str:
        return self._get_db_name_for_query(
            default=self.get_default_db_name(),
            from_query=db_name_from_query,
            warn_override=self.warn_on_default_db_name_override,
        )

    @staticmethod
    def _get_db_name_for_query(default: Optional[str], from_query: Optional[str], warn_override: bool) -> str:
        if default is not None and from_query is not None:
            if default != from_query and warn_override:
                LOGGER.warning(f"Divergence in DB names: default='{default}' from_query='{from_query}'")
            return from_query
        elif default is not None:
            return default
        elif from_query is not None:
            return from_query
        else:
            raise ValueError("Can not determine DB name for engine construction. Both are None")


class WithNoneRowConverters:
    def _get_row_converters(self, cursor_info: ExecutionStepCursorInfo) -> Tuple[Optional[Callable[[Any], Any]], ...]:
        return tuple(None for _ in cursor_info.raw_cursor_description)
