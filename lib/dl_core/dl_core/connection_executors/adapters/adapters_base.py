from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Generator,
    Iterable,
    Optional,
    Sequence,
    TypeVar,
)

import attr
from typing_extensions import final

from dl_core.connection_executors.adapters.adapter_actions.sync_base import (
    SyncDBVersionAdapterAction,
    SyncDBVersionAdapterActionNotImplemented,
    SyncSchemaNamesAdapterAction,
    SyncSchemaNamesAdapterActionNotImplemented,
    SyncTableExistsActionNotImplemented,
    SyncTableExistsAdapterAction,
    SyncTableInfoAdapterAction,
    SyncTableInfoAdapterActionNotImplemented,
    SyncTableNamesAdapterAction,
    SyncTableNamesAdapterActionNotImplemented,
    SyncTestAdapterAction,
    SyncTestAdapterActionNotImplemented,
    SyncTypedQueryAdapterAction,
    SyncTypedQueryAdapterActionNotImplemented,
)
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
    RawSchemaInfo,
)
from dl_core.connection_models import (
    DBIdent,
    SchemaIdent,
    TableDefinition,
    TableIdent,
)


if TYPE_CHECKING:
    from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO  # noqa: F401
    from dl_dashsql.typed_query.primitives import (
        TypedQuery,
        TypedQueryResult,
    )


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class DBAdapterQueryResult:
    cursor_info: dict = attr.ib()
    data_chunks: Iterable[Sequence] = attr.ib()
    # Notable difference from `cursor_info`: raw value is not meant to be serialized.
    raw_cursor_info: Optional[ExecutionStepCursorInfo] = attr.ib(default=None)

    def get_all(self) -> list[Sequence]:
        """
        :return: All fetched data
        """
        result: list[Sequence] = []
        for chunk in self.data_chunks:
            result.extend(chunk)

        return result


_TARGET_DTO_TV = TypeVar("_TARGET_DTO_TV", bound="ConnTargetDTO")


@attr.s
class SyncDirectDBAdapter(CommonBaseDirectAdapter[_TARGET_DTO_TV], metaclass=abc.ABCMeta):
    # Adapter action fields
    _sync_db_version_action: SyncDBVersionAdapterAction = attr.ib(init=False)
    _sync_schema_names_action: SyncSchemaNamesAdapterAction = attr.ib(init=False)
    _sync_table_names_action: SyncTableNamesAdapterAction = attr.ib(init=False)
    _sync_test_action: SyncTestAdapterAction = attr.ib(init=False)
    _sync_table_info_action: SyncTableInfoAdapterAction = attr.ib(init=False)
    _sync_table_exists_action: SyncTableExistsAdapterAction = attr.ib(init=False)
    _sync_typed_query_action: SyncTypedQueryAdapterAction = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._initialize_actions()

    def _initialize_actions(self):
        self._sync_db_version_action = self._make_sync_db_version_action()
        self._sync_schema_names_action = self._make_sync_schema_names_action()
        self._sync_table_names_action = self._make_sync_table_names_action()
        self._sync_test_action = self._make_sync_test_action()
        self._sync_table_info_action = self._make_sync_table_info_action()
        self._sync_table_exists_action = self._make_sync_table_exists_action()
        self._sync_typed_query_action = self._make_sync_typed_query_action()

    # Action factory methods

    def _make_sync_db_version_action(self) -> SyncDBVersionAdapterAction:
        # Redefine this method to enable `get_db_version`
        return SyncDBVersionAdapterActionNotImplemented()

    def _make_sync_schema_names_action(self) -> SyncSchemaNamesAdapterAction:
        # Redefine this method to enable `get_schema_names`
        return SyncSchemaNamesAdapterActionNotImplemented()

    def _make_sync_table_names_action(self) -> SyncTableNamesAdapterAction:
        # Redefine this method to enable `get_table_names`
        return SyncTableNamesAdapterActionNotImplemented()

    def _make_sync_test_action(self) -> SyncTestAdapterAction:
        # Redefine this method to enable `test`
        return SyncTestAdapterActionNotImplemented()

    def _make_sync_table_info_action(self) -> SyncTableInfoAdapterAction:
        # Redefine this method to enable `get_table_info`
        return SyncTableInfoAdapterActionNotImplemented()

    def _make_sync_table_exists_action(self) -> SyncTableExistsAdapterAction:
        # Redefine this method to enable `is_table_exists`
        return SyncTableExistsActionNotImplemented()

    def _make_sync_typed_query_action(self) -> SyncTypedQueryAdapterAction:
        # Redefine this method to enable `execute_typed_query`
        return SyncTypedQueryAdapterActionNotImplemented()

    @abc.abstractmethod
    def execute_by_steps(self, db_adapter_query: DBAdapterQuery) -> Generator[ExecutionStep, None, None]:
        pass

    @abc.abstractmethod
    def test(self) -> None:
        pass

    def execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        return self._sync_typed_query_action.run_typed_query_action(typed_query=typed_query)

    # TODO CONSIDER: Make version with simplified interface execute(query: str, db_name: Optional[str] = None)
    @final
    def execute(self, query: DBAdapterQuery) -> DBAdapterQueryResult:
        steps_generator = self.execute_by_steps(query)

        cursor_info_step = next(steps_generator)
        if not isinstance(cursor_info_step, ExecutionStepCursorInfo):
            raise RuntimeError(f"Unexpected type of first step from database: {cursor_info_step}")

        def real_generator() -> Generator[Sequence[Sequence], None, None]:
            for step in steps_generator:
                if not isinstance(step, ExecutionStepDataChunk):
                    raise RuntimeError(f"Unexpected type of non-first step from database: {step}")
                yield step.chunk

        return DBAdapterQueryResult(
            cursor_info=cursor_info_step.cursor_info,
            data_chunks=real_generator(),
            raw_cursor_info=cursor_info_step,
        )

    def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return self._sync_db_version_action.run_db_version_action(db_ident=db_ident)

    def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        return self._sync_schema_names_action.run_schema_names_action(db_ident=db_ident)

    def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        return self._sync_table_names_action.run_table_names_action(schema_ident=schema_ident)

    def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        return self._sync_table_info_action.run_table_info_action(
            table_def=table_def,
            fetch_idx_info=fetch_idx_info,
        )

    def is_table_exists(self, table_ident: TableIdent) -> bool:
        return self._sync_table_exists_action.run_table_exists_action(table_ident=table_ident)

    @abc.abstractmethod
    def close(self) -> None:
        pass
