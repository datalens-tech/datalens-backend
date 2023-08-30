from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING, Generator, Iterable, List, Optional, Sequence, TypeVar

import attr
from typing_extensions import final

from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from bi_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
    RawSchemaInfo,
)
from bi_core.connection_models import TableIdent, SchemaIdent, DBIdent, TableDefinition

if TYPE_CHECKING:
    from bi_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO  # noqa: F401


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class DBAdapterQueryResult:
    cursor_info: dict = attr.ib()
    data_chunks: Iterable[Sequence] = attr.ib()
    # Notable difference from `cursor_info`: raw value is not meant to be serialized.
    raw_cursor_info: Optional[ExecutionStepCursorInfo] = attr.ib(default=None)

    def get_all(self) -> Sequence[Sequence]:
        """
        :return: All fetched data
        """
        result: List[Sequence] = []
        for chunk in self.data_chunks:
            result.extend(chunk)

        return result


_TARGET_DTO_TV = TypeVar("_TARGET_DTO_TV", bound='ConnTargetDTO')


class SyncDirectDBAdapter(CommonBaseDirectAdapter[_TARGET_DTO_TV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def execute_by_steps(self, db_adapter_query: DBAdapterQuery) -> Generator[ExecutionStep, None, None]:
        pass

    @abc.abstractmethod
    def test(self) -> None:
        pass

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

    @abc.abstractmethod
    def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        pass

    @abc.abstractmethod
    def get_schema_names(self, db_ident: DBIdent) -> List[str]:
        pass

    @abc.abstractmethod
    def get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        pass

    @abc.abstractmethod
    def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        pass

    @abc.abstractmethod
    def is_table_exists(self, table_ident: TableIdent) -> bool:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass
