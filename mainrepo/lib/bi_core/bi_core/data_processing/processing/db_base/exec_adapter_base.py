from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Sequence,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.selectable import Select

from bi_constants.types import TBIDataValue
from bi_core.data_processing.types import TValuesChunkStream
from bi_core.utils import make_id

if TYPE_CHECKING:
    from bi_constants.enums import BIType
    from bi_core.data_processing.cache.primitives import LocalKeyRepresentation
    from bi_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo


LOGGER = logging.getLogger(__name__)
DEFAULT_CHUNK_SIZE = 1000


@attr.s
class ProcessorDbExecAdapterBase(abc.ABC):
    _default_chunk_size: ClassVar[int] = 1000
    _log: ClassVar[logging.Logger] = LOGGER.getChild("ProcessorDbExecAdapterBase")

    @abc.abstractmethod
    async def _execute_and_fetch(
        self,
        *,
        query: Union[Select, str],
        user_types: Sequence[BIType],
        chunk_size: int,
        joint_dsrc_info: Optional[PreparedMultiFromInfo] = None,
        query_id: str,
    ) -> TValuesChunkStream:
        """
        Execute SELECT statement.
        Return data as ``AsyncChunked``.
        """
        raise NotImplementedError

    async def scalar(self, query: Union[str, Select], user_type: BIType) -> TBIDataValue:
        """Execute a statement returning a scalar value."""
        data_stream = await self._execute_and_fetch(
            query_id=make_id(),
            query=query,
            user_types=[user_type],
            chunk_size=5,
        )
        data = await data_stream.all()
        assert len(data) == 1, f"Expected 1 entry, got {len(data)}"
        assert len(data[0]) == 1, f"Expected 1 column, got {len(data[0])}"
        return data[0][0]

    async def fetch_data_from_select(
        self,
        *,
        query: Union[str, sa.sql.selectable.Select],
        user_types: Sequence[BIType],
        chunk_size: Optional[int] = None,
        joint_dsrc_info: Optional[PreparedMultiFromInfo] = None,
        query_id: str,
    ) -> TValuesChunkStream:
        """Fetch data from a table"""

        chunk_size = chunk_size or self._default_chunk_size
        self._log.info(f"Fetching data from query {query_id}")

        return await self._execute_and_fetch(
            query=query,
            user_types=user_types,
            chunk_size=chunk_size,
            joint_dsrc_info=joint_dsrc_info,
            query_id=query_id,
        )

    def get_data_key(
        self,
        *,
        query_id: str,
        query: Union[str, Select],
        user_types: Sequence[BIType],
        joint_dsrc_info: Optional[PreparedMultiFromInfo] = None,
    ) -> Optional[LocalKeyRepresentation]:
        return None
