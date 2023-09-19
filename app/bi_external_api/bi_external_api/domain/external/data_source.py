import abc
import enum
from typing import (
    ClassVar,
    Generic,
    Optional,
    Sequence,
    TypeVar,
    final,
)

import attr

from bi_external_api.attrs_model_mapper import (
    AttribDescriptor,
    ModelDescriptor,
)
from bi_external_api.domain.utils import ensure_tuple
from bi_external_api.enums import ExtAPIType

from .model_tags import ExtModelTags


_PROCESSOR_RETURN_DATA_TV = TypeVar("_PROCESSOR_RETURN_DATA_TV")


class ExtDataSourceProcessor(Generic[_PROCESSOR_RETURN_DATA_TV], metaclass=abc.ABCMeta):
    @final
    def process(self, dsrc: "DataSource") -> _PROCESSOR_RETURN_DATA_TV:
        spec = dsrc.spec

        if isinstance(spec, TableDataSourceSpec):
            return self.process_table(spec)

        if isinstance(spec, SchematizedTableDataSourceSpec):
            return self.process_schematized_table(spec)

        if isinstance(spec, CHYTTableRangeDataSourceSpec):
            return self.process_chyt_range(spec)

        if isinstance(spec, CHYTTableListDataSourceSpec):
            return self.process_chyt_list(spec)

        if isinstance(spec, SubSelectDataSourceSpec):
            return self.process_sub_select(spec)

        raise NotImplementedError()

    @abc.abstractmethod
    def process_table(self, dsrc: "TableDataSourceSpec") -> _PROCESSOR_RETURN_DATA_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def process_schematized_table(self, dsrc: "TableDataSourceSpec") -> _PROCESSOR_RETURN_DATA_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def process_sub_select(self, dsrc: "SubSelectDataSourceSpec") -> _PROCESSOR_RETURN_DATA_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def process_chyt_range(self, dsrc: "CHYTTableRangeDataSourceSpec") -> _PROCESSOR_RETURN_DATA_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def process_chyt_list(self, dsrc: "CHYTTableListDataSourceSpec") -> _PROCESSOR_RETURN_DATA_TV:
        raise NotImplementedError()


class DataSourceKind(enum.Enum):
    sql_table = enum.auto()
    sql_table_in_schema = enum.auto()
    sql_query = enum.auto()
    chyt_table_list = enum.auto()
    chyt_table_range = enum.auto()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class DataSourceSpec:
    kind: ClassVar[DataSourceKind]


# TODO FIX: Nest datasource params
@ModelDescriptor()
@attr.s(frozen=True)
class DataSource:
    id: str = attr.ib()
    title: str = attr.ib()
    connection_ref: str = attr.ib(metadata=AttribDescriptor(tags=frozenset({ExtModelTags.connection_name})).to_meta())
    spec: DataSourceSpec = attr.ib()


@ModelDescriptor()
@attr.s
class TableDataSourceSpec(DataSourceSpec):
    kind = DataSourceKind.sql_table

    db_name: Optional[str] = attr.ib()
    table_name: str = attr.ib()


@ModelDescriptor()
@attr.s
class SchematizedTableDataSourceSpec(TableDataSourceSpec):
    kind = DataSourceKind.sql_table_in_schema

    schema_name: Optional[str] = attr.ib()


@ModelDescriptor()
@attr.s
class SubSelectDataSourceSpec(DataSourceSpec):
    kind = DataSourceKind.sql_query

    sql: str = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.CORE, ExtAPIType.YA_TEAM])
@attr.s
class CHYTTableListDataSourceSpec(DataSourceSpec):
    kind = DataSourceKind.chyt_table_list

    tables: Sequence[str] = attr.ib(converter=ensure_tuple)


@ModelDescriptor(api_types=[ExtAPIType.CORE, ExtAPIType.YA_TEAM])
@attr.s
class CHYTTableRangeDataSourceSpec(DataSourceSpec):
    kind = DataSourceKind.chyt_table_range

    path: str = attr.ib()
    start: str = attr.ib()
    end: str = attr.ib()
