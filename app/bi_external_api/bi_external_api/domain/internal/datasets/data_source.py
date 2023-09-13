from typing import Optional, TypeVar, Generic, Type, ClassVar

import attr

from bi_constants.enums import ManagedBy, CreateDSFrom
from bi_external_api.attrs_model_mapper import ModelDescriptor, AttribDescriptor
from bi_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_TABLE, SOURCE_TYPE_PG_SUBSELECT
from bi_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE, SOURCE_TYPE_CH_SUBSELECT
from bi_connector_chyt_internal.core.constants import (
    SOURCE_TYPE_CHYT_TABLE,
    SOURCE_TYPE_CHYT_TABLE_LIST,
    SOURCE_TYPE_CHYT_TABLE_RANGE,
    SOURCE_TYPE_CHYT_SUBSELECT,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE,
    SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
)

from .data_source_parameters import (
    DataSourceParams,
    DataSourceParamsSQL,
    DataSourceParamsSchematizedSQL,
    DataSourceParamsSubSQL,
    DataSourceParamsCHYTTableList,
    DataSourceParamsCHYTTableRange,
)
from ..dl_common.base import DatasetAPIBaseModel, IntModelTags

_PARAMS_T = TypeVar("_PARAMS_T", bound=DataSourceParams)
_DSRC_T = TypeVar("_DSRC_T", bound="DataSource")


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="source_type")
@attr.s(kw_only=True, frozen=True)
class DataSource(Generic[_PARAMS_T], DatasetAPIBaseModel):
    source_type: ClassVar[CreateDSFrom]

    id: str = attr.ib()
    title: str = attr.ib()
    connection_id: str = attr.ib(metadata=AttribDescriptor(tags=frozenset({IntModelTags.connection_id})).to_meta())
    # raw_schema
    # index_info_set
    parameters: _PARAMS_T = attr.ib()  # redefined in subclasses
    # parameter_hash
    managed_by: Optional[ManagedBy] = attr.ib(default=None)
    # virtual
    valid: Optional[bool] = attr.ib(default=True)

    ignored_keys: ClassVar[set[str]] = {
        'index_info_set',
        'ref_source_id',
        'raw_schema',
        'parameter_hash',
        'virtual',
    }

    @classmethod
    def create(
            cls: Type[_DSRC_T],
            *,
            id: str,
            title: str,
            connection_id: str,
            parameters: _PARAMS_T,
    ) -> _DSRC_T:
        return cls(
            id=id,
            title=title,
            connection_id=connection_id,
            parameters=parameters,
        )


@ModelDescriptor(is_abstract=True)
@attr.s(kw_only=True)
class DataSourceSQL(DataSource):
    parameters: DataSourceParamsSQL = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s(kw_only=True)
class DataSourceSchematizedSQL(DataSource):
    parameters: DataSourceParamsSchematizedSQL = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s(kw_only=True)
class DataSourceSubSQL(DataSource):
    parameters: DataSourceParamsSubSQL = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s(kw_only=True)
class DataSourceAnyCHYTTableList(DataSource):
    parameters: DataSourceParamsCHYTTableList = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s(kw_only=True)
class DataSourceAnyCHYTTableRange(DataSource):
    parameters: DataSourceParamsCHYTTableRange = attr.ib()


# Non-abstract sources

@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTSubSelect(DataSourceSubSQL):
    source_type = SOURCE_TYPE_CHYT_SUBSELECT


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTUserAuthSubSelect(DataSourceSubSQL):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTTable(DataSourceSQL):
    source_type = SOURCE_TYPE_CHYT_TABLE


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTUserAuthTable(DataSourceSQL):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTTableList(DataSourceAnyCHYTTableList):
    source_type = SOURCE_TYPE_CHYT_TABLE_LIST


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTUserAuthTableList(DataSourceAnyCHYTTableList):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTTableRange(DataSourceAnyCHYTTableRange):
    source_type = SOURCE_TYPE_CHYT_TABLE_RANGE


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceCHYTUserAuthTableRange(DataSourceAnyCHYTTableRange):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourcePGTable(DataSourceSchematizedSQL):
    source_type = SOURCE_TYPE_PG_TABLE


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourcePGSubSQL(DataSourceSubSQL):
    source_type = SOURCE_TYPE_PG_SUBSELECT


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceClickHouseTable(DataSourceSQL):
    source_type = SOURCE_TYPE_CH_TABLE


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceClickHouseSubSQL(DataSourceSubSQL):
    source_type = SOURCE_TYPE_CH_SUBSELECT
