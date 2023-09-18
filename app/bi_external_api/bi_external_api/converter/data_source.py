from typing import Type, TypedDict

import attr

from dl_constants.enums import ConnectionType
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import datasets

from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)


@attr.s
class ConverterExtDataSourceToParams(ext.ExtDataSourceProcessor[datasets.DataSourceParams]):

    def process_schematized_table(self, dsrc: ext.TableDataSourceSpec) -> datasets.DataSourceParams:
        return datasets.DataSourceParamsSQL(
            db_name=dsrc.db_name,
            table_name=dsrc.table_name,
            db_version=None,
        )

    def process_table(self, dsrc: ext.TableDataSourceSpec) -> datasets.DataSourceParams:
        return datasets.DataSourceParamsSQL(
            db_name=dsrc.db_name,
            table_name=dsrc.table_name,
            db_version=None,
        )

    def process_chyt_range(self, dsrc: ext.CHYTTableRangeDataSourceSpec) -> datasets.DataSourceParams:
        return datasets.DataSourceParamsCHYTTableRange(
            directory_path=dsrc.path,
            range_from=dsrc.start,
            range_to=dsrc.end,
        )

    def process_sub_select(self, dsrc: ext.SubSelectDataSourceSpec) -> datasets.DataSourceParams:
        return datasets.DataSourceParamsSubSQL(subsql=dsrc.sql)

    def process_chyt_list(self, dsrc: ext.CHYTTableListDataSourceSpec) -> datasets.DataSourceParams:
        return datasets.DataSourceParamsCHYTTableList(
            table_names="\n".join(dsrc.tables)
        )


MAP_EXT_DSRC_TYPE_CONN_TYPE_INT_DSRC_TYPE: dict[
    Type[ext.DataSourceSpec], dict[ConnectionType, Type[datasets.DataSource]]
] = {
    ext.TableDataSourceSpec: {
        CONNECTION_TYPE_CH_OVER_YT: datasets.DataSourceCHYTTable,
        CONNECTION_TYPE_CH_OVER_YT_USER_AUTH: datasets.DataSourceCHYTUserAuthTable,
        CONNECTION_TYPE_CLICKHOUSE: datasets.DataSourceClickHouseTable,
    },
    ext.SchematizedTableDataSourceSpec: {
        CONNECTION_TYPE_POSTGRES: datasets.DataSourcePGTable,
    },
    ext.SubSelectDataSourceSpec: {
        CONNECTION_TYPE_CH_OVER_YT: datasets.DataSourceCHYTSubSelect,
        CONNECTION_TYPE_CH_OVER_YT_USER_AUTH: datasets.DataSourceCHYTUserAuthSubSelect,
        CONNECTION_TYPE_POSTGRES: datasets.DataSourcePGSubSQL,
        CONNECTION_TYPE_CLICKHOUSE: datasets.DataSourceClickHouseSubSQL,
    },
    ext.CHYTTableRangeDataSourceSpec: {
        CONNECTION_TYPE_CH_OVER_YT: datasets.DataSourceCHYTTableRange,
        CONNECTION_TYPE_CH_OVER_YT_USER_AUTH: datasets.DataSourceCHYTUserAuthTableRange,
    },
    ext.CHYTTableListDataSourceSpec: {
        CONNECTION_TYPE_CH_OVER_YT: datasets.DataSourceCHYTTableList,
        CONNECTION_TYPE_CH_OVER_YT_USER_AUTH: datasets.DataSourceCHYTUserAuthTableList,
    }
}


def resolve_internal_dsrc_type(dsrc: ext.DataSource, conn_type: ConnectionType) -> Type[datasets.DataSource]:
    ext_dsrc_type = type(dsrc.spec)

    try:
        return MAP_EXT_DSRC_TYPE_CONN_TYPE_INT_DSRC_TYPE[ext_dsrc_type][conn_type]
    except KeyError:
        raise ValueError(f"Connection {conn_type} does not support data source {ext_dsrc_type.__name__}")


def convert_external_dsrc_to_add_action(
        dsrc: ext.DataSource, *,
        wb_context: WorkbookContext
) -> datasets.ActionDataSourceAdd:
    internal_conn_inst = wb_context.resolve_entry(
        datasets.ConnectionInstance,
        ref=wb_context.ref(name=dsrc.connection_ref),
    )

    params = ConverterExtDataSourceToParams().process(dsrc)

    int_data_source = resolve_internal_dsrc_type(dsrc, internal_conn_inst.type).create(
        id=dsrc.id,
        title=dsrc.title,
        connection_id=internal_conn_inst.summary.id,
        parameters=params,
    )

    return datasets.ActionDataSourceAdd(source=int_data_source)


@attr.s
class ConverterDataSourceParamsToExt:
    class CommonArgs(TypedDict):
        connection_ref: str
        id: str
        title: str

    common_args: CommonArgs = attr.ib()

    def process(self, dsrc_params: datasets.DataSourceParams) -> ext.DataSource:
        return ext.DataSource(
            spec=self.process_spec(dsrc_params),
            **self.common_args,
        )

    def process_spec(self, dsrc_params: datasets.DataSourceParams) -> ext.DataSourceSpec:
        if isinstance(dsrc_params, datasets.DataSourceParamsSQL):
            db_name = dsrc_params.db_name
            table_name = dsrc_params.table_name
            # TODO FIX: BI-3005 Find out in which cases None is valid value & make dedicated ext class for it
            assert table_name is not None, "Got None in data source table name from internal API"

            if isinstance(dsrc_params, datasets.DataSourceParamsSchematizedSQL):
                return ext.SchematizedTableDataSourceSpec(
                    db_name=db_name,
                    schema_name=dsrc_params.schema_name,
                    table_name=table_name,
                )

            return ext.TableDataSourceSpec(
                db_name=db_name,
                table_name=table_name,
            )

        if isinstance(dsrc_params, datasets.DataSourceParamsSubSQL):
            return ext.SubSelectDataSourceSpec(
                sql=dsrc_params.subsql,
            )

        if isinstance(dsrc_params, datasets.DataSourceParamsCHYTTableList):
            return ext.CHYTTableListDataSourceSpec(
                tables=tuple(table_name for table_name in dsrc_params.table_names.split("\n")),
            )

        if isinstance(dsrc_params, datasets.DataSourceParamsCHYTTableRange):
            return ext.CHYTTableRangeDataSourceSpec(
                path=dsrc_params.directory_path,
                start=dsrc_params.range_from,
                end=dsrc_params.range_to,
            )

        raise TypeError(f"Unexpected type of data source params: {type(dsrc_params)}")


def convert_internal_datasource_to_external_data_source(
        int_dsrc: datasets.DataSource,
        *,
        wb_context: WorkbookContext
) -> ext.DataSource:
    internal_conn_inst = wb_context.resolve_entry(
        datasets.ConnectionInstance,
        ref=wb_context.ref(id=int_dsrc.connection_id),
    )

    return ConverterDataSourceParamsToExt(common_args=dict(
        connection_ref=internal_conn_inst.summary.name,
        id=int_dsrc.id,
        title=int_dsrc.title,
    )).process(int_dsrc.parameters)
