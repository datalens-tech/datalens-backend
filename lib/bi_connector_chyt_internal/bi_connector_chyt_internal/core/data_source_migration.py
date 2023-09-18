from dl_connector_chyt.core.data_source_spec import (
    CHYTTableDataSourceSpec, CHYTSubselectDataSourceSpec,
    CHYTTableListDataSourceSpec, CHYTTableRangeDataSourceSpec
)
from dl_connector_chyt.core.data_source_migration import BaseCHYTDataSourceMigrator
from bi_connector_chyt_internal.core.constants import (
    SOURCE_TYPE_CHYT_TABLE, SOURCE_TYPE_CHYT_SUBSELECT,
    SOURCE_TYPE_CHYT_TABLE_LIST, SOURCE_TYPE_CHYT_TABLE_RANGE,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE, SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST, SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE,
)


class CHYTInternalDataSourceMigrator(BaseCHYTDataSourceMigrator):
    table_source_type = SOURCE_TYPE_CHYT_TABLE
    table_dsrc_spec_cls = CHYTTableDataSourceSpec

    subselect_source_type = SOURCE_TYPE_CHYT_SUBSELECT
    subselect_dsrc_spec_cls = CHYTSubselectDataSourceSpec

    table_list_source_type = SOURCE_TYPE_CHYT_TABLE_LIST
    table_list_dsrc_spec_cls = CHYTTableListDataSourceSpec

    table_range_source_type = SOURCE_TYPE_CHYT_TABLE_RANGE
    table_range_dsrc_spec_cls = CHYTTableRangeDataSourceSpec


class CHYTInternalUserAuthDataSourceMigrator(BaseCHYTDataSourceMigrator):
    table_source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE
    table_dsrc_spec_cls = CHYTTableDataSourceSpec

    subselect_source_type = SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT
    subselect_dsrc_spec_cls = CHYTSubselectDataSourceSpec

    table_list_source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST
    table_list_dsrc_spec_cls = CHYTTableListDataSourceSpec

    table_range_source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE
    table_range_dsrc_spec_cls = CHYTTableRangeDataSourceSpec
