from typing import Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor


@attr.s(frozen=True)
class DataSourceParams:
    pass


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceParamsSQL(DataSourceParams):
    db_name: Optional[str] = attr.ib()
    table_name: Optional[str] = attr.ib()
    db_version: Optional[str] = attr.ib()


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceParamsSchematizedSQL(DataSourceParamsSQL):
    schema_name: Optional[str] = attr.ib()


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceParamsSubSQL(DataSourceParams):
    subsql: str = attr.ib()


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceParamsCHYTTableList(DataSourceParams):
    table_names: str = attr.ib()  # newline-separated tables


@ModelDescriptor()
@attr.s(kw_only=True)
class DataSourceParamsCHYTTableRange(DataSourceParams):
    directory_path: str = attr.ib()
    range_from: str = attr.ib()
    range_to: str = attr.ib()
