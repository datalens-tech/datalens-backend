import attr

from dl_configs.settings_loaders.meta_definition import s_attrib


@attr.s(frozen=True)
class DatasourceTemplateSettingsMixin:
    """Mixin for connector settings that enables datasource template functionality."""

    ENABLE_DATASOURCE_TEMPLATE: bool = s_attrib("ENABLE_DATASOURCE_TEMPLATE", missing=False)  # type: ignore


@attr.s(frozen=True)
class TableDatasourceSettingsMixin:
    """Mixin for connector settings that enables table datasource functionality."""

    ENABLE_TABLE_DATASOURCE_FORM: bool = s_attrib("ENABLE_TABLE_DATASOURCE_FORM", missing=False)  # type: ignore
