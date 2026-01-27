import attr

from dl_configs.settings_loaders.meta_definition import s_attrib
import dl_settings


@attr.s(frozen=True)
class DeprecatedDatasourceTemplateSettingsMixin:
    """Mixin for connector settings that enables datasource template functionality."""

    ENABLE_DATASOURCE_TEMPLATE: bool = s_attrib("ENABLE_DATASOURCE_TEMPLATE", missing=True)  # type: ignore


@attr.s(frozen=True)
class DeprecatedTableDatasourceSettingsMixin:
    """Mixin for connector settings that enables table datasource functionality."""

    ENABLE_TABLE_DATASOURCE_FORM: bool = s_attrib("ENABLE_TABLE_DATASOURCE_FORM", missing=True)  # type: ignore


class DatasourceTemplateSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that enables datasource template functionality."""

    ENABLE_DATASOURCE_TEMPLATE: bool = True


class TableDatasourceSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that enables table datasource functionality."""

    ENABLE_TABLE_DATASOURCE_FORM: bool = True
