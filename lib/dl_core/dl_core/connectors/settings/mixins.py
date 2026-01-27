import dl_settings


class DatasourceTemplateSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that enables datasource template functionality."""

    ENABLE_DATASOURCE_TEMPLATE: bool = True


class TableDatasourceSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that enables table datasource functionality."""

    ENABLE_TABLE_DATASOURCE_FORM: bool = True
