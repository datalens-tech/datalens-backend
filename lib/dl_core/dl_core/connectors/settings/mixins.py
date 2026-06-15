import pydantic

from dl_core.connectors.settings.query_settings import QuerySettingsSettings
import dl_settings


class DatasourceTemplateSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that enables datasource template functionality."""

    ENABLE_DATASOURCE_TEMPLATE: bool = True


class TemplateNameSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that have an associated template name."""

    TEMPLATE_NAME: str


class TableDatasourceSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that enables table datasource functionality."""

    ENABLE_TABLE_DATASOURCE_FORM: bool = True


class DirectSQLSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that gate the DirectSQL endpoint and the readwrite raw SQL level."""

    ENABLE_DIRECTSQL: bool = False


class QuerySettingsSettingsMixin(dl_settings.BaseSettings):
    """Mixin for connector settings that support per-dataset session settings (`Dataset.query_settings`).

    By default this uses `QuerySettingsSettings()` with the feature disabled. The default settings
    object is intentionally restrictive rather than unrestricted (`ENABLED=False`, `ALLOWED=frozenset()`),
    so connectors typically override this field with their own subclass that provides the appropriate
    safe `FORBIDDEN` defaults — and a wider `ALLOWED` if the deployment wants to open up.
    """

    QUERY_SETTINGS: QuerySettingsSettings = pydantic.Field(default_factory=QuerySettingsSettings)
