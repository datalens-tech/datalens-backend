from __future__ import annotations

from enum import Enum
from typing import (
    Optional,
    Sequence,
    Type,
)

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models.api_schema import (
    FormActionApiSchema,
    FormApiSchema,
    FormFieldApiSchema,
)
from dl_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    ConnectionFormMode,
)
from dl_api_connector.form_config.models.common import (
    CommonFieldName,
    OAuthApplication,
)
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.rows.base import FormRow
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_connector_ydb.api.ydb.connection_info import YDBConnectionInfoProvider
from dl_connector_ydb.api.ydb.i18n.localizer import Translatable
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings


class YDBOAuthApplication(OAuthApplication):
    ydb = "ydb"


class YDBConnectionFormFactory(ConnectionFormFactory):
    def _get_base_common_api_schema_items(self, names_source: Type[Enum]) -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(name=names_source.host, required=True),  # type: ignore  # 2024-01-24 # TODO: "type[Enum]" has no attribute "host"  [attr-defined]
            FormFieldApiSchema(name=names_source.port, required=True),  # type: ignore  # 2024-01-24 # TODO: "type[Enum]" has no attribute "port"  [attr-defined]
            FormFieldApiSchema(name=names_source.db_name, required=True),  # type: ignore  # 2024-01-24 # TODO: "type[Enum]" has no attribute "db_name"  [attr-defined]
        ]

    def _get_default_db_section(self, rc: RowConstructor, connector_settings: YDBConnectorSettings) -> list[FormRow]:
        oauth_row = C.OAuthTokenRow(
            name=CommonFieldName.token,
            fake_value="******" if self.mode == ConnectionFormMode.edit else None,
            application=YDBOAuthApplication.ydb,
        )
        return [
            oauth_row,
            rc.host_row(default_value=connector_settings.DEFAULT_HOST_VALUE),
            rc.port_row(default_value="2135"),
            rc.db_name_row(),
        ]

    def _get_base_edit_api_schema(self) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            ]
        )

    def _get_base_create_api_schema(self, edit_api_schema: FormActionApiSchema) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ]
        )

    def _get_base_check_api_schema(self, common_api_schema_items: list[FormFieldApiSchema]) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *common_api_schema_items,
                *self._get_top_level_check_api_schema_items(),
            ]
        )

    def _get_base_form_config(
        self,
        db_section_rows: Sequence[FormRow],
        create_api_schema: FormActionApiSchema,
        edit_api_schema: FormActionApiSchema,
        check_api_schema: FormActionApiSchema,
        rc: RowConstructor,
    ) -> ConnectionForm:
        return ConnectionForm(
            title=YDBConnectionInfoProvider.get_title(self._localizer),
            rows=[
                *db_section_rows,
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )

    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, YDBConnectorSettings)
        rc = RowConstructor(localizer=self._localizer)

        edit_api_schema = self._get_base_edit_api_schema()
        common_api_schema_items = self._get_base_common_api_schema_items(names_source=CommonFieldName)
        db_section_rows = self._get_default_db_section(rc=rc, connector_settings=connector_settings)
        common_api_schema_items.append(
            FormFieldApiSchema(
                name=CommonFieldName.token,
                required=self.mode == ConnectionFormMode.create and connector_settings.HAS_AUTH,  # type: ignore  # 2024-01-24 # TODO: Argument "required" to "FormFieldApiSchema" has incompatible type "bool | None"; expected "bool"  [arg-type]
            )
        )
        edit_api_schema.items.extend(common_api_schema_items)

        create_api_schema = self._get_base_create_api_schema(edit_api_schema=edit_api_schema)
        check_api_schema = self._get_base_check_api_schema(common_api_schema_items=common_api_schema_items)
        return self._get_base_form_config(
            db_section_rows=db_section_rows,
            create_api_schema=create_api_schema,
            edit_api_schema=edit_api_schema,
            check_api_schema=check_api_schema,
            rc=rc,
        )
