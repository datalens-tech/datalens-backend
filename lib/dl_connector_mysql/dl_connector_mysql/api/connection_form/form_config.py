import typing

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
from dl_api_connector.form_config.models.common import CommonFieldName
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.rows.base import FormRow
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_connector_mysql.api.connection_info import MySQLConnectionInfoProvider


class MySQLConnectionFormFactory(ConnectionFormFactory):
    DEFAULT_PORT = "3306"

    def _get_host_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.host_row()]

    def _get_port_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.port_row(default_value=self.DEFAULT_PORT)]

    def _get_db_name_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.db_name_row()]

    def _get_username_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.username_row()]

    def _get_password_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.password_row(mode=self.mode)]

    def _get_common_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [
            C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
            rc.raw_sql_level_row(),
            rc.collapse_advanced_settings_row(),
            rc.data_export_forbidden_row(),
        ]

    def _get_edit_api_schema(
        self,
        connector_settings: ConnectorSettingsBase | None,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.username, required=True),
                FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
                FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
                FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
            ],
        )

    def _get_create_api_schema(
        self,
        connector_settings: ConnectorSettingsBase | None,
        edit_api_schema: FormActionApiSchema,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[*edit_api_schema.items, *self._get_top_level_create_api_schema_items()],
            conditions=edit_api_schema.conditions.copy(),
        )

    def _get_check_api_schema(
        self,
        connector_settings: ConnectorSettingsBase | None,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
                FormFieldApiSchema(name=CommonFieldName.username, required=True),
                FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
                *self._get_top_level_check_api_schema_items(),
            ]
        )

    def get_form_config(
        self,
        connector_settings: ConnectorSettingsBase | None,
        tenant: TenantDef | None,
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        edit_api_schema = self._get_edit_api_schema(connector_settings)
        create_api_schema = self._get_create_api_schema(connector_settings, edit_api_schema)
        check_api_schema = self._get_check_api_schema(connector_settings)

        return ConnectionForm(
            title=MySQLConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    *self._get_host_section(rc, connector_settings),
                    *self._get_port_section(rc, connector_settings),
                    *self._get_db_name_section(rc, connector_settings),
                    *self._get_username_section(rc, connector_settings),
                    *self._get_password_section(rc, connector_settings),
                    *self._get_common_section(rc, connector_settings),
                ]
            ),
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
