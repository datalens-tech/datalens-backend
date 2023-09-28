from __future__ import annotations

from typing import Optional

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
from dl_constants.enums import RawSQLLevel

from bi_connector_mdb_base.api.form_config.models.common import MDBFieldName
from bi_connector_mdb_base.api.form_config.models.rows.prepared import components as mdb_components
from bi_connector_yql.api.yq.connection_info import YQConnectionInfoProvider
from bi_connector_yql.api.yql_base.i18n.localizer import Translatable
from bi_connector_yql.core.yq.settings import YQConnectorSettings


class YQConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, YQConnectorSettings)

        rc = RowConstructor(localizer=self._localizer)

        mdb_enabled = connector_settings.USE_MDB_CLUSTER_PICKER

        sa_section: list[FormRow]
        if not mdb_enabled:
            sa_key_row = C.CustomizableRow(
                items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(Translatable("field_sa-key")),
                    ),
                    C.InputRowItem(
                        name=CommonFieldName.password,
                        width="m",
                        default_value="" if self.mode == ConnectionFormMode.create else None,
                        fake_value=None if self.mode == ConnectionFormMode.create else "******",
                        control_props=C.InputRowItem.Props(type="password"),
                    ),
                ]
            )
            sa_section = [sa_key_row]

            edit_api_schema = FormActionApiSchema(
                items=[
                    FormFieldApiSchema(name=CommonFieldName.password),
                    FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                    FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
                ]
            )

            create_api_schema = FormActionApiSchema(
                items=[
                    FormFieldApiSchema(name=CommonFieldName.password, required=True),
                    FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                    FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
                    *self._get_top_level_create_api_schema_items(),
                ]
            )

            check_api_schema = FormActionApiSchema(
                items=[
                    FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
                    *self._get_top_level_check_api_schema_items(),
                ]
            )

        else:
            sa_section = [
                mdb_components.CloudTreeSelectRow(name=MDBFieldName.folder_id),
                mdb_components.ServiceAccountRow(name=MDBFieldName.service_account_id),
            ]

            edit_api_schema = FormActionApiSchema(
                items=[
                    FormFieldApiSchema(name=MDBFieldName.folder_id, required=True),
                    FormFieldApiSchema(name=MDBFieldName.service_account_id, required=True),
                    FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                    FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
                ]
            )

            create_api_schema = FormActionApiSchema(
                items=[*edit_api_schema.items, *self._get_top_level_create_api_schema_items()]
            )

            check_api_schema = FormActionApiSchema(
                items=[
                    FormFieldApiSchema(name=MDBFieldName.folder_id, required=True),
                    FormFieldApiSchema(name=MDBFieldName.service_account_id, required=True),
                    *self._get_top_level_check_api_schema_items(),
                ]
            )

        return ConnectionForm(
            title=YQConnectionInfoProvider.get_title(self._localizer),
            rows=[
                *sa_section,
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(default_value=RawSQLLevel.dashsql),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
