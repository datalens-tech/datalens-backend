from __future__ import annotations

from typing import Optional

from bi_constants.enums import ConnectionType

from bi_configs.connectors_settings import ConnectorSettingsBase, ClickHouseConnectorSettings

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import (
    FormActionApiSchema, FormFieldApiAction, FormFieldApiSchema, FormFieldApiActionCondition, FormFieldSelector,
    FormFieldConditionalApiAction, FormApiSchema)
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.common import CommonFieldName
from bi_api_connector.form_config.models.rows.base import FormRow
from bi_api_connector.form_config.models.shortcuts.mdb import get_db_host_section

from bi_connector_clickhouse.bi.connection_info import ClickHouseConnectionInfoProvider
from bi_connector_clickhouse.bi.i18n.localizer import Translatable


class ClickHouseConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, ClickHouseConnectorSettings)
        rc = RowConstructor(localizer=self._localizer)
        mdb_enabled = connector_settings.USE_MDB_CLUSTER_PICKER

        mdb_form_fill_row = C.MDBFormFillRow()
        sql_user_management_hidden_row = C.CustomizableRow(items=[
            C.HiddenRowItem(
                inner=True,
                name=C.MDBClusterRow.Inner.sql_user_management,
                default_value=False,
            ),
        ])

        cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = None, None, None
        if mdb_enabled:
            cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = get_db_host_section(
                is_org=self._is_current_tenant_with_org(tenant),
                db_type=ConnectionType.clickhouse,
            )

        manual_host_row = rc.host_row(
            display_conditions={C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.manually} if mdb_enabled else None
        )

        host_section = self._filter_nulls([cloud_tree_selector_row, mdb_cluster_row, mdb_host_row, manual_host_row])

        username_section: list[FormRow]
        if mdb_enabled and mdb_cluster_row is not None:
            mdb_username_row = C.MDBUsernameRow(
                name=CommonFieldName.username,
                db_type=ConnectionType.clickhouse,
                display_conditions={
                    C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.cloud,
                    mdb_cluster_row.Inner.sql_user_management: False,
                },
            )

            manual_username_rows = [
                rc.username_row(
                    display_conditions={
                        C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.cloud,
                        mdb_cluster_row.Inner.sql_user_management: True,
                    },
                ),
                rc.username_row(
                    display_conditions={
                        C.MDBFormFillRow.Inner.mdb_fill_mode: C.MDBFormFillRow.Value.manually,
                    },
                ),
            ]

            username_section = [mdb_username_row, *manual_username_rows]
        else:
            username_section = [rc.username_row()]

        common_api_schema_items: list[FormFieldApiSchema] = [
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CommonFieldName.username),
            FormFieldApiSchema(name=CommonFieldName.password),
            FormFieldApiSchema(name=CommonFieldName.secure),
            FormFieldApiSchema(name=CommonFieldName.ssl_ca),
            FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),

        ]

        edit_api_schema = FormActionApiSchema(
            items=[
                *common_api_schema_items,
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            ],
        )
        if mdb_enabled:
            assert mdb_cluster_row is not None
            edit_api_schema.items.extend(self._filter_nulls([
                FormFieldApiSchema(name=mdb_cluster_row.name, default_action=FormFieldApiAction.skip, required=True),
                FormFieldApiSchema(
                    name=cloud_tree_selector_row.name,
                    default_action=FormFieldApiAction.skip,
                    nullable=True
                ) if cloud_tree_selector_row is not None else None,
            ]))
            edit_api_schema.conditions.append(
                FormFieldApiActionCondition(
                    when=FormFieldSelector(name=C.MDBFormFillRow.Inner.mdb_fill_mode),
                    equals=C.MDBFormFillRow.Value.cloud,
                    then=self._filter_nulls([
                        FormFieldConditionalApiAction(
                            selector=FormFieldSelector(name=mdb_cluster_row.name),
                            action=FormFieldApiAction.include,
                        ),
                        FormFieldConditionalApiAction(
                            selector=FormFieldSelector(name=cloud_tree_selector_row.name),
                            action=FormFieldApiAction.include,
                        ) if cloud_tree_selector_row is not None else None,
                    ])
                )
            )

        create_api_schema = FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ],
            conditions=edit_api_schema.conditions.copy(),
        ) if self.mode == ConnectionFormMode.create else None

        check_api_schema = FormActionApiSchema(items=[
            *common_api_schema_items,
            *self._get_top_level_check_api_schema_items(),
        ])

        return ConnectionForm(
            title=ClickHouseConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls([
                mdb_form_fill_row if mdb_enabled else None,
                *host_section,
                rc.port_row(label_text=Translatable('field_click-house-port'), default_value='8443'),
                sql_user_management_hidden_row if mdb_enabled else None,
                *username_section,
                rc.password_row(mode=self.mode),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(),
                rc.collapse_advanced_settings_row(),
                *rc.ssl_rows(
                    enabled_name=CommonFieldName.secure,
                    enabled_help_text=self._localizer.translate(Translatable('label_clickhouse-ssl-enabled-tooltip')),
                    enabled_default_value=True,
                ),
                rc.data_export_forbidden_row(),
            ]),
            api_schema=FormApiSchema(
                create=create_api_schema,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            )
        )
