from __future__ import annotations

from typing import Optional

from bi_constants.enums import ConnectionType

from bi_configs.connectors_settings import ConnectorSettingsBase, ClickHouseConnectorSettings

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
import bi_connector_mdb_base.bi.form_config.models.rows.prepared.components as mdb_c
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import (
    FormFieldApiAction, FormFieldApiSchema, FormFieldApiActionCondition, FormFieldSelector,
    FormFieldConditionalApiAction,
)
from bi_api_connector.form_config.models.base import ConnectionForm
from bi_api_connector.form_config.models.common import CommonFieldName
from bi_api_connector.form_config.models.rows.base import FormRow
from bi_api_connector.form_config.models.shortcuts.mdb import get_db_host_section

from bi_connector_clickhouse.core.clickhouse.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_connector_clickhouse.bi.connection_form.form_config import ClickHouseConnectionFormFactory


class ClickHouseMDBConnectionFormFactory(ClickHouseConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, ClickHouseConnectorSettings)
        rc = RowConstructor(localizer=self._localizer)
        mdb_enabled = connector_settings.USE_MDB_CLUSTER_PICKER

        cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = None, None, None
        host_section: list[FormRow]
        if mdb_enabled:
            cloud_tree_selector_row, mdb_cluster_row, mdb_host_row = get_db_host_section(
                is_org=self._is_current_tenant_with_org(tenant),
                db_type=CONNECTION_TYPE_CLICKHOUSE,
            )
            host_section = self._filter_nulls([
                mdb_c.MDBFormFillRow(),
                cloud_tree_selector_row,
                mdb_cluster_row,
                mdb_host_row,
                rc.host_row(display_conditions={mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.manually}),
            ])
        else:
            host_section = [rc.host_row()]

        username_section: list[FormRow]
        if mdb_enabled and mdb_cluster_row is not None:
            sql_user_management_hidden_row = C.CustomizableRow(items=[
                C.HiddenRowItem(
                    inner=True,
                    name=mdb_c.MDBClusterRow.Inner.sql_user_management,
                    default_value=False,
                ),
            ])

            mdb_username_row = mdb_c.MDBUsernameRow(
                name=CommonFieldName.username,
                db_type=CONNECTION_TYPE_CLICKHOUSE,
                display_conditions={
                    mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.cloud,
                    mdb_cluster_row.Inner.sql_user_management: False,
                },
            )

            manual_username_rows = [
                rc.username_row(
                    display_conditions={
                        mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.cloud,
                        mdb_cluster_row.Inner.sql_user_management: True,
                    },
                ),
                rc.username_row(
                    display_conditions={
                        mdb_c.MDBFormFillRow.Inner.mdb_fill_mode: mdb_c.MDBFormFillRow.Value.manually,
                    },
                ),
            ]

            username_section = [sql_user_management_hidden_row, mdb_username_row, *manual_username_rows]
        else:
            username_section = [rc.username_row()]

        edit_api_schema = self.get_base_edit_api_schema()
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
                    when=FormFieldSelector(name=mdb_c.MDBFormFillRow.Inner.mdb_fill_mode),
                    equals=mdb_c.MDBFormFillRow.Value.cloud,
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
        create_api_schema = self.get_base_create_api_schema(edit_api_schema)
        check_api_schema = self.get_base_check_api_schema()

        return self.get_base_form_config(
            host_section=host_section,
            username_section=username_section,
            create_api_schema=create_api_schema,
            edit_api_schema=edit_api_schema,
            check_api_schema=check_api_schema,
            rc=rc,
        )
