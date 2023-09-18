from __future__ import annotations

from enum import unique
from typing import Optional

import attr

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
    FormFieldName,
    MarkdownStr,
    remap_skip_if_null,
)
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.rows.base import (
    DisplayConditionsMixin,
    FormRow,
    TDisplayConditions,
)
from dl_api_connector.form_config.models.rows.prepared.base import PreparedRow
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_connector_snowflake.bi.connection_info import SnowflakeConnectionInfoProvider
from dl_connector_snowflake.bi.i18n.localizer import Translatable


@unique
class SnowFlakeFieldName(FormFieldName):
    account_name = "account_name"
    client_id = "client_id"
    client_secret = "client_secret"
    schema = "schema"
    warehouse = "warehouse"
    user_name = "user_name"
    user_role = "user_role"

    snowflake_auth = "snowflake_auth"
    snowflake_db_details = "snowflake_db_details"
    # ^ actually internal fields that show if a certain section is opened, but it is easier to have them here for now


@attr.s(kw_only=True, frozen=True)
class SnowFlakeOAuthIntegrationRow(PreparedRow, DisplayConditionsMixin):
    type = "snowflake_oauth_integration"


@attr.s(kw_only=True, frozen=True)
class OAuthSnowFlakeRow(PreparedRow, DisplayConditionsMixin):
    type = "oauth_snowflake"

    class Inner(PreparedRow.Inner):
        refresh_token = "refresh_token"
        refresh_token_expire_time = "refresh_token_expire_time"

    fake_value: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null("fakeValue"))
    button_text: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null("buttonText"))


def _basic_input_row(
    label_text: str,
    field: FormFieldName,
    help_text: Optional[MarkdownStr],
    display_conditions: Optional[TDisplayConditions] = None,
    password_input: bool = False,
    fake_value: Optional[str] = None,
) -> C.CustomizableRow:
    return C.CustomizableRow(
        items=[
            C.LabelRowItem(
                text=label_text,
                help_text=help_text,
                display_conditions=display_conditions,
            ),
            C.InputRowItem(
                name=field,
                width="m",
                display_conditions=display_conditions,
                control_props=C.InputRowItem.Props(type="password") if password_input else None,
                fake_value=fake_value,
            ),
        ]
    )


class SnowFlakeConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        on_create = self.mode == ConnectionFormMode.create

        common_api_schema_items: list[FormFieldApiSchema] = [
            FormFieldApiSchema(name=SnowFlakeFieldName.account_name, required=True),
            FormFieldApiSchema(name=SnowFlakeFieldName.client_id, required=on_create),
            FormFieldApiSchema(name=SnowFlakeFieldName.client_secret, required=on_create),
            FormFieldApiSchema(name=OAuthSnowFlakeRow.Inner.refresh_token, required=on_create),
            FormFieldApiSchema(name=SnowFlakeFieldName.user_name, required=True),
            FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
            FormFieldApiSchema(name=SnowFlakeFieldName.schema, required=True),
            FormFieldApiSchema(name=SnowFlakeFieldName.warehouse, required=True),
        ]

        edit_api_schema = FormActionApiSchema(
            items=[
                *common_api_schema_items,
                FormFieldApiSchema(name=OAuthSnowFlakeRow.Inner.refresh_token_expire_time, nullable=True),
                FormFieldApiSchema(name=SnowFlakeFieldName.user_role, nullable=True),
            ]
        )

        create_api_schema = FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ]
        )

        check_api_schema = FormActionApiSchema(
            items=[
                *common_api_schema_items,
                *self._get_top_level_check_api_schema_items(),
            ]
        )

        on_auth_opened: TDisplayConditions = {SnowFlakeFieldName.snowflake_auth: "opened"}
        on_db_opened: TDisplayConditions = {SnowFlakeFieldName.snowflake_db_details: "opened"}

        help_text_sf_docs = self._localizer.translate(Translatable("label_snowflake-doc-help"))
        sf_docs_url = "https://docs.snowflake.com/en"
        help_account_name = help_text_sf_docs.format(
            LINK=f"{sf_docs_url}/user-guide/client-redirect.html#snowsight-the-snowflake-web-interface"
        )
        help_client = help_text_sf_docs.format(
            LINK=f"{sf_docs_url}/sql-reference/functions/system_show_oauth_client_secrets.html#system-show-oauth-client-secrets"
        )
        help_user_name = help_text_sf_docs.format(LINK=f"{sf_docs_url}/sql-reference/sql/create-user.html#create-user")
        help_db = help_text_sf_docs.format(LINK=f"{sf_docs_url}/sql-reference/sql/create-database.html#create-database")
        help_schema = help_text_sf_docs.format(LINK=f"{sf_docs_url}/sql-reference/sql/create-schema.html#create-schema")
        help_warehouse = help_text_sf_docs.format(
            LINK=f"{sf_docs_url}/sql-reference/sql/create-warehouse.html#create-warehouse"
        )
        help_user_role = help_text_sf_docs.format(LINK=f"{sf_docs_url}/sql-reference/sql/create-role.html#create-role")

        auth_section: list[FormRow] = [
            SnowFlakeOAuthIntegrationRow(display_conditions=on_auth_opened),
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_account-name")),
                field=SnowFlakeFieldName.account_name,
                help_text=help_account_name,
                display_conditions=on_auth_opened,
            ),
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_client-id")),
                field=SnowFlakeFieldName.client_id,
                help_text=help_client,
                display_conditions=on_auth_opened,
            ),
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_client-secret")),
                field=SnowFlakeFieldName.client_secret,
                help_text=help_client,
                display_conditions=on_auth_opened,
                password_input=True,
                fake_value="******" if self.mode == ConnectionFormMode.edit else None,
            ),
            OAuthSnowFlakeRow(
                button_text=None
                if self.mode == ConnectionFormMode.create
                else self._localizer.translate(Translatable("button_get-new-token")),
                display_conditions=on_auth_opened,
                fake_value="******" if self.mode == ConnectionFormMode.edit else None,
            ),
            C.CustomizableRow(
                items=[
                    C.HiddenRowItem(name=OAuthSnowFlakeRow.Inner.refresh_token_expire_time),
                ]
            ),
        ]

        db_section: list[FormRow] = [
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_user-name")),
                field=SnowFlakeFieldName.user_name,
                help_text=help_user_name,
                display_conditions=on_db_opened,
            ),
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_database")),
                field=CommonFieldName.db_name,
                help_text=help_db,
                display_conditions=on_db_opened,
            ),
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_database-schema")),
                field=SnowFlakeFieldName.schema,
                help_text=help_schema,
                display_conditions=on_db_opened,
            ),
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_warehouse")),
                field=SnowFlakeFieldName.warehouse,
                help_text=help_warehouse,
                display_conditions=on_db_opened,
            ),
            _basic_input_row(
                label_text=self._localizer.translate(Translatable("label_user-role")),
                field=SnowFlakeFieldName.user_role,
                help_text=help_user_role,
                display_conditions=on_db_opened,
            ),
        ]

        return ConnectionForm(
            title=SnowflakeConnectionInfoProvider.get_title(self._localizer),
            rows=[
                C.CollapseRow(
                    name=SnowFlakeFieldName.snowflake_auth,
                    text=self._localizer.translate(Translatable("label_showflake-auth-section-title")),
                    component_props=C.CollapseRow.Props(default_expanded=self.mode == ConnectionFormMode.create),
                ),
                *auth_section,
                C.CollapseRow(
                    name=SnowFlakeFieldName.snowflake_db_details,
                    text=self._localizer.translate(Translatable("label_showflake-db-details-section-title")),
                    component_props=C.CollapseRow.Props(default_expanded=self.mode == ConnectionFormMode.edit),
                ),
                *db_section,
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
