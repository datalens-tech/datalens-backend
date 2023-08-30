from __future__ import annotations

import abc
from typing import Optional, ClassVar

from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormApiSchema, FormFieldApiSchema
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.common import CommonFieldName, OAuthApplication
from bi_api_connector.form_config.models.rows.base import FormRow

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import Translatable


class ServiceOAuthApplication(OAuthApplication):
    service_oauth = 'service_oauth'


class ServiceConnectionBaseFormFactory(ConnectionFormFactory, metaclass=abc.ABCMeta):
    template_name: ClassVar[str]

    @abc.abstractmethod
    def _title(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def _description(self) -> str:
        raise NotImplementedError

    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        rows: list[FormRow] = [
            C.CustomizableRow(items=[C.DescriptionRowItem(text=self._description())]),
        ]
        create_api_schema: Optional[FormActionApiSchema] = None

        if self.mode == ConnectionFormMode.create:
            rows.append(rc.auto_create_dash_row())

            create_api_schema = FormActionApiSchema(items=[
                *self._get_top_level_create_api_schema_items(),
            ])

        return ConnectionForm(
            title=self._title(),
            template_name=self.template_name,
            rows=rows,
            api_schema=FormApiSchema(create=create_api_schema),
        )


class ServiceConnectionWithTokenBaseFormFactory(ServiceConnectionBaseFormFactory, metaclass=abc.ABCMeta):
    oauth_application: ClassVar[ServiceOAuthApplication] = ServiceOAuthApplication.service_oauth

    def _get_token_button_text(self) -> str:
        return self._localizer.translate(Translatable('button_get-token'))

    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        rows: list[FormRow] = [
            C.CustomizableRow(items=[C.DescriptionRowItem(text=self._description())]),
            C.OAuthTokenRow(
                name=CommonFieldName.token,
                fake_value='******' if self.mode == ConnectionFormMode.edit else None,
                application=self.oauth_application,
                button_text=self._get_token_button_text(),
            )
        ]
        if self.mode == ConnectionFormMode.create:
            rows.append(rc.auto_create_dash_row())

        create_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.token, required=True),
            *self._get_top_level_create_api_schema_items(),
        ]) if self.mode == ConnectionFormMode.create else None

        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.token),
        ]) if self.mode == ConnectionFormMode.edit else None

        check_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.token, required=self.mode == ConnectionFormMode.create),
            *self._get_top_level_check_api_schema_items(),
        ])

        return ConnectionForm(
            title=self._title(),
            template_name=self.template_name,
            rows=rows,
            api_schema=FormApiSchema(
                create=create_api_schema,
                edit=edit_api_schema,
                check=check_api_schema,
            ),
        )
