from __future__ import annotations

import abc
from typing import (
    ClassVar,
    Optional,
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

import dl_connector_metrica.api.connection_form.components as components
from dl_connector_metrica.api.connection_form.components import MetricaFieldName
from dl_connector_metrica.api.connection_form.rows import (
    AccuracyRow,
    AppMetricaCounterRowItem,
    CounterRow,
    MetricaCounterRowItem,
)
from dl_connector_metrica.api.connection_info import (
    AppMetricaConnectionInfoProvider,
    MetricaConnectionInfoProvider,
)
from dl_connector_metrica.api.i18n.localizer import Translatable
from dl_connector_metrica.core.settings import (
    AppmetricaConnectorSettings,
    MetricaConnectorSettings,
)


class MetricaOAuthApplication(OAuthApplication):
    metrika_api = "metrika_api"
    appmetrica_api = "appmetrica_api"


class MetricaLikeBaseFormFactory(ConnectionFormFactory, metaclass=abc.ABCMeta):
    template_name: ClassVar[str]
    oauth_application: ClassVar[OAuthApplication]

    @abc.abstractmethod
    def _title(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def _counter_row(
        self, manual_input: bool, connector_settings: ConnectorSettingsBase
    ) -> CounterRow | C.CustomizableRow:
        raise NotImplementedError

    @abc.abstractmethod
    def _allow_manual_counter_input(self, connector_settings: ConnectorSettingsBase) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def _allow_auto_dash_creation(self, connector_settings: ConnectorSettingsBase) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def _is_backend_driven_form(self, connector_settings: ConnectorSettingsBase) -> bool:
        raise NotImplementedError

    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None
        rc = RowConstructor(localizer=self._localizer)

        oauth_token_row = (
            C.OAuthTokenRow(
                name=CommonFieldName.token,
                fake_value="******" if self.mode == ConnectionFormMode.edit else None,
                application=self.oauth_application,
                label_text=self._localizer.translate(Translatable("field_oauth-token")),
                button_text=self._localizer.translate(Translatable("button_get-token")),
            )
            if not self._is_backend_driven_form(connector_settings)
            else components.oauth_token_row(localizer=self._localizer, mode=self.mode)
        )

        rows: list[FormRow] = [
            oauth_token_row,
            self._counter_row(
                manual_input=self._allow_manual_counter_input(connector_settings), connector_settings=connector_settings
            ),
            AccuracyRow(name=MetricaFieldName.accuracy),
            rc.collapse_advanced_settings_row(),
            rc.data_export_forbidden_row(),
        ]

        edit_api_schema = (
            FormActionApiSchema(
                items=[
                    FormFieldApiSchema(name=MetricaFieldName.counter_id, required=True),
                    FormFieldApiSchema(name=CommonFieldName.token),
                    FormFieldApiSchema(name=MetricaFieldName.accuracy, nullable=True),
                    FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
                ]
            )
            if self.mode == ConnectionFormMode.edit
            else None
        )

        create_api_schema = (
            FormActionApiSchema(
                items=[
                    FormFieldApiSchema(name=MetricaFieldName.counter_id, required=True),
                    FormFieldApiSchema(name=CommonFieldName.token, required=True),
                    FormFieldApiSchema(name=MetricaFieldName.accuracy, nullable=True),
                    *self._get_top_level_create_api_schema_items(),
                    FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
                ]
            )
            if self.mode == ConnectionFormMode.create
            else None
        )

        if self.mode == ConnectionFormMode.create and self._allow_auto_dash_creation(connector_settings):
            rows.append(rc.auto_create_dash_row())

        return ConnectionForm(
            title=self._title(),
            template_name=self.template_name,
            rows=rows,
            api_schema=FormApiSchema(create=create_api_schema, edit=edit_api_schema),
        )


class MetricaAPIConnectionFormFactory(MetricaLikeBaseFormFactory):
    template_name = "metrica_api"
    oauth_application = MetricaOAuthApplication.metrika_api

    def _title(self) -> str:
        return MetricaConnectionInfoProvider.get_title(self._localizer)

    def _counter_row(
        self, manual_input: bool, connector_settings: ConnectorSettingsBase
    ) -> MetricaCounterRowItem | C.CustomizableRow:
        return (
            MetricaCounterRowItem(
                name=MetricaFieldName.counter_id,
                allow_manual_input=manual_input,
            )
            if not self._is_backend_driven_form(connector_settings)
            else C.CustomizableRow(
                items=[
                    C.LabelRowItem(text=self._localizer.translate(Translatable("field_counter-id-metrica"))),
                    C.InputRowItem(
                        name=MetricaFieldName.counter_id,
                        width="l",
                        placeholder=self._localizer.translate(Translatable("placeholder_counter-id-metrica")),
                    ),
                ]
            )
        )

    def _allow_manual_counter_input(self, connector_settings: ConnectorSettingsBase) -> bool:
        assert isinstance(connector_settings, MetricaConnectorSettings)
        return connector_settings.COUNTER_ALLOW_MANUAL_INPUT

    def _allow_auto_dash_creation(self, connector_settings: ConnectorSettingsBase) -> bool:
        assert isinstance(connector_settings, MetricaConnectorSettings)
        return connector_settings.ALLOW_AUTO_DASH_CREATION

    def _is_backend_driven_form(self, connector_settings: ConnectorSettingsBase) -> bool:
        assert isinstance(connector_settings, MetricaConnectorSettings)
        return connector_settings.BACKEND_DRIVEN_FORM


class AppMetricaAPIConnectionFormFactory(MetricaLikeBaseFormFactory):
    template_name = "appmetrica_api"
    oauth_application = MetricaOAuthApplication.appmetrica_api

    def _title(self) -> str:
        return AppMetricaConnectionInfoProvider.get_title(self._localizer)

    def _counter_row(
        self, manual_input: bool, connector_settings: ConnectorSettingsBase
    ) -> AppMetricaCounterRowItem | C.CustomizableRow:
        return (
            AppMetricaCounterRowItem(
                name=MetricaFieldName.counter_id,
                allow_manual_input=manual_input,
            )
            if not self._is_backend_driven_form(connector_settings)
            else C.CustomizableRow(
                items=[
                    C.LabelRowItem(text=self._localizer.translate(Translatable("field_counter-id-appmetrica"))),
                    C.InputRowItem(
                        name=MetricaFieldName.counter_id,
                        width="l",
                        placeholder=self._localizer.translate(Translatable("placeholder_counter-id-appmetrica")),
                    ),
                ]
            )
        )

    def _allow_manual_counter_input(self, connector_settings: ConnectorSettingsBase) -> bool:
        assert isinstance(connector_settings, AppmetricaConnectorSettings)
        return connector_settings.COUNTER_ALLOW_MANUAL_INPUT

    def _allow_auto_dash_creation(self, connector_settings: ConnectorSettingsBase) -> bool:
        assert isinstance(connector_settings, AppmetricaConnectorSettings)
        return connector_settings.ALLOW_AUTO_DASH_CREATION

    def _is_backend_driven_form(self, connector_settings: ConnectorSettingsBase) -> bool:
        assert isinstance(connector_settings, AppmetricaConnectorSettings)
        return connector_settings.BACKEND_DRIVEN_FORM
