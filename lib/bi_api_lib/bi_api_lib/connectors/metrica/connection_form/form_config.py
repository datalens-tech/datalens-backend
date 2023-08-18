from __future__ import annotations

import abc
from enum import unique
from typing import Optional, ClassVar

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormApiSchema, FormFieldApiSchema
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.common import CommonFieldName, FormFieldName, OAuthApplication
from bi_api_connector.form_config.models.rows.base import FormRow

from bi_api_lib.connectors.metrica.connection_info import (
    MetricaConnectionInfoProvider, AppMetricaConnectionInfoProvider,
)
from bi_api_lib.i18n.localizer import Translatable


class MetricaOAuthApplication(OAuthApplication):
    metrika_api = 'metrika_api'
    appmetrica_api = 'appmetrica_api'


@unique
class MetricaFieldName(FormFieldName):
    counter_id = 'counter_id'
    accuracy = 'accuracy'


class MetricaLikeBaseFormFactory(ConnectionFormFactory, metaclass=abc.ABCMeta):
    template_name: ClassVar[str]
    oauth_application: ClassVar[OAuthApplication]

    @abc.abstractmethod
    def _title(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def _counter_row(self, manual_input: bool) -> C.CounterRow:
        raise NotImplementedError

    @abc.abstractmethod
    def _allow_manual_counter_input(self, connector_settings: ConnectorsSettingsByType) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def _allow_auto_dash_creation(self, connector_settings: ConnectorsSettingsByType) -> bool:
        raise NotImplementedError

    def get_form_config(
            self,
            connectors_settings: Optional[ConnectorsSettingsByType],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connectors_settings is not None
        rc = RowConstructor(localizer=self._localizer)

        rows: list[FormRow] = [
            C.OAuthTokenRow(
                name=CommonFieldName.token,
                fake_value='******' if self.mode == ConnectionFormMode.edit else None,
                application=self.oauth_application,
                label_text=self._localizer.translate(Translatable('field_oauth-token')),
                button_text=self._localizer.translate(Translatable('button_get-token')),
            ),
            self._counter_row(manual_input=self._allow_manual_counter_input(connectors_settings)),
            C.AccuracyRow(name=MetricaFieldName.accuracy),
        ]

        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=MetricaFieldName.counter_id, required=True),
            FormFieldApiSchema(name=CommonFieldName.token),
            FormFieldApiSchema(name=MetricaFieldName.accuracy, nullable=True),
        ]) if self.mode == ConnectionFormMode.edit else None

        create_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=MetricaFieldName.counter_id, required=True),
            FormFieldApiSchema(name=CommonFieldName.token, required=True),
            FormFieldApiSchema(name=MetricaFieldName.accuracy, nullable=True),
            *self._get_top_level_create_api_schema_items(),
        ]) if self.mode == ConnectionFormMode.create else None

        if self.mode == ConnectionFormMode.create and self._allow_auto_dash_creation(connectors_settings):
            rows.append(rc.auto_create_dash_row())

        return ConnectionForm(
            title=self._title(),
            template_name=self.template_name,
            rows=rows,
            api_schema=FormApiSchema(create=create_api_schema, edit=edit_api_schema),
        )


class MetricaAPIConnectionFormFactory(MetricaLikeBaseFormFactory):
    template_name = 'metrica_api'
    oauth_application = MetricaOAuthApplication.metrika_api

    def _title(self) -> str:
        return MetricaConnectionInfoProvider.get_title(self._localizer)

    def _counter_row(self, manual_input: bool) -> C.MetricaCounterRowItem:
        return C.MetricaCounterRowItem(
            name=MetricaFieldName.counter_id,
            allow_manual_input=manual_input,
        )

    def _allow_manual_counter_input(self, connector_settings: ConnectorsSettingsByType) -> bool:
        assert (metrica_settings := connector_settings.METRICA) is not None
        return metrica_settings.COUNTER_ALLOW_MANUAL_INPUT

    def _allow_auto_dash_creation(self, connector_settings: ConnectorsSettingsByType) -> bool:
        assert (metrica_settings := connector_settings.METRICA) is not None
        return metrica_settings.ALLOW_AUTO_DASH_CREATION


class AppMetricaAPIConnectionFormFactory(MetricaLikeBaseFormFactory):
    template_name = 'appmetrica_api'
    oauth_application = MetricaOAuthApplication.appmetrica_api

    def _title(self) -> str:
        return AppMetricaConnectionInfoProvider.get_title(self._localizer)

    def _counter_row(self, manual_input: bool) -> C.AppMetricaCounterRowItem:
        return C.AppMetricaCounterRowItem(
            name=MetricaFieldName.counter_id,
            allow_manual_input=manual_input,
        )

    def _allow_manual_counter_input(self, connector_settings: ConnectorsSettingsByType) -> bool:
        assert (appmetrica_settings := connector_settings.APPMETRICA) is not None
        return appmetrica_settings.COUNTER_ALLOW_MANUAL_INPUT

    def _allow_auto_dash_creation(self, connector_settings: ConnectorsSettingsByType) -> bool:
        assert (appmetrica_settings := connector_settings.APPMETRICA) is not None
        return appmetrica_settings.ALLOW_AUTO_DASH_CREATION
