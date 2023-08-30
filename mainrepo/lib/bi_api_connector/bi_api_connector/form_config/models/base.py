from __future__ import annotations

import abc
from enum import Enum
from typing import Optional, Any

import attr

from bi_api_connector.form_config.models.rows import CustomizableRow
from bi_api_connector.form_config.models.rows.prepared.base import PreparedRow
from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantDef, TenantYCOrganization

from bi_api_connector.form_config.models.api_schema import FormApiSchema, FormFieldApiSchema
from bi_api_connector.form_config.models.common import (
    SerializableConfig,
    TopLevelFieldName,
    TFieldName,
    remap_skip_if_null,
)
from bi_api_connector.form_config.models.rows.base import FormRow, FormFieldMixin, DisplayConditionsMixin
from bi_i18n.localizer_base import Localizer


@attr.s(kw_only=True, frozen=True)
class FormUIOverride(SerializableConfig):
    show_create_dataset_btn: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null('showCreateDatasetButton'))
    show_create_ql_chart_btn: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null('showCreateQlChartButton'))


TOP_LEVEL_NON_CONFIG_FIELDS: set[TopLevelFieldName] = {
    field_name for field_name in TopLevelFieldName
}


@attr.s(kw_only=True, frozen=True)
class ConnectionForm(SerializableConfig):
    """
    title               shown at the top of the form
    rows                form body (labels, fields, etc. => without action buttons)
    api_schema          specifies form fields' validation and action request body schemas
    template_name       name of the template that stored in the US; for connectors with templates (service, partner)
    form_ui_override    override form elements' visibility

    Keep in mind that the naming convention here is camelCase,
    so either use it in field names or remap keys using SerializableConfig features where it is needed
    """

    title: str = attr.ib()
    rows: list[FormRow] = attr.ib()
    api_schema: Optional[FormApiSchema] = attr.ib(default=None, metadata=remap_skip_if_null('apiSchema'))
    template_name: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null('templateName'))
    form_ui_override: Optional[FormUIOverride] = attr.ib(default=None, metadata=remap_skip_if_null('uiSchema'))

    def validate_conditional_fields(self) -> None:
        form_field_names = self._get_form_field_names()
        fields_in_conditions = self._get_conditional_fields()

        rogue_fields = fields_in_conditions - form_field_names - TOP_LEVEL_NON_CONFIG_FIELDS

        assert not rogue_fields, (
            f'Fields {rogue_fields} are used in conditions, but not defined in the form or component inner fields')

    def validate_api_schema_fields(self) -> None:
        form_field_names = self._get_form_field_names()
        api_schema_fields = self._get_api_schema_fields()

        rogue_fields = api_schema_fields - form_field_names - TOP_LEVEL_NON_CONFIG_FIELDS

        assert not rogue_fields, (
            f'Fields {rogue_fields} are referred to in api schema conditions,'
            f' but not defined in the form or component inner fields')

    def _get_form_field_names(self) -> set[TFieldName]:
        form_field_names: set[TFieldName] = set()

        for row in self.rows:
            if isinstance(row, CustomizableRow):
                for row_item in row.items:
                    if isinstance(row_item, FormFieldMixin):
                        form_field_names.add(row_item.name)
            if isinstance(row, PreparedRow):
                for inner_field in row.Inner:
                    form_field_names.add(inner_field)
                if isinstance(row, FormFieldMixin):
                    form_field_names.add(row.name)

        return form_field_names

    def _get_conditional_fields(self) -> set[TFieldName]:
        fields_in_conditions: set[TFieldName] = set()

        for row in self.rows:
            if isinstance(row, CustomizableRow):
                for row_item in row.items:
                    if isinstance(row_item, DisplayConditionsMixin) and row_item.display_conditions is not None:
                        fields_in_conditions |= set(row_item.display_conditions.keys())
            elif isinstance(row, PreparedRow):
                if isinstance(row, DisplayConditionsMixin) and row.display_conditions is not None:
                    fields_in_conditions |= set(row.display_conditions.keys())

        return fields_in_conditions

    def _get_api_schema_fields(self) -> set[TFieldName]:
        api_schema_fields: set[TFieldName] = set()

        defined_api_schemas = [
            sch for sch in [
                self.api_schema.create,
                self.api_schema.edit,
                self.api_schema.check
            ] if sch is not None
        ] if self.api_schema is not None else []
        for api_schema in defined_api_schemas:
            api_schema_fields |= set(form_field_api_schema.name for form_field_api_schema in api_schema.items)
            for condition in (api_schema.conditions or []):
                api_schema_fields.add(condition.when.name)
                api_schema_fields |= set(conditional_action.selector.name for conditional_action in condition.then)

        return api_schema_fields


class ConnectionFormMode(Enum):
    create = 'create'
    edit = 'edit'


class ConnectionFormFactory:
    def __init__(self, mode: ConnectionFormMode, localizer: Localizer):
        self.mode = mode
        self._localizer = localizer

    def _is_current_tenant_with_org(self, tenant: Optional[TenantDef]) -> bool:
        return isinstance(tenant, TenantYCOrganization)

    def _filter_nulls(self, coll: list[Optional[Any]]) -> list[Any]:
        return [item for item in coll if item is not None]

    def _get_top_level_create_api_schema_items(self) -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(name=TopLevelFieldName.type_),
        ]

    def _get_top_level_check_api_schema_items(self) -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(name=TopLevelFieldName.type_),
        ] if self.mode == ConnectionFormMode.create else []

    @abc.abstractmethod
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef]
    ) -> ConnectionForm:
        """ Returns a form config built according to the specified settings """

        raise NotImplementedError
