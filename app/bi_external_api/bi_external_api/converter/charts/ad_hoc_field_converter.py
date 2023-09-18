import attr

from bi_external_api.converter.charts.utils import ChartActionConverter
from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.converter_exc_composer import ConversionErrHandlingContext
from bi_external_api.converter.dataset_field import DatasetFieldConverter
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    datasets,
)
from bi_external_api.domain.internal.datasets import ActionFieldAdd


@attr.s()
class AdHocFieldConverter:
    _wb_context: WorkbookContext = attr.ib()
    _converter_context: ConverterContext = attr.ib()

    def convert_ad_hoc_field_to_actions(self, ad_hoc_field: ext.AdHocField) -> charts.ChartActionFieldAdd:
        ds_instance = self._wb_context.resolve_dataset_by_name(ad_hoc_field.strict_dataset_name)

        with ConversionErrHandlingContext(current_path=[f"{ad_hoc_field.dataset_name}.ad_hoc_field"]).cm() as err_hdr:
            with err_hdr.postpone_error_with_path(ad_hoc_field.field.id):
                field = DatasetFieldConverter.convert_single_ext_field_to_int(ad_hoc_field.field)
                dataset_action = ActionFieldAdd(field=field, order_index=0)

        assert isinstance(dataset_action, datasets.ActionFieldAdd)

        return ChartActionConverter.convert_action_add_field_dataset_to_chart(
            dataset_action,
            dataset_id=ds_instance.summary.id,
        )

    def convert_action_to_ad_hoc_field(self, action: charts.ChartActionFieldAdd) -> ext.AdHocField:
        return ext.AdHocField(
            dataset_name=self._wb_context.resolve_dataset_by_id(action.field.datasetId).summary.name,
            field=DatasetFieldConverter.convert_int_field_to_ext_field(
                action.field,
                self._converter_context,
            ),
        )

    @classmethod
    def post_process_with_validation_results(
        cls,
        action: charts.ChartActionFieldAdd,
        field_after_validation: datasets.ResultSchemaFieldFull,
    ) -> charts.ChartActionFieldAdd:
        orig_field = action.field
        fixed_field: charts.ChartActionField

        if orig_field.formula and orig_field.guid_formula:
            return action
        elif orig_field.formula:
            assert orig_field.formula == field_after_validation.formula
            fixed_field = attr.evolve(orig_field, guid_formula=field_after_validation.guid_formula)
        elif orig_field.guid_formula:
            assert orig_field.guid_formula == field_after_validation.guid_formula
            fixed_field = attr.evolve(orig_field, formula=field_after_validation.formula)
        else:
            # After conversion GUID-formula is None
            fixed_field = attr.evolve(orig_field, guid_formula="")

        return attr.evolve(action, field=fixed_field)
