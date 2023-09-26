import re
from typing import (
    ClassVar,
    Optional,
    Sequence,
)

from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.converter_exc import (
    ConstraintViolationError,
    DatasetFieldNotFound,
    MissingGuidFormula,
    NotSupportedYet,
)
from bi_external_api.converter.converter_exc_composer import ConversionErrHandlingContext
from bi_external_api.converter.literal_to_str import DefaultValueConverterExtStringValue
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import datasets
from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
    UserDataType,
)


class DatasetFieldConverter:
    @classmethod
    def convert_agg_ext_to_int(cls, agg: ext.Aggregation) -> AggregationFunction:
        return AggregationFunction[agg.name]

    @classmethod
    def convert_agg_int_to_ext(cls, agg: AggregationFunction) -> ext.Aggregation:
        return ext.Aggregation[agg.name]

    @classmethod
    def convert_data_type_ext_to_int(cls, data_type: ext.FieldType) -> UserDataType:
        return UserDataType[data_type.name]

    @classmethod
    def convert_data_type_int_to_ext(cls, data_type: UserDataType) -> ext.FieldType:
        return ext.FieldType[data_type.name]

    @classmethod
    def _validate_title(cls, field: ext.DatasetField) -> None:
        if len(field.title) > 35:
            raise ConstraintViolationError("Title exceeds max length of 35 characters")

    _id_re = re.compile(r"[a-zA-Z0-9_]+")

    @classmethod
    def _validate_id(cls, field: ext.DatasetField) -> None:
        if not cls._id_re.match(field.id):
            raise ConstraintViolationError("Got invalid ID for a field, allowed values: [a-zA-Z0-9_]+")

    @classmethod
    def validate_field(cls, field_def: ext.DatasetField) -> None:
        for fn_validation, name in [
            (cls._validate_title, "title"),
            (cls._validate_id, "id"),
        ]:
            with ConversionErrHandlingContext(current_path=["attrs"]).cm() as err_hdr:
                with err_hdr.postpone_error_with_path(name):
                    fn_validation(field_def)

    FIELD_RE: ClassVar[re.Pattern] = re.compile(r"\[([^]]*)\]")

    # Temporary workaround: BI-4542
    # Should be removed after BI-4543 resolution
    @classmethod
    def validate_id_formula_field(cls, field_def: ext.DatasetField, existing_ids: set[str]) -> None:
        cs = field_def.calc_spec
        if not isinstance(cs, ext.IDFormulaCS):
            return

        id_formula = cs.formula

        ids_in_formula = set(cls.FIELD_RE.findall(id_formula))

        no_existing_ids = ids_in_formula - existing_ids

        if no_existing_ids:
            raise DatasetFieldNotFound(
                f"next fields in ID formula not found in dataset: {','.join(sorted(no_existing_ids))}"
            )

    @classmethod
    def convert_single_ext_field_to_int(
        cls,
        field_def: ext.DatasetField,
    ) -> datasets.ResultSchemaField:
        calc_mode: CalcMode
        formula: str
        guid_formula: Optional[str]
        avatar_id: Optional[str]
        field_name: str
        default_value: Optional[datasets.DefaultValue]

        calc_spec = field_def.calc_spec

        cls.validate_field(field_def)

        cast = cls.convert_data_type_ext_to_int(field_def.cast)

        if isinstance(calc_spec, ext.FormulaCS):
            calc_mode = CalcMode.formula
            formula = calc_spec.formula
            guid_formula = None
            avatar_id = None
            # TODO FIX: BI-2945 Check if empty string is ok
            field_name = ""
            default_value = None
        elif isinstance(calc_spec, ext.IDFormulaCS):
            calc_mode = CalcMode.formula
            formula = ""
            guid_formula = calc_spec.formula
            avatar_id = None
            # TODO FIX: BI-2945 Check if empty string is ok
            field_name = ""
            default_value = None
        elif isinstance(calc_spec, ext.DirectCS):
            calc_mode = CalcMode.direct
            formula = ""
            guid_formula = None
            avatar_id = calc_spec.strict_avatar_id
            field_name = calc_spec.field_name
            default_value = None
        elif isinstance(calc_spec, ext.ParameterCS):
            calc_mode = CalcMode.parameter
            formula = ""
            guid_formula = None
            avatar_id = None
            # TODO FIX: BI-2945 Check if empty string is ok
            field_name = ""
            default_value = DefaultValueConverterExtStringValue.convert_ext_to_int(
                bi_type=cast,
                ext_value=calc_spec.default_value,
            )
        else:
            raise AssertionError("Got unexpected field type")

        return datasets.ResultSchemaField(
            guid=field_def.id,
            title=field_def.title,
            description=field_def.description or "",  # In internal API we does not accept null-description
            hidden=field_def.hidden,
            calc_mode=calc_mode,
            formula=formula,
            guid_formula=guid_formula,
            source=field_name,
            avatar_id=avatar_id,
            aggregation=cls.convert_agg_ext_to_int(field_def.strict_aggregation),
            cast=cls.convert_data_type_ext_to_int(field_def.cast),
            default_value=default_value,
        )

    @classmethod
    def convert_ext_fields_to_actions(cls, ext_fields: Sequence[ext.DatasetField]) -> list[datasets.Action]:
        internal_fields: list[datasets.ResultSchemaField] = []
        all_field_ids = {ef.id for ef in ext_fields}

        with ConversionErrHandlingContext().cm() as err_hdr:
            for ext_field in ext_fields:
                with err_hdr.postpone_error_with_path(str(ext_field.id)):
                    internal_fields.append(cls.convert_single_ext_field_to_int(ext_field))
                    # Temporary workaround: BI-4542
                    # Should be removed after BI-4543 resolution
                    cls.validate_id_formula_field(ext_field, all_field_ids)

        return [datasets.ActionFieldAdd(field=field, order_index=idx) for idx, field in enumerate(internal_fields)]

    @classmethod
    def convert_int_field_to_ext_field(
        cls,
        int_field: datasets.ResultSchemaField,
        converter_ctx: ConverterContext,
    ) -> ext.DatasetField:
        calc_spec: ext.CalcSpec

        if int_field.calc_mode == CalcMode.direct:
            assert int_field.source is not None
            calc_spec = ext.DirectCS(
                avatar_id=int_field.avatar_id,
                field_name=int_field.source,
            )
        elif int_field.calc_mode == CalcMode.formula:
            if converter_ctx.use_id_formula:
                if not int_field.guid_formula:
                    # todo: maybe pull this check up and ensures guid formula all the time
                    raise MissingGuidFormula(f"Formula {int_field.title} missing guid.")

                calc_spec = ext.IDFormulaCS(formula=int_field.guid_formula)
            else:
                calc_spec = ext.FormulaCS(
                    formula=int_field.formula or "",
                )
        elif int_field.calc_mode == CalcMode.parameter:
            default_param_value = int_field.default_value
            assert default_param_value is not None

            calc_spec = ext.ParameterCS(
                default_value=DefaultValueConverterExtStringValue.convert_int_to_ext(default_param_value)
            )
        else:
            raise NotSupportedYet(f"Calc mode {int_field.calc_mode}")

        return ext.DatasetField(
            id=int_field.guid,
            title=int_field.title,
            description=int_field.description or None,  # To be None if empty string
            hidden=int_field.hidden,
            cast=cls.convert_data_type_int_to_ext(int_field.cast),
            aggregation=cls.convert_agg_int_to_ext(int_field.aggregation),
            calc_spec=calc_spec,
        )
