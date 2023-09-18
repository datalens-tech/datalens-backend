from typing import Sequence, Optional

import attr

from dl_constants.enums import FieldType
from bi_external_api.converter.converter_exc import MalformedEntryConfig
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    datasets,
)


def get_malformed_ext_unsuitable_field(
        bad_field_msg: str,
        field: Optional[charts.ChartField],
) -> MalformedEntryConfig:
    field_str: str

    if field is None:
        field_str = "None"
    else:
        field_str = f"id={field.guid} title={field.title!r} type={field.type!r}"

    return MalformedEntryConfig(f"{bad_field_msg}: {field_str}")


@attr.s(frozen=True, auto_attribs=True)
class IntVisPack:
    vis: Optional[charts.Visualization]
    js: Optional[str]
    sort: Sequence[charts.ChartFieldSort]
    colors: Sequence[charts.ChartField]
    colors_config: Optional[charts.ColorConfig]
    shapes: Sequence[charts.ChartField]
    shapes_config: Optional[charts.ShapeConfig]

    @property
    def vis_strict(self) -> charts.Visualization:
        vis = self.vis
        assert vis is not None
        return vis

    def get_single_color_field(self) -> Optional[charts.ChartField]:
        if len(self.colors) == 0:
            return None
        if len(self.colors) == 1:
            return self.colors[0]
        raise MalformedEntryConfig("Visualization contains multiple color fields")

    def get_single_shape_field(self) -> Optional[charts.ChartField]:
        if len(self.shapes) == 0:
            return None
        if len(self.shapes) == 1:
            return self.shapes[0]
        raise MalformedEntryConfig("Visualization contains multiple shape fields")


def convert_field_type_dataset_to_chart(rs_field_type: FieldType) -> charts.DatasetFieldType:
    return {
        FieldType.DIMENSION: charts.DatasetFieldType.DIMENSION,
        FieldType.MEASURE: charts.DatasetFieldType.MEASURE,
    }[rs_field_type]


class ChartActionConverter:
    @staticmethod
    def convert_actions_add_field_chart_to_dataset(
            chart_action: charts.ChartActionFieldAdd
    ) -> tuple[str, datasets.ActionFieldAdd]:
        field_kwargs = attr.asdict(chart_action.field, recurse=False)
        dataset_id = field_kwargs.pop("datasetId")

        return dataset_id, datasets.ActionFieldAdd(
            field=datasets.ResultSchemaField(**field_kwargs)
        )

    @staticmethod
    def convert_action_add_field_dataset_to_chart(
            dataset_add_field_action: datasets.ActionFieldAdd,
            *,
            dataset_id: str,
    ) -> charts.ChartActionFieldAdd:
        return charts.ChartActionFieldAdd(
            field=charts.ChartActionField(
                datasetId=dataset_id,
                **attr.asdict(dataset_add_field_action.field, recurse=False),
            )
        )


class FieldShapeConverter:
    @classmethod
    def int_to_ext(cls, str_val: str) -> ext.FieldShape:
        return ext.FieldShape(str_val)

    @classmethod
    def ext_to_int(cls, val: ext.FieldShape) -> str:
        str_val = val.value
        assert isinstance(str_val, str), f"Unexpected type value in ext.FieldShape: {val!r}"
        return str_val
