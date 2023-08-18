from bi_constants.enums import FieldRole, PivotRole

from bi_api_lib.query.formalization.raw_specs import (
    RawQuerySpecUnion, RawSelectFieldSpec, TitleFieldRef, RawTemplateRoleSpec, RawRoleSpec, PlaceholderRef,
)
from bi_api_lib.query.formalization.raw_pivot_specs import (
    RawPivotSpec, RawPivotLegendItem, RawPivotMeasureRoleSpec, RawDimensionPivotRoleSpec,
)
from bi_api_lib.request_model.data import PivotDataRequestModel
from bi_api_lib.request_model.normalization.drm_normalizer_pivot import PivotRequestModelNormalizer


def test_simple_totals():
    original_drm = PivotDataRequestModel(
        raw_query_spec_union=RawQuerySpecUnion(
            select_specs=[
                RawSelectFieldSpec(ref=TitleFieldRef(title='First Dim'), legend_item_id=0),
                RawSelectFieldSpec(ref=TitleFieldRef(title='Second Dim'), legend_item_id=1),
                RawSelectFieldSpec(ref=TitleFieldRef(title='First Measure'), legend_item_id=2),
            ],
        ),
        pivot=RawPivotSpec(
            structure=[
                RawPivotLegendItem(
                    legend_item_ids=[0],
                    role_spec=RawDimensionPivotRoleSpec(role=PivotRole.pivot_row),
                ),
                RawPivotLegendItem(
                    legend_item_ids=[1],
                    role_spec=RawDimensionPivotRoleSpec(role=PivotRole.pivot_column),
                ),
                RawPivotLegendItem(
                    legend_item_ids=[2],
                    role_spec=RawPivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                ),
            ],
            totals=None,
            with_totals=True,
        ),
    )
    normalizer = PivotRequestModelNormalizer()
    normalized_drm = normalizer.normalize_drm(original_drm)

    expected_drm = PivotDataRequestModel(
        raw_query_spec_union=RawQuerySpecUnion(
            select_specs=[
                RawSelectFieldSpec(ref=TitleFieldRef(title='First Dim'), legend_item_id=0, block_id=0),
                RawSelectFieldSpec(ref=TitleFieldRef(title='Second Dim'), legend_item_id=1, block_id=0),
                RawSelectFieldSpec(ref=TitleFieldRef(title='First Measure'), legend_item_id=2, block_id=0),

                RawSelectFieldSpec(
                    ref=PlaceholderRef(),
                    role_spec=RawTemplateRoleSpec(role=FieldRole.template, template=''),
                    legend_item_id=3, block_id=1,
                ),
                RawSelectFieldSpec(ref=TitleFieldRef(title='Second Dim'), legend_item_id=4, block_id=1),
                RawSelectFieldSpec(
                    ref=TitleFieldRef(title='First Measure'),
                    role_spec=RawRoleSpec(role=FieldRole.total),
                    legend_item_id=5, block_id=1
                ),

                RawSelectFieldSpec(
                    ref=PlaceholderRef(),
                    role_spec=RawTemplateRoleSpec(role=FieldRole.template, template=''),
                    legend_item_id=6, block_id=2,
                ),
                RawSelectFieldSpec(ref=TitleFieldRef(title='First Dim'), legend_item_id=7, block_id=2),
                RawSelectFieldSpec(
                    ref=TitleFieldRef(title='First Measure'),
                    role_spec=RawRoleSpec(role=FieldRole.total),
                    legend_item_id=8, block_id=2,
                ),

                RawSelectFieldSpec(
                    ref=PlaceholderRef(),
                    role_spec=RawTemplateRoleSpec(role=FieldRole.template, template=''),
                    legend_item_id=9, block_id=3,
                ),
                RawSelectFieldSpec(
                    ref=PlaceholderRef(),
                    role_spec=RawTemplateRoleSpec(role=FieldRole.template, template=''),
                    legend_item_id=10, block_id=3,
                ),
                RawSelectFieldSpec(
                    ref=TitleFieldRef(title='First Measure'),
                    role_spec=RawRoleSpec(role=FieldRole.total),
                    legend_item_id=11, block_id=3,
                ),
            ],
        ),
        pivot=RawPivotSpec(
            structure=[
                RawPivotLegendItem(
                    legend_item_ids=[1, 4, 6, 10],
                    role_spec=RawDimensionPivotRoleSpec(role=PivotRole.pivot_column),
                ),
                RawPivotLegendItem(
                    legend_item_ids=[0, 3, 7, 9],
                    role_spec=RawDimensionPivotRoleSpec(role=PivotRole.pivot_row),
                ),
                RawPivotLegendItem(
                    legend_item_ids=[2, 5, 8, 11],
                    role_spec=RawPivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                ),
            ],
            totals=None,
            with_totals=None,
        )
    )

    assert normalized_drm == expected_drm
