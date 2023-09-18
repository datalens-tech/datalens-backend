from dl_constants.enums import FieldRole, WhereClauseOperation

from dl_query_processing.base_specs.dimensions import DimensionValueSpec
from dl_api_lib.query.formalization.raw_specs import (
    RawQuerySpecUnion, RawSelectFieldSpec, TitleFieldRef,
    RawRowRoleSpec, RawTreeRoleSpec,
)
from dl_query_processing.legend.field_legend import FilterRoleSpec
from dl_api_lib.query.formalization.legend_formalizer import ResultLegendFormalizer

from bi_legacy_test_bundle_tests.api_lib.utils import make_dataset_with_tree


def test_tree_legend_formalization(api_v1, clickhouse_db, default_sync_usm, connection_id):
    db = clickhouse_db
    api_ds = make_dataset_with_tree(db=db, api_v1=api_v1, connection_id=connection_id)
    dataset = default_sync_usm.get_by_id(entry_id=api_ds.id)
    raw_query_spec_union = RawQuerySpecUnion(
        select_specs=[
            RawSelectFieldSpec(
                ref=TitleFieldRef(title='const_value'),
                role_spec=RawRowRoleSpec(role=FieldRole.row),
                legend_item_id=0,
            ),
            RawSelectFieldSpec(
                ref=TitleFieldRef(title='tree_str_value'),
                role_spec=RawTreeRoleSpec(
                    role=FieldRole.tree,
                    prefix='["abc"]', level=2,
                    dimension_values=[
                        DimensionValueSpec(legend_item_id=0, value='0'),
                    ],
                ),
                legend_item_id=1,
            ),
        ],
    )
    legend_formalizer = ResultLegendFormalizer(dataset=dataset)
    legend = legend_formalizer.make_legend(raw_query_spec_union=raw_query_spec_union)
    filter_items = [
        item for item in legend.items
        if isinstance(item.role_spec, FilterRoleSpec)
    ]
    filter_role_specs = [
        item.role_spec for item in legend.items
        if isinstance(item.role_spec, FilterRoleSpec)
    ]
    assert len(filter_role_specs) == 3
    assert filter_items[0].id == api_ds.find_field('tree_str_value').id
    assert filter_role_specs[0].operation == WhereClauseOperation.LENGTE
    assert filter_role_specs[0].values == [2]
    assert filter_items[1].id == api_ds.find_field('tree_str_value').id
    assert filter_role_specs[1].operation == WhereClauseOperation.STARTSWITH
    assert filter_role_specs[1].values == [['abc']]
    assert filter_items[2].id == api_ds.find_field('const_value').id
    assert filter_role_specs[2].operation == WhereClauseOperation.EQ
    assert filter_role_specs[2].operation == WhereClauseOperation.EQ
    assert filter_role_specs[2].values == ['0']
