from bi_legacy_test_bundle_tests.api_lib.utils import make_dataset_with_tree
from dl_api_lib.query.formalization.query_formalizer import SimpleQuerySpecFormalizer
from dl_constants.enums import (
    FieldRole,
    FieldType,
    UserDataType,
)
from dl_query_processing.compilation.specs import ArrayPrefixSelectWrapperSpec
from dl_query_processing.enums import SelectValueType
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.legend.field_legend import (
    FieldObjSpec,
    Legend,
    LegendItem,
    RowRoleSpec,
    TreeRoleSpec,
)


def test_tree_in_query_formalizer(api_v1, clickhouse_db, default_sync_usm, connection_id):
    us_manager = default_sync_usm
    db = clickhouse_db
    api_ds = make_dataset_with_tree(db=db, api_v1=api_v1, connection_id=connection_id)
    dataset = us_manager.get_by_id(entry_id=api_ds.id)
    field_int = api_ds.find_field("int_value")
    field_tree = api_ds.find_field("tree_str_value")
    block_spec = BlockSpec(
        block_id=0,
        parent_block_id=None,
        legend_item_ids=[0, 1],
        legend=Legend(
            items=[
                LegendItem(
                    legend_item_id=0,
                    data_type=UserDataType.integer,
                    obj=FieldObjSpec(id=field_int.id, title=field_int.title),
                    field_type=FieldType.DIMENSION,
                    role_spec=RowRoleSpec(role=FieldRole.row),
                ),
                LegendItem(
                    legend_item_id=1,
                    data_type=UserDataType.tree_str,
                    obj=FieldObjSpec(id=field_tree.id, title=field_tree.title),
                    field_type=FieldType.DIMENSION,
                    role_spec=TreeRoleSpec(
                        role=FieldRole.tree,
                        level=2,
                        prefix=["abc"],
                        dimension_values=[],
                    ),
                ),
            ],
        ),
    )
    query_formalizer = SimpleQuerySpecFormalizer(dataset=dataset, us_entry_buffer=us_manager.get_entry_buffer())
    query_spec = query_formalizer.make_query_spec(block_spec=block_spec)

    assert len(query_spec.select_specs) == 2
    select_spec = query_spec.select_specs[1]
    assert isinstance(select_spec.wrapper, ArrayPrefixSelectWrapperSpec)
    assert select_spec.wrapper.type == SelectValueType.array_prefix
    assert select_spec.wrapper.length == 2

    assert len(query_spec.group_by_specs) == 2
    gb_spec = query_spec.group_by_specs[1]
    assert isinstance(gb_spec.wrapper, ArrayPrefixSelectWrapperSpec)
    assert gb_spec.wrapper.type == SelectValueType.array_prefix
    assert gb_spec.wrapper.length == 2
