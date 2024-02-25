from dl_core.components.dependencies.factory import ComponentDependencyManagerFactory
from dl_core_tests.db.base import DefaultCoreTestClass


class TestAvatarTreeResolution(DefaultCoreTestClass):
    def test_resolve_req_avatars_with_gaps(
        self,
        empty_saved_dataset,
        sync_us_manager,
        conn_default_service_registry,
        dataset_builder_factory,
    ):
        dataset = empty_saved_dataset
        dataset_builder = dataset_builder_factory.get_dataset_builder(dataset=dataset)

        dsrc_1_proxy = dataset_builder.add_data_source()
        dsrc_2_proxy = dataset_builder.add_data_source()
        dsrc_3_proxy = dataset_builder.add_data_source()

        avatar_1_proxy = dsrc_1_proxy.add_avatar()
        avatar_2_proxy = dsrc_2_proxy.add_avatar()
        avatar_3_proxy = dsrc_3_proxy.add_avatar()

        relation_1_2_proxy = avatar_1_proxy.add_relation_simple_eq(
            right=avatar_2_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        relation_2_3_proxy = avatar_2_proxy.add_relation_simple_eq(
            right=avatar_3_proxy, left_col_name="int_value", right_col_name="int_value"
        )

        dep_mgr_factory = ComponentDependencyManagerFactory(dataset=dataset)
        tree_resolver = dep_mgr_factory.get_avatar_tree_resolver()
        root_avatar, required_avatar_ids, required_relation_ids = tree_resolver.expand_required_avatar_ids(
            required_avatar_ids={
                avatar_1_proxy.avatar_id,
                avatar_3_proxy.avatar_id,
            },  # leave a gap in place of avatar_2_id
        )

        assert root_avatar == avatar_1_proxy.avatar_id
        assert required_avatar_ids == {avatar_1_proxy.avatar_id, avatar_2_proxy.avatar_id, avatar_3_proxy.avatar_id}
        assert required_relation_ids == {relation_1_2_proxy.relation_id, relation_2_3_proxy.relation_id}

    def test_resolve_avatars_with_required_relations(
        self,
        empty_saved_dataset,
        sync_us_manager,
        conn_default_service_registry,
        dataset_builder_factory,
    ):
        dataset = empty_saved_dataset
        dataset_builder = dataset_builder_factory.get_dataset_builder(dataset=dataset)

        dsrc_1_proxy = dataset_builder.add_data_source()
        dsrc_2_proxy = dataset_builder.add_data_source()
        dsrc_3_proxy = dataset_builder.add_data_source()
        dsrc_4_proxy = dataset_builder.add_data_source()
        dsrc_5_proxy = dataset_builder.add_data_source()
        dsrc_6_proxy = dataset_builder.add_data_source()

        avatar_1_proxy = dsrc_1_proxy.add_avatar()
        avatar_2_proxy = dsrc_2_proxy.add_avatar()
        avatar_3_proxy = dsrc_3_proxy.add_avatar()
        avatar_4_proxy = dsrc_4_proxy.add_avatar()
        avatar_5_proxy = dsrc_5_proxy.add_avatar()
        avatar_6_proxy = dsrc_6_proxy.add_avatar()
        avatar_7_proxy = dsrc_6_proxy.add_avatar()
        avatar_8_proxy = dsrc_5_proxy.add_avatar()
        avatar_9_proxy = dsrc_6_proxy.add_avatar()
        avatar_10_proxy = dsrc_6_proxy.add_avatar()

        relation_1_2_proxy = avatar_1_proxy.add_relation_simple_eq(
            right=avatar_2_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        relation_2_3_proxy = avatar_2_proxy.add_relation_simple_eq(
            right=avatar_3_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        relation_3_4_proxy = avatar_3_proxy.add_relation_simple_eq(
            right=avatar_4_proxy, left_col_name="int_value", right_col_name="int_value", required=True
        )
        relation_3_5_proxy = avatar_3_proxy.add_relation_simple_eq(
            right=avatar_5_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        avatar_4_proxy.add_relation_simple_eq(
            right=avatar_6_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        relation_4_7_proxy = avatar_4_proxy.add_relation_simple_eq(
            right=avatar_7_proxy, left_col_name="int_value", right_col_name="int_value", required=True
        )
        avatar_4_proxy.add_relation_simple_eq(
            right=avatar_10_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        relation_2_8_proxy = avatar_2_proxy.add_relation_simple_eq(
            right=avatar_8_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        relation_8_9_proxy = avatar_8_proxy.add_relation_simple_eq(
            right=avatar_9_proxy, left_col_name="int_value", right_col_name="int_value", required=True
        )

        """
        Relations looks like this
        
              8 -r- 9     10
              |           |
        1 --- 2 --- 3 -r- 4 --- 6
                    |     |
                    5     * --r- 7
                    
        --- just relation
        -r- required by user relation
                    
        nodes required by arguments is 5 and 1, so in result expected all nodes between 5 and 1 (it's 3, 2) and
        all required by user nodes attached to them (directly and indirectly) - it is nodes 1, 2, 3, 4, 5, 7, 8, 9
        """

        dep_mgr_factory = ComponentDependencyManagerFactory(dataset=dataset)
        tree_resolver = dep_mgr_factory.get_avatar_tree_resolver()
        root_avatar, required_avatar_ids, required_relation_ids = tree_resolver.expand_required_avatar_ids(
            required_avatar_ids={
                avatar_1_proxy.avatar_id,
                avatar_5_proxy.avatar_id,
            },
        )

        assert root_avatar == avatar_1_proxy.avatar_id
        assert required_avatar_ids == {
            avatar_1_proxy.avatar_id,
            avatar_2_proxy.avatar_id,
            avatar_3_proxy.avatar_id,
            avatar_4_proxy.avatar_id,
            avatar_5_proxy.avatar_id,
            avatar_7_proxy.avatar_id,
            avatar_8_proxy.avatar_id,
            avatar_9_proxy.avatar_id,
        }
        assert required_relation_ids == {
            relation_1_2_proxy.relation_id,
            relation_2_3_proxy.relation_id,
            relation_3_4_proxy.relation_id,
            relation_3_5_proxy.relation_id,
            relation_4_7_proxy.relation_id,
            relation_2_8_proxy.relation_id,
            relation_8_9_proxy.relation_id,
        }
