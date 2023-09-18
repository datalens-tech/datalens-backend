from dl_constants.enums import JoinType, OrderDirection

import dl_formula.core.fork_nodes as formula_fork_nodes
from dl_formula.core.index import NodeHierarchyIndex
from dl_formula.shortcuts import n

from dl_query_processing.enums import ExecutionLevel, QueryPart
from dl_query_processing.compilation.primitives import (
    FromColumn, AvatarFromObject, SubqueryFromObject, JoinedFromObject,
    CompiledFormulaInfo, CompiledOrderByFormulaInfo, CompiledJoinOnFormulaInfo,
    CompiledQuery, CompiledMultiQuery,
)
from dl_query_processing.multi_query.splitters.mask_based import (
    MultiQuerySplitter, QuerySplitMask, AliasedFormulaSplitMask, AddFormulaInfo, SubqueryType,
)
from dl_query_processing.multi_query.splitters.prefiltered import PrefilteredFieldMultiQuerySplitter
from dl_query_processing.multi_query.mutators.splitter_based import SplitterMultiQueryMutator
from dl_query_processing.utils.name_gen import PrefixedIdGen


class MyMultiQuerySplitter(MultiQuerySplitter):
    def __init__(self):
        self._done_once = False

    def get_split_masks(
            self, query: CompiledQuery, expr_id_gen: PrefixedIdGen, query_id_gen: PrefixedIdGen,
    ) -> list[QuerySplitMask]:
        """
        A splitter that splits the `m_1` field between the `SUM` and `FUNC_2` calls
        and joins the sub-queries along using the `dim_2`/`custom_alias_2` dimension.
        """

        if self._done_once:
            return []

        self._done_once = True
        return [
            QuerySplitMask(
                subquery_type=SubqueryType.default,
                subquery_id=query_id_gen.get_id(),
                formula_split_masks=(
                    AliasedFormulaSplitMask(
                        alias='custom_alias_1',
                        query_part=QueryPart.select,
                        formula_list_idx=0,
                        outer_node_idx=NodeHierarchyIndex(indices=(0, 0)),
                        inner_node_idx=NodeHierarchyIndex(indices=(0, 0)),
                    ),
                    AliasedFormulaSplitMask(
                        alias='custom_alias_1',
                        query_part=QueryPart.order_by,
                        formula_list_idx=0,
                        outer_node_idx=NodeHierarchyIndex(indices=(0, 0)),
                        inner_node_idx=NodeHierarchyIndex(indices=(0, 0)),
                    ),
                ),
                filter_indices=frozenset(),
                join_type=JoinType.inner,
                joining_node=formula_fork_nodes.QueryForkJoiningWithList.make(
                    condition_list=[
                        formula_fork_nodes.SelfEqualityJoinCondition.make(n.field('custom_alias_2')),
                    ],
                ),
                add_formulas=(
                    AddFormulaInfo(
                        alias='custom_alias_2',
                        expr=n.func.FUNC_3(n.field('asd')),
                        from_ids=frozenset({'ava_1'}),
                        is_group_by=True,
                    ),
                ),
            )
        ]

    def mutate_cropped_query(self, query: CompiledQuery) -> CompiledQuery:
        return query.clone(group_by=[])


def test_query_splitter():
    original_query = CompiledMultiQuery(
        queries=[
            CompiledQuery(
                id='qq',
                level_type=ExecutionLevel.source_db,
                select=[
                    CompiledFormulaInfo(
                        alias='m_1',
                        formula_obj=n.formula(n.func.FUNC_2(n.func.AVG(n.field('qwe')))),
                        original_field_id='field_1',
                        avatar_ids={'ava_1'},
                    ),
                    CompiledFormulaInfo(
                        alias='dim_2',
                        formula_obj=n.formula(n.func.FUNC_3(n.field('asd'))),
                        original_field_id='field_2',
                        avatar_ids={'ava_1'},
                    ),
                    CompiledFormulaInfo(
                        alias='m_3',
                        formula_obj=n.formula(n.func.SUM(n.func.FUNC_4(n.field('zxc')))),
                        original_field_id='field_3',
                        avatar_ids={'ava_1'},
                    ),
                ],
                group_by=[
                    CompiledFormulaInfo(
                        alias='dim_2',
                        formula_obj=n.formula(n.func.FUNC_3(n.field('asd'))),
                        original_field_id='field_2',
                        avatar_ids={'ava_1'},
                    ),
                ],
                order_by=[
                    CompiledOrderByFormulaInfo(
                        alias='m_1',
                        formula_obj=n.formula(n.func.FUNC_2(n.func.AVG(n.field('qwe')))),
                        original_field_id='field_1',
                        avatar_ids={'ava_1'},
                        direction=OrderDirection.desc,
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='ava_1',
                    froms=[
                        AvatarFromObject(
                            id='ava_1', avatar_id='ava_1', source_id='src_1', alias='ava_1',
                            columns=(
                                FromColumn(id='qwe', name='qwe'),
                                FromColumn(id='asd', name='asd'),
                                FromColumn(id='zxc', name='zxc'),
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )
    mutator = SplitterMultiQueryMutator(splitters=[MyMultiQuerySplitter()])
    actual_result = mutator.mutate_multi_query(original_query)
    expected_result = CompiledMultiQuery(
        queries=[
            CompiledQuery(
                id='qq',
                level_type=ExecutionLevel.source_db,
                select=[
                    CompiledFormulaInfo(
                        formula_obj=n.formula(n.func.FUNC_2(n.field('custom_alias_1'))),
                        alias='m_1',
                        avatar_ids={'q_0'},
                        original_field_id='field_1',
                    ),
                    CompiledFormulaInfo(
                        formula_obj=n.formula(n.field('e_1')),
                        alias='dim_2',
                        avatar_ids={'q_1'},
                        original_field_id='field_2',
                    ),
                    CompiledFormulaInfo(
                        formula_obj=n.formula(n.field('e_0')),
                        alias='m_3',
                        avatar_ids={'q_1'},
                        original_field_id='field_3',
                    ),
                ],
                order_by=[
                    CompiledOrderByFormulaInfo(
                        formula_obj=n.formula(n.func.FUNC_2(n.field('custom_alias_1'))),
                        alias='m_1',
                        avatar_ids={'q_0'},
                        original_field_id='field_1',
                        direction=OrderDirection.desc,
                    ),
                ],
                join_on=[
                    CompiledJoinOnFormulaInfo(
                        alias=None,
                        formula_obj=n.formula(n.binary('_dneq', n.field('e_1'), n.field('custom_alias_2'))),
                        avatar_ids={'q_0', 'q_1'},
                        original_field_id=None,
                        left_id='q_1',
                        right_id='q_0',
                        join_type=JoinType.inner,
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='q_1',
                    froms=[
                        SubqueryFromObject(
                            id='q_1', alias='q_1', query_id='q_1',
                            columns=(
                                FromColumn(id='e_0', name='e_0'),
                                FromColumn(id='e_1', name='e_1'),
                            ),
                        ),
                        SubqueryFromObject(
                            id='q_0', alias='q_0', query_id='q_0',
                            columns=(
                                FromColumn(id='custom_alias_1', name='custom_alias_1'),
                                FromColumn(id='custom_alias_2', name='custom_alias_2'),
                            ),
                        ),
                    ],
                ),
            ),
            CompiledQuery(
                id='q_1',
                level_type=ExecutionLevel.source_db,
                select=[
                    CompiledFormulaInfo(
                        alias='e_0',
                        formula_obj=n.formula(n.func.SUM(n.func.FUNC_4(n.field('zxc')))),
                        original_field_id='field_3',
                        avatar_ids={'ava_1'},
                    ),
                    CompiledFormulaInfo(
                        formula_obj=n.formula(n.func.FUNC_3(n.field('asd'))),
                        alias='e_1',
                        avatar_ids={'ava_1'},
                        original_field_id=None,
                    ),
                ],
                group_by=[
                    CompiledFormulaInfo(
                        formula_obj=n.formula(n.func.FUNC_3(n.field('asd'))),
                        alias='e_1',
                        avatar_ids={'ava_1'},
                        original_field_id=None,
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='ava_1',
                    froms=[
                        AvatarFromObject(
                            id='ava_1', avatar_id='ava_1', source_id='src_1', alias='ava_1',
                            columns=(
                                FromColumn(id='qwe', name='qwe'),
                                FromColumn(id='asd', name='asd'),
                                FromColumn(id='zxc', name='zxc'),
                            ),
                        ),
                    ],
                ),
            ),
            CompiledQuery(
                id='q_0',
                level_type=ExecutionLevel.source_db,
                select=[
                    CompiledFormulaInfo(
                        alias='custom_alias_1',
                        formula_obj=n.formula(n.func.AVG(n.field('qwe'))),
                        avatar_ids={'ava_1'},
                        original_field_id='field_1',
                    ),
                    CompiledFormulaInfo(
                        formula_obj=n.formula(n.func.FUNC_3(n.field('asd'))),
                        alias='custom_alias_2',
                        avatar_ids={'ava_1'},
                        original_field_id=None,
                    ),
                ],
                group_by=[
                    CompiledFormulaInfo(
                        formula_obj=n.formula(n.func.FUNC_3(n.field('asd'))),
                        alias='custom_alias_2',
                        avatar_ids={'ava_1'},
                        original_field_id=None,
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='ava_1',
                    froms=[
                        AvatarFromObject(
                            id='ava_1', avatar_id='ava_1', source_id='src_1', alias='ava_1',
                            columns=(
                                FromColumn(id='qwe', name='qwe'),
                                FromColumn(id='asd', name='asd'),
                                FromColumn(id='zxc', name='zxc'),
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )
    assert expected_result == actual_result


def test_field_splitter():
    original_query = CompiledMultiQuery(
        queries=[
            CompiledQuery(
                id='qq',
                level_type=ExecutionLevel.source_db,
                select=[
                    CompiledFormulaInfo(
                        alias='f_1',
                        formula_obj=n.formula(n.func.FUNC_1(n.field('qwe'))),
                        original_field_id='field_1',
                        avatar_ids={'ava_1'},
                    ),
                    CompiledFormulaInfo(
                        alias='f_2',
                        formula_obj=n.formula(n.func.FUNC_2(n.field('asd'))),
                        original_field_id='field_2',
                        avatar_ids={'ava_1'},
                    ),
                    CompiledFormulaInfo(
                        alias='f_3',
                        formula_obj=n.formula(n.func.FUNC_3(n.field('zxc'))),
                        original_field_id='field_3',
                        avatar_ids={'ava_2'},
                    ),
                ],
                order_by=[
                    CompiledOrderByFormulaInfo(
                        alias='f_1',
                        formula_obj=n.formula(n.func.FUNC_1(n.field('qwe'))),
                        original_field_id='field_1',
                        avatar_ids={'ava_1'},
                        direction=OrderDirection.desc,
                    ),
                ],
                join_on=[
                    CompiledJoinOnFormulaInfo(
                        alias=None,
                        formula_obj=n.formula(n.binary('_dneq', n.field('asd'), n.field('zxc'))),
                        avatar_ids={'ava_1', 'ava_2'},
                        original_field_id=None,
                        left_id='ava_1',
                        right_id='ava_2',
                        join_type=JoinType.inner,
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='ava_1',
                    froms=[
                        AvatarFromObject(
                            id='ava_1', avatar_id='ava_1', source_id='src_1', alias='ava_1',
                            columns=(
                                FromColumn(id='qwe', name='qwe'),
                                FromColumn(id='asd', name='asd'),
                            ),
                        ),
                        AvatarFromObject(
                            id='ava_2', avatar_id='ava_2', source_id='src_2', alias='ava_2',
                            columns=(
                                FromColumn(id='zxc', name='zxc'),
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )
    mutator = SplitterMultiQueryMutator(
        splitters=[
            PrefilteredFieldMultiQuerySplitter(crop_to_level_type=ExecutionLevel.compeng)
        ],
    )
    actual_result = mutator.mutate_multi_query(original_query)
    expected_result = CompiledMultiQuery(
        queries=[
            CompiledQuery(
                id='qq',
                level_type=ExecutionLevel.compeng,
                select=[
                    CompiledFormulaInfo(
                        alias='f_1',
                        formula_obj=n.formula(n.func.FUNC_1(n.field('e_0'))),
                        original_field_id='field_1',
                        avatar_ids={'q_0'},
                    ),
                    CompiledFormulaInfo(
                        alias='f_2',
                        formula_obj=n.formula(n.func.FUNC_2(n.field('e_1'))),
                        original_field_id='field_2',
                        avatar_ids={'q_0'},
                    ),
                    CompiledFormulaInfo(
                        alias='f_3',
                        formula_obj=n.formula(n.func.FUNC_3(n.field('e_2'))),
                        original_field_id='field_3',
                        avatar_ids={'q_1'},
                    ),
                ],
                order_by=[
                    CompiledOrderByFormulaInfo(
                        alias='f_1',
                        formula_obj=n.formula(n.func.FUNC_1(n.field('e_0'))),
                        original_field_id='field_1',
                        avatar_ids={'q_0'},
                        direction=OrderDirection.desc,
                    ),
                ],
                join_on=[
                    CompiledJoinOnFormulaInfo(
                        alias=None,
                        formula_obj=n.formula(n.binary('_dneq', n.field('e_1'), n.field('e_2'))),
                        avatar_ids={'q_0', 'q_1'},
                        original_field_id=None,
                        left_id='q_0',
                        right_id='q_1',
                        join_type=JoinType.inner,
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='q_0',
                    froms=[
                        SubqueryFromObject(
                            id='q_0', query_id='q_0', alias='q_0',
                            columns=(
                                FromColumn(id='e_0', name='e_0'),
                                FromColumn(id='e_1', name='e_1'),
                            ),
                        ),
                        SubqueryFromObject(
                            id='q_1', query_id='q_1', alias='q_1',
                            columns=(
                                FromColumn(id='e_2', name='e_2'),
                            ),
                        ),
                    ],
                ),
            ),
            CompiledQuery(
                id='q_0',
                level_type=ExecutionLevel.source_db,
                select=[
                    CompiledFormulaInfo(
                        alias='e_0',
                        formula_obj=n.formula(n.field('qwe')),
                        original_field_id=None,
                        avatar_ids={'ava_1'},
                    ),
                    CompiledFormulaInfo(
                        alias='e_1',
                        formula_obj=n.formula(n.field('asd')),
                        original_field_id=None,
                        avatar_ids={'ava_1'},
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='ava_1',
                    froms=[
                        AvatarFromObject(
                            id='ava_1', avatar_id='ava_1', source_id='src_1', alias='ava_1',
                            columns=(
                                FromColumn(id='qwe', name='qwe'),
                                FromColumn(id='asd', name='asd'),
                            ),
                        ),
                    ],
                ),
            ),
            CompiledQuery(
                id='q_1',
                level_type=ExecutionLevel.source_db,
                select=[
                    CompiledFormulaInfo(
                        alias='e_2',
                        formula_obj=n.formula(n.field('zxc')),
                        original_field_id=None,
                        avatar_ids={'ava_2'},
                    ),
                ],
                joined_from=JoinedFromObject(
                    root_from_id='ava_2',
                    froms=[
                        AvatarFromObject(
                            id='ava_2', avatar_id='ava_2', source_id='src_2', alias='ava_2',
                            columns=(
                                FromColumn(id='zxc', name='zxc'),
                            ),
                        ),
                    ],
                ),
            ),
        ],
    )
    assert expected_result == actual_result
