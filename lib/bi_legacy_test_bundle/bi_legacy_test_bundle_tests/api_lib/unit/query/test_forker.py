from __future__ import annotations

from bi_constants.enums import JoinType

from bi_formula.shortcuts import n

from bi_query_processing.enums import ExecutionLevel
from bi_query_processing.compilation.primitives import (
    CompiledFormulaInfo, CompiledJoinOnFormulaInfo, CompiledQuery,
    FromColumn, AvatarFromObject,
)
from bi_query_processing.legacy_pipeline.separation.primitives import CompiledMultiLevelQuery, CompiledLevel
from bi_query_processing.legacy_pipeline.subqueries.forker import QueryForker

from bi_legacy_test_bundle_tests.api_lib.unit.query.utils import joined_from_from_avatar_ids


def test_forking_with_explicit_dimension_condition_list():
    base_avatars = {
        'a1': AvatarFromObject(
            id='a1', avatar_id='a1', source_id='s1', alias='a1',
            columns=(
                FromColumn(id='dim_1', name='dim_1'),
                FromColumn(id='dim_2', name='dim_2'),
                FromColumn(id='val_1', name='val_1'),
            )
        ),
    }
    compiled_multi_query = CompiledMultiLevelQuery(
        levels=[
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q1',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1')),
                                alias='dim_1_1',
                                original_field_id='dim_1',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2')),
                                alias='dim_2_1',
                                original_field_id='dim_2',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.func.SUM(n.field('val_1'))),
                                alias='val_1_1',
                                original_field_id='val_1',
                                avatar_ids={'a1'},
                            ),
                        ],
                        group_by=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1')),
                                alias='dim_1_1',
                                original_field_id='dim_1',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2')),
                                alias='dim_2_1',
                                original_field_id='dim_2',
                                avatar_ids={'a1'},
                            ),
                        ],
                        order_by=[],
                        filters=[],
                        join_on=[],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['a1'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                        limit=None,
                        offset=None,
                    ),
                ],
            ),
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q2',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1_1')),
                                alias='dim_1_1_1',
                                original_field_id='dim_1',
                                avatar_ids={'q1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2_1')),
                                alias='dim_2_1_1',
                                original_field_id='dim_2',
                                avatar_ids={'q1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.fork(
                                    result_expr=n.field('val_1_1'),
                                    joining=n.joining(
                                        conditions=[
                                            n.self_condition(
                                                expr=n.field('dim_1_1'),
                                            ),
                                            n.bin_condition(
                                                expr=n.field('dim_2_1'),
                                                fork_expr=n.func.SOMEFUNC(n.field('dim_2_1')),
                                            ),
                                        ],
                                    ),
                                    lod=n.fixed(n.field('dim_1_1'), n.field('dim_2_1')),
                                )),
                                alias='val_1_1_1',
                                original_field_id='val_1',
                                avatar_ids={'q1'},
                            ),
                        ],
                        group_by=[],
                        order_by=[],
                        filters=[],
                        join_on=[],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['q1'], base_avatars=base_avatars,
                            cols_by_query={'q1': ['dim_1', 'dim_2', 'val_1']},
                        ),
                        limit=None,
                        offset=None,
                    ),
                ],
            ),
        ],
    )
    forker = QueryForker(verbose_logging=True)
    forked_compiled_multi_query = forker.scan_and_fork_multi_query(
        compiled_multi_query=compiled_multi_query,
    )

    expected_forked_multi_query = CompiledMultiLevelQuery(
        levels=[
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q1',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1')),
                                alias='dim_1_1',
                                original_field_id='dim_1',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2')),
                                alias='dim_2_1',
                                original_field_id='dim_2',
                                avatar_ids={'a1'},
                            ),
                        ],
                        group_by=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1')),
                                alias='dim_1_1',
                                original_field_id='dim_1',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2')),
                                alias='dim_2_1',
                                original_field_id='dim_2',
                                avatar_ids={'a1'},
                            ),
                        ],
                        order_by=[],
                        filters=[],
                        join_on=[],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['a1'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                        limit=None,
                        offset=None,
                    ),
                    CompiledQuery(
                        id='q1_f0',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1')),
                                alias='q1_f0_0',
                                original_field_id='dim_1',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2')),
                                alias='q1_f0_1',
                                original_field_id='dim_2',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.func.SUM(n.field('val_1'))),
                                alias='q1_f0_2',
                                original_field_id='val_1',
                                avatar_ids={'a1'},
                            ),
                        ],
                        group_by=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1')),
                                alias='q1_f0_0',
                                original_field_id='dim_1',
                                avatar_ids={'a1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2')),
                                alias='q1_f0_1',
                                original_field_id='dim_2',
                                avatar_ids={'a1'},
                            ),
                        ],
                        order_by=[],
                        filters=[],
                        join_on=[],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['a1'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                        limit=None,
                        offset=None,
                    ),
                ],
            ),
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q2',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_1_1')),
                                alias='dim_1_1_1',
                                original_field_id='dim_1',
                                avatar_ids={'q1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('dim_2_1')),
                                alias='dim_2_1_1',
                                original_field_id='dim_2',
                                avatar_ids={'q1'},
                            ),
                            CompiledFormulaInfo(
                                formula_obj=n.formula(n.field('q1_f0_2')),
                                alias='val_1_1_1',
                                original_field_id='val_1',
                                avatar_ids={'q1', 'q1_f0'},
                            ),
                        ],
                        group_by=[],
                        order_by=[],
                        filters=[],
                        join_on=[
                            CompiledJoinOnFormulaInfo(
                                formula_obj=n.formula(
                                    n.binary(
                                        name='and',
                                        left=n.binary(
                                            name='_==',
                                            left=n.field('dim_1_1'),
                                            right=n.field('q1_f0_0'),
                                        ),
                                        right=n.binary(
                                            name='_==',
                                            left=n.field('dim_2_1'),
                                            right=n.func.SOMEFUNC(n.field('q1_f0_1')),
                                        ),
                                    ),
                                ),
                                alias=None,
                                original_field_id=None,
                                avatar_ids={'q1', 'q1_f0'},
                                left_id='q1',
                                right_id='q1_f0',
                                join_type=JoinType.left,
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['q1', 'q1_f0'], base_avatars=base_avatars,
                            cols_by_query={
                                'q1': ['dim_1_1', 'dim_2_1', 'val_1_1'],
                                'q1_f0': ['q1_f0_0', 'q1_f0_1', 'q1_f0_2'],
                            },
                        ),
                        limit=None,
                        offset=None,
                    ),
                ],
            ),
        ],
    )
    assert forked_compiled_multi_query == expected_forked_multi_query
