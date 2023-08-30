from bi_constants.enums import JoinType

from bi_formula.shortcuts import n

from bi_query_processing.enums import ExecutionLevel
from bi_query_processing.compilation.primitives import (
    CompiledQuery, CompiledFormulaInfo, CompiledJoinOnFormulaInfo,
    FromColumn, AvatarFromObject,
)
from bi_query_processing.legacy_pipeline.separation.primitives import CompiledMultiLevelQuery, CompiledLevel
from bi_query_processing.legacy_pipeline.subqueries.sanitizer import MultiQuerySanitizer

from bi_api_lib_tests.unit.query.utils import joined_from_from_avatar_ids


def test_simple_select_sanitizing():
    base_avatars = {
        'ava_0': AvatarFromObject(
            id='ava_0', avatar_id='ava_0', source_id='src_0', alias='ava_0',
            columns=(
                FromColumn(id='c_0', name='c_0'),
                FromColumn(id='c_1', name='c_1'),
            )
        ),
    }
    compiled_multi_query = CompiledMultiLevelQuery(
        levels=[
            # Lower level
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_0',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            # Used by higher level (in expressions q_1_0 and q_1_1)
                            CompiledFormulaInfo(
                                alias='q_0_0',
                                formula_obj=n.formula(n.func.MY_FUNC(n.field('c_0'))),
                                avatar_ids={'ava_0'},
                                original_field_id=None,
                            ),
                            # Not used by higher level
                            CompiledFormulaInfo(
                                alias='q_0_1',
                                formula_obj=n.formula(n.func.OTHER_FUNC(n.field('c_1'))),
                                avatar_ids={'ava_0'},
                                original_field_id=None,
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['ava_0'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                    ),
                ],
            ),
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_1',
                        level_type=ExecutionLevel.source_db,
                        select=[  # All top-level SELECTs are considered useful
                            CompiledFormulaInfo(
                                alias='q_1_0',
                                formula_obj=n.formula(n.func.CUSTOM_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_0',
                            ),
                            CompiledFormulaInfo(
                                alias='q_1_1',
                                formula_obj=n.formula(n.func.WHATEVER_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_1',
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['q_0'], base_avatars=base_avatars,
                            cols_by_query={'q_0': ['q_0_0', 'q_0_1']},
                        ),
                    ),
                ],
            ),
        ],
    )

    sanitizer = MultiQuerySanitizer()
    actual_sanitized_multi_query = sanitizer.sanitize_multi_query(compiled_multi_query)

    # Expected: q_0_1 is removed form the lower-level query select
    expected_sanitized_multi_query = CompiledMultiLevelQuery(
        levels=[
            # Lower level
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_0',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            CompiledFormulaInfo(
                                alias='q_0_0',
                                formula_obj=n.formula(n.func.MY_FUNC(n.field('c_0'))),
                                avatar_ids={'ava_0'},
                                original_field_id=None,
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['ava_0'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                    ),
                ],
            ),
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_1',
                        level_type=ExecutionLevel.source_db,
                        select=[  # All top-level SELECTs are considered useful
                            CompiledFormulaInfo(
                                alias='q_1_0',
                                formula_obj=n.formula(n.func.CUSTOM_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_0',
                            ),
                            CompiledFormulaInfo(
                                alias='q_1_1',
                                formula_obj=n.formula(n.func.WHATEVER_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_1',
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['q_0'], base_avatars=base_avatars,
                            cols_by_query={'q_0': ['q_0_0', 'q_0_1']},
                        ),
                    ),
                ],
            ),
        ],
    )
    assert actual_sanitized_multi_query == expected_sanitized_multi_query


def test_join_sanitizing():
    base_avatars = {
        'ava_0': AvatarFromObject(
            id='ava_0', avatar_id='ava_0', source_id='src_0', alias='ava_0',
            columns=(
                FromColumn(id='c_0', name='c_0'),
            )
        ),
        'ava_1': AvatarFromObject(
            id='ava_1', avatar_id='ava_1', source_id='src_1', alias='ava_1',
            columns=(
                FromColumn(id='c_1', name='c_1'),
            )
        ),
        'ava_3': AvatarFromObject(
            id='ava_3', avatar_id='ava_3', source_id='src_3', alias='ava_3',
            columns=(
                FromColumn(id='c_3', name='c_3'),
            )
        ),
    }
    compiled_multi_query = CompiledMultiLevelQuery(
        levels=[
            # Lower level
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_0',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            # Used by higher level (in expressions q_1_0 and q_1_1)
                            CompiledFormulaInfo(
                                alias='q_0_0',
                                formula_obj=n.formula(n.func.MY_FUNC(n.field('c_0'))),
                                avatar_ids={'ava_0'},
                                original_field_id=None,
                            ),
                            # Not used by higher level; from joined avatar
                            CompiledFormulaInfo(
                                alias='q_0_1',
                                formula_obj=n.formula(n.func.OTHER_FUNC(n.field('c_1'))),
                                avatar_ids={'ava_1'},
                                original_field_id=None,
                            ),
                        ],
                        join_on=[
                            # Used by higher level (in expressions q_1_0 and q_1_1)
                            CompiledJoinOnFormulaInfo(
                                alias=None,
                                formula_obj=n.formula(n.func(name='==')(n.field('c_0'), n.field('c_1'))),
                                avatar_ids={'ava_0', 'ava_1'},
                                left_id='ava_0', right_id='ava_1',
                                join_type=JoinType.inner,
                                original_field_id=None,
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['ava_0', 'ava_1'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                    ),
                    CompiledQuery(
                        id='q1_0',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            # Not used
                            CompiledFormulaInfo(
                                alias='q_0_3',
                                formula_obj=n.formula(n.func.MY_FUNC(n.field('c_3'))),
                                avatar_ids={'ava_3'},
                                original_field_id=None,
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['ava_3'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                    ),
                ],
            ),
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_1',
                        level_type=ExecutionLevel.source_db,
                        select=[  # All top-level SELECTs are considered useful
                            CompiledFormulaInfo(
                                alias='q_1_0',
                                formula_obj=n.formula(n.func.CUSTOM_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_0',
                            ),
                            CompiledFormulaInfo(
                                alias='q_1_1',
                                formula_obj=n.formula(n.func.WHATEVER_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_1',
                            ),
                        ],
                        join_on=[
                            # Used by higher level (in expressions q_1_0 and q_1_1)
                            CompiledJoinOnFormulaInfo(
                                alias=None,
                                formula_obj=n.formula(n.func(name='==')(n.field('q_0_0'), n.field('q_0_1'))),
                                avatar_ids={'q_0', 'q1_0'},
                                left_id='q_0', right_id='q1_0',
                                join_type=JoinType.inner,
                                original_field_id=None,
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['q_0', 'q1_0'], base_avatars=base_avatars,
                            cols_by_query={
                                'q_0': ['q_0_0', 'q_0_1'],
                                'q1_0': ['q_0_3'],
                            },
                        ),
                    ),
                ],
            ),
        ],
    )

    sanitizer = MultiQuerySanitizer()
    actual_sanitized_multi_query = sanitizer.sanitize_multi_query(compiled_multi_query)

    expected_sanitized_multi_query = CompiledMultiLevelQuery(
        levels=[
            # Lower level
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_0',
                        level_type=ExecutionLevel.source_db,
                        select=[
                            # Used by higher level (in expressions q_1_0 and q_1_1)
                            CompiledFormulaInfo(
                                alias='q_0_0',
                                formula_obj=n.formula(n.func.MY_FUNC(n.field('c_0'))),
                                avatar_ids={'ava_0'},
                                original_field_id=None,
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['ava_0'], base_avatars=base_avatars,
                            cols_by_query={},
                        ),
                    ),
                ],
            ),
            CompiledLevel(
                level_type=ExecutionLevel.source_db,
                queries=[
                    CompiledQuery(
                        id='q_1',
                        level_type=ExecutionLevel.source_db,
                        select=[  # All top-level SELECTs are considered useful
                            CompiledFormulaInfo(
                                alias='q_1_0',
                                formula_obj=n.formula(n.func.CUSTOM_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_0',
                            ),
                            CompiledFormulaInfo(
                                alias='q_1_1',
                                formula_obj=n.formula(n.func.WHATEVER_FUNC(n.field('q_0_0'))),
                                avatar_ids={'q_0'},
                                original_field_id='f_1',
                            ),
                        ],
                        joined_from=joined_from_from_avatar_ids(
                            from_ids=['q_0'], base_avatars=base_avatars,
                            cols_by_query={
                                'q_0': ['q_0_0', 'q_0_1'],
                                'q1_0': ['q_0_3'],
                            },
                        ),
                    ),
                ],
            ),
        ],
    )
    assert actual_sanitized_multi_query == expected_sanitized_multi_query
