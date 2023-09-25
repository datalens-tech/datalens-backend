from dl_constants.enums import OrderDirection
from dl_formula.shortcuts import n
from dl_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    CompiledOrderByFormulaInfo,
    CompiledQuery,
)
from dl_query_processing.enums import ExecutionLevel
from dl_query_processing.legacy_pipeline.subqueries.query_tools import copy_and_remap_query
from dl_query_processing_tests.unit.utils import joined_from_from_avatar_ids


def test_copy_and_remap_query():
    base_avatars = {}
    query = CompiledQuery(
        id="q",
        level_type=ExecutionLevel.source_db,
        select=[
            CompiledFormulaInfo(
                formula_obj=n.formula(n.func.SOMEFUNC(n.field("old_1"))),
                alias="res_1",
                avatar_ids={"ava_1"},
            )
        ],
        group_by=[],
        order_by=[
            CompiledOrderByFormulaInfo(
                formula_obj=n.formula(n.func.SOMEFUNC(n.field("old_2"))),
                alias="res_2",
                avatar_ids={"ava_1"},
                direction=OrderDirection.desc,
            )
        ],
        filters=[],
        join_on=[],
        limit=None,
        offset=None,
        joined_from=joined_from_from_avatar_ids(
            from_ids=["ava_1"],
            base_avatars=base_avatars,
            cols_by_query={"ava_1": ["old_1", "old_2"]},
        ),
    )
    field_name_map = {"old_1": "new_1", "old_2": "new_2"}
    remapped_query, remapped_aliases = copy_and_remap_query(
        query,
        id="new_q",
        field_name_map=field_name_map,
        avatar_map={"ava_1": "ava_2"},
    )
    assert remapped_aliases == {"res_1": "new_q_0", "res_2": "new_q_1"}
    assert remapped_query == CompiledQuery(
        id="new_q",
        level_type=ExecutionLevel.source_db,
        select=[
            CompiledFormulaInfo(
                formula_obj=n.formula(n.func.SOMEFUNC(n.field("new_1"))),
                alias="new_q_0",
                avatar_ids={"ava_2"},
            )
        ],
        group_by=[],
        order_by=[
            CompiledOrderByFormulaInfo(
                formula_obj=n.formula(n.func.SOMEFUNC(n.field("new_2"))),
                alias="new_q_1",
                avatar_ids={"ava_2"},
                direction=OrderDirection.desc,
            )
        ],
        filters=[],
        join_on=[],
        limit=None,
        offset=None,
        joined_from=joined_from_from_avatar_ids(
            from_ids=["ava_2"],
            base_avatars=base_avatars,
            cols_by_query={"ava_2": ["new_1", "new_2"]},
        ),
    )
