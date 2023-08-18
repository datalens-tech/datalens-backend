from __future__ import annotations

from typing import Optional, Tuple

import attr
import shortuuid

from bi_constants.enums import AggregationFunction, BIType, FieldType, ManagedBy, BinaryJoinOperator

from bi_core.components.ids import AvatarId, FieldId
from bi_core.components.dependencies.primitives import FieldInterDependencyInfo
from bi_core.components.dependencies.field_shallow import FieldShallowInterDependencyManager
from bi_core.components.dependencies.field_deep import FieldDeepInterDependencyManager
from bi_core.components.dependencies.field_avatar import FieldAvatarDependencyManager
from bi_core.components.dependencies.relation_avatar import RelationAvatarDependencyManager
from bi_core.fields import BIField, ResultSchema, DirectCalculationSpec, FormulaCalculationSpec
from bi_core.multisource import AvatarRelation, BinaryCondition, ConditionPartDirect, ConditionPartResultField


@attr.s
class ShallowInfo:
    field_1_id: FieldId = attr.ib(factory=shortuuid.uuid)
    field_2_id: FieldId = attr.ib(factory=shortuuid.uuid)
    field_3_id: FieldId = attr.ib(factory=shortuuid.uuid)
    field_4_id: FieldId = attr.ib(factory=shortuuid.uuid)
    shallow: FieldShallowInterDependencyManager = attr.ib(
        factory=lambda: FieldShallowInterDependencyManager(
            inter_dep_info=FieldInterDependencyInfo()
        )
    )


def make_shallow() -> ShallowInfo:
    shallow_info = ShallowInfo()

    shallow_info.shallow.set_field_direct_references(
        dep_field_id=shallow_info.field_1_id,
        ref_field_ids=[shallow_info.field_2_id, shallow_info.field_3_id]
    )
    shallow_info.shallow.set_field_direct_references(
        dep_field_id=shallow_info.field_4_id,
        ref_field_ids=[shallow_info.field_1_id]
    )
    return shallow_info


def make_direct_field(field_id: FieldId, avatar_id: Optional[AvatarId]) -> BIField:
    return BIField.make(
        guid=field_id,
        title=shortuuid.uuid(),
        type=FieldType.DIMENSION,
        aggregation=AggregationFunction.none,
        initial_data_type=BIType.integer,
        cast=BIType.integer,
        data_type=BIType.integer,
        has_auto_aggregation=False,
        lock_aggregation=False,
        valid=True,
        managed_by=ManagedBy.user,
        calc_spec=DirectCalculationSpec(
            avatar_id=avatar_id,
            source=shortuuid.uuid(),
        ),
    )


def make_formula_field(field_id: FieldId) -> BIField:
    return BIField.make(
        guid=field_id,
        title=shortuuid.uuid(),
        type=FieldType.DIMENSION,
        aggregation=AggregationFunction.none,
        initial_data_type=BIType.integer,
        cast=BIType.integer,
        data_type=BIType.integer,
        has_auto_aggregation=False,
        lock_aggregation=False,
        valid=True,
        managed_by=ManagedBy.user,
        calc_spec=FormulaCalculationSpec(
            formula='...',
        ),
    )


def make_result_schema(shallow_info) -> Tuple[ResultSchema, AvatarId, AvatarId]:
    avatar_1_id = shortuuid.uuid()
    avatar_2_id = shortuuid.uuid()
    return (
        ResultSchema(fields=[
            make_formula_field(field_id=shallow_info.field_1_id),
            make_direct_field(field_id=shallow_info.field_2_id, avatar_id=avatar_1_id),
            make_direct_field(field_id=shallow_info.field_3_id, avatar_id=avatar_2_id),
            make_formula_field(field_id=shallow_info.field_4_id),
        ]),
        avatar_1_id,
        avatar_2_id,
    )


def test_shallow_manager():
    shallow_info = make_shallow()

    assert shallow_info.shallow.get_field_direct_references(
        dep_field_id=shallow_info.field_1_id
    ) == {
        shallow_info.field_2_id,
        shallow_info.field_3_id,
    }
    assert shallow_info.shallow.get_field_direct_references(
        dep_field_id=shallow_info.field_2_id
    ) == set()
    assert shallow_info.shallow.get_field_direct_references(
        dep_field_id=shallow_info.field_4_id
    ) == {
        shallow_info.field_1_id,
    }


def test_deep_manager():
    shallow_info = make_shallow()
    deep = FieldDeepInterDependencyManager(shallow=shallow_info.shallow)

    assert deep.get_field_deep_references(
        dep_field_id=shallow_info.field_1_id
    ) == {
        shallow_info.field_2_id,
        shallow_info.field_3_id,
    }
    assert deep.get_field_deep_references(
        dep_field_id=shallow_info.field_2_id
    ) == set()
    assert deep.get_field_deep_references(
        dep_field_id=shallow_info.field_4_id
    ) == {
        shallow_info.field_1_id,
        shallow_info.field_2_id,
        shallow_info.field_3_id,
    }


def test_field_avatar_manager():
    shallow_info = make_shallow()
    deep = FieldDeepInterDependencyManager(shallow=shallow_info.shallow)
    result_schema, avatar_1_id, avatar_2_id = make_result_schema(shallow_info=shallow_info)
    field_avatar_mgr = FieldAvatarDependencyManager(
        result_schema=result_schema,
        deep=deep,
    )

    assert field_avatar_mgr.get_field_avatar_references(
        dep_field_id=shallow_info.field_1_id
    ) == {
        avatar_1_id,
        avatar_2_id,
    }
    assert field_avatar_mgr.get_field_avatar_references(
        dep_field_id=shallow_info.field_2_id
    ) == {
        avatar_1_id,
    }
    assert field_avatar_mgr.get_field_avatar_references(
        dep_field_id=shallow_info.field_4_id
    ) == {
        avatar_1_id,
        avatar_2_id,
    }


def test_relation_avatar_manager():
    shallow_info = make_shallow()
    deep = FieldDeepInterDependencyManager(shallow=shallow_info.shallow)
    result_schema, avatar_1_id, avatar_2_id = make_result_schema(shallow_info=shallow_info)
    avatar_3_id = shortuuid.uuid()
    field_avatar_mgr = FieldAvatarDependencyManager(
        result_schema=result_schema,
        deep=deep,
    )
    relation_1_id = shortuuid.uuid()
    relation_2_id = shortuuid.uuid()
    avatar_relations = [
        AvatarRelation(
            id=relation_1_id,
            left_avatar_id=avatar_1_id,
            right_avatar_id=avatar_2_id,
            conditions=[BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='col_1'),
                right_part=ConditionPartDirect(source='col_2'),
            )]
        ),
        AvatarRelation(
            id=relation_2_id,
            left_avatar_id=avatar_1_id,
            right_avatar_id=avatar_3_id,
            conditions=[BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartResultField(field_id=shallow_info.field_1_id),
                right_part=ConditionPartDirect(source='col_2'),
            )]
        ),
    ]
    relation_avatar_mgr = RelationAvatarDependencyManager(
        avatar_relations=avatar_relations,
        field_avatar_mgr=field_avatar_mgr,
    )

    assert relation_avatar_mgr.get_relation_avatar_references(
        relation_id=relation_1_id
    ) == {
        avatar_1_id,
        avatar_2_id,
    }
    assert relation_avatar_mgr.get_relation_avatar_references(
        relation_id=relation_2_id
    ) == {
        avatar_1_id,
        avatar_2_id,
        avatar_3_id,
    }
