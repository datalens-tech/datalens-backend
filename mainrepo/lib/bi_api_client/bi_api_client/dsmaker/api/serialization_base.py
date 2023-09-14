from __future__ import annotations

from functools import singledispatchmethod
from typing import (
    List,
    Union,
)

import attr
from marshmallow import fields as ma_fields

from bi_api_client.dsmaker.api.schemas.base import DefaultSchema
from bi_api_client.dsmaker.api.schemas.dataset import (
    DatasetContentInternalSchema,
    ObligatoryFilterSchema,
    ParameterValueConstraintSchema,
    WhereClauseSchema,
)
from bi_api_client.dsmaker.primitives import (
    Action,
    ApiProxyObject,
    AvatarRelation,
    Container,
    Dataset,
    DataSource,
    DirectJoinPart,
    FormulaJoinPart,
    JoinPart,
    ObligatoryFilter,
    ResultField,
    ResultFieldJoinPart,
    SourceAvatar,
    UpdateAction,
)
from bi_constants.enums import JoinConditionType


class ObligatoryFilterUpdateSchema(DefaultSchema[ObligatoryFilter]):
    TARGET_CLS = ObligatoryFilter

    id = ma_fields.String()
    default_filters = ma_fields.List(ma_fields.Nested(WhereClauseSchema))
    valid = ma_fields.Boolean()


@attr.s
class BaseApiV1SerializationAdapter:
    # TODO: switch to marshmallow

    def dump_item(self, item: ApiProxyObject, action: Action = None) -> dict:
        """
        Dump item data for API request. Return dict

        :param item: the object to dump
        :param action: dump object data for given update action
        :return:
        """
        return self._dump_item(item, action=action)

    @singledispatchmethod
    def _dump_item(self, item: ApiProxyObject, action: Action) -> dict:
        raise TypeError(f"Unknown item type: {type(item)}")

    @_dump_item.register(DataSource)
    def dump_data_source(self, item: DataSource, action: Action) -> dict:
        if action == Action.delete:
            return dict(id=item.id)
        else:
            return dict(
                id=item.id,
                connection_id=item.connection_id,
                source_type=item.source_type.name,
                title=item.title,
                raw_schema=[
                    dict(
                        name=col.name,
                        title=col.title,
                        user_type=col.user_type.name,
                        native_type=col.native_type,
                        description=col.description,
                        nullable=col.nullable,
                        has_auto_aggregation=col.has_auto_aggregation,
                        lock_aggregation=col.lock_aggregation,
                    )
                    for col in item.raw_schema
                ]
                if item.raw_schema is not None
                else None,
                index_info_set=None if item.index_info_set is None else list(item.index_info_set),
                parameters=item.parameters,
                managed_by=item.managed_by.name,
                valid=item.valid,
            )

    @_dump_item.register(SourceAvatar)
    def dump_source_avatar(self, item: SourceAvatar, action: Action) -> dict:
        if action == Action.delete:
            return dict(id=item.id)
        else:
            return dict(
                id=item.id,
                source_id=item.source_id,
                title=item.title,
                is_root=item.is_root,
                managed_by=item.managed_by.name,
            )

    @_dump_item.register(AvatarRelation)
    def dump_avatar_relation(self, item: AvatarRelation, action: Action) -> dict:
        if action == Action.delete:
            return dict(id=item.id)
        else:

            def dump_condition_part(part: JoinPart) -> dict:
                data = dict(calc_mode=part.calc_mode.name)
                if isinstance(part, DirectJoinPart):
                    data["source"] = part.source
                elif isinstance(part, FormulaJoinPart):
                    data["formula"] = part.formula
                elif isinstance(part, ResultFieldJoinPart):
                    data["field_id"] = part.field_id
                return data

            return dict(
                id=item.id,
                left_avatar_id=item.left_avatar_id,
                right_avatar_id=item.right_avatar_id,
                conditions=[
                    dict(
                        type=JoinConditionType.binary.name,
                        left=dump_condition_part(condition.left_part),
                        right=dump_condition_part(condition.right_part),
                        operator=condition.operator.name,
                    )
                    for condition in item.conditions
                ],
                join_type=item.join_type.name,
                managed_by=item.managed_by.name,
            )

    @_dump_item.register(ResultField)
    def dump_field(self, item: ResultField, action: Action) -> dict:
        if action == Action.delete:
            return dict(guid=item.id)
        if action == Action.add:
            return dict(
                guid=item.id,
                title=item.title,
                calc_mode=item.calc_mode.name if item.calc_mode is not None else None,
                aggregation=item.aggregation.name if item.aggregation is not None else None,
                type=item.type.name if item.type is not None else None,
                source=item.source,
                hidden=item.hidden,
                description=item.description,
                formula=item.formula,
                initial_data_type=item.initial_data_type.name if item.initial_data_type is not None else None,
                cast=item.cast.name if item.cast is not None else None,
                data_type=item.data_type.name if item.data_type is not None else None,
                valid=item.valid,
                has_auto_aggregation=item.has_auto_aggregation,
                lock_aggregation=item.lock_aggregation,
                avatar_id=item.avatar_id,
                managed_by=item.managed_by.name,
                default_value=item.default_value.value if item.default_value is not None else None,
                value_constraint=ParameterValueConstraintSchema().dump(item.value_constraint)
                if item.value_constraint is not None
                else None,
            )
        else:
            return dict(guid=item.id)

    @_dump_item.register(ObligatoryFilter)
    def dump_obligatory_filter(self, item: ObligatoryFilter, action: Action = None) -> dict:
        if action == Action.delete:
            return dict(id=item.id)
        if action == Action.add:
            return ObligatoryFilterSchema().dump(item)
        else:
            return ObligatoryFilterUpdateSchema().dump(item)

    def _strip_implicit_updates_from_dataset(self, dataset: Dataset) -> Dataset:
        return dataset.clone(
            sources=Container({name: item for name, item in dataset.sources.items() if item.created_}),
            source_avatars=Container({name: item for name, item in dataset.source_avatars.items() if item.created_}),
            avatar_relations=Container(
                {name: item for name, item in dataset.avatar_relations.items() if item.created_}
            ),
            result_schema=Container({name: item for name, item in dataset.result_schema.items() if item.created_}),
        )

    def dump_dataset(self, item: Dataset) -> dict:
        stripped_dataset = self._strip_implicit_updates_from_dataset(dataset=item)
        dataset_data = DatasetContentInternalSchema().dump(stripped_dataset)
        return {"dataset": dataset_data}

    def generate_implicit_updates(self, dataset: Dataset) -> List[UpdateAction]:
        updates = []
        for dsrc in dataset.sources:
            if not dsrc.created_:
                dsrc.prepare()
                updates.append(dsrc.add())
        for avatar in dataset.source_avatars:
            if not avatar.created_:
                avatar.prepare()
                updates.append(avatar.add())
        for relation in dataset.avatar_relations:
            if not relation.created_:
                relation.prepare()
                updates.append(relation.add())
        for field in dataset.result_schema:
            if not field.created_:
                field.prepare()
                updates.append(field.add())
        return updates

    @staticmethod
    def _get_action_postfix(item: ApiProxyObject) -> str:
        return {
            DataSource: "source",
            SourceAvatar: "source_avatar",
            AvatarRelation: "avatar_relation",
            ResultField: "field",
            ObligatoryFilter: "obligatory_filter",
        }[type(item)]

    def dump_updates(self, updates: List[Union[UpdateAction, dict]] = None) -> List[dict]:
        result = []
        for update in updates or ():
            if isinstance(update, UpdateAction):
                action_postfix = self._get_action_postfix(update.item)
                update.item.prepare()
                item_data = dict(self.dump_item(update.item, action=update.action), **(update.custom_data or {}))
                item_data = {k: v for k, v in item_data.items() if v is not None}
                result.append(
                    {
                        "action": "{}_{}".format(update.action.name, action_postfix),
                        "order_index": update.order_index,
                        action_postfix: item_data,
                    }
                )
            elif isinstance(update, dict):
                # as raw dict; pass it on directly
                result.append(update)
            else:
                raise TypeError(type(update))

        return result
