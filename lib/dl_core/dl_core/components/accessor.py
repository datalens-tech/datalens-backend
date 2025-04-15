import attr

from dl_constants.enums import CalcMode
from dl_core.base_models import ObligatoryFilter
from dl_core.data_source_spec.collection import DataSourceCollectionSpec
import dl_core.exc as exc
from dl_core.multisource import (
    AvatarRelation,
    SourceAvatar,
)
from dl_core.us_dataset import Dataset
from dl_model_tools.typed_values import BIValue
from dl_query_processing.compilation.specs import ParameterValueSpec


@attr.s(frozen=True)
class DatasetComponentAccessor:
    _dataset: Dataset = attr.ib(kw_only=True)

    def get_avatar_list(self, source_id: str | None = None) -> list[SourceAvatar]:
        """
        Return list of source avatars
        :param source_id: limit result to avatars with this source ID
        :return: list of ``SourceAvatar`` objects
        """
        return [
            avatar for avatar in self._dataset.data.source_avatars if (not source_id or avatar.source_id == source_id)
        ]

    def has_avatar(self, avatar_id: str) -> bool:
        return self.get_avatar_opt(avatar_id=avatar_id) is not None

    def get_avatar_opt(self, avatar_id: str) -> SourceAvatar | None:
        for avatar in self._dataset.data.source_avatars:
            if avatar.id == avatar_id:
                return avatar
        return None

    def get_avatar_strict(self, avatar_id: str) -> SourceAvatar:
        avatar = self.get_avatar_opt(avatar_id=avatar_id)
        if avatar is None:
            raise exc.SourceAvatarNotFound(f"Unknown source avatar: {avatar_id}")
        return avatar

    def get_root_avatar_opt(self) -> SourceAvatar | None:
        for avatar in self._dataset.data.source_avatars:
            if avatar.is_root:
                return avatar
        return None

    def get_root_avatar_strict(self) -> SourceAvatar:
        root_avatar = self.get_root_avatar_opt()
        if root_avatar is None:
            raise exc.DatasetConfigurationError("No avatars found")
        return root_avatar

    def get_data_source_id_list(self) -> list[str]:
        return [dsrc_coll_spec.id for dsrc_coll_spec in self.get_data_source_coll_spec_list()]

    def get_data_source_coll_spec_list(self) -> list[DataSourceCollectionSpec]:
        return [dsrc_coll_spec for dsrc_coll_spec in self._dataset.data.source_collections]

    def get_data_source_coll_spec_opt(self, source_id: str) -> DataSourceCollectionSpec | None:
        for dsrc_coll_spec in self._dataset.data.source_collections:
            if dsrc_coll_spec.id == source_id:
                return dsrc_coll_spec
        return None

    def get_data_source_coll_spec_strict(self, source_id: str) -> DataSourceCollectionSpec:
        dsrc_coll_spec = self.get_data_source_coll_spec_opt(source_id=source_id)
        if dsrc_coll_spec is None:
            raise exc.DataSourceNotFound(f"Unknown data source: {source_id}")
        return dsrc_coll_spec

    def get_avatar_relation_list(
        self, left_avatar_id: str | None = None, right_avatar_id: str | None = None
    ) -> list[AvatarRelation]:
        """
        Return list of relations
        :param left_avatar_id: limit result to relations with this left ID
        :param right_avatar_id: limit result to relations with this left ID
        :return: list of ``AvatarRelation`` objects
        """
        result = []
        for relation in self._dataset.data.avatar_relations:
            if (not left_avatar_id or relation.left_avatar_id == left_avatar_id) and (
                not right_avatar_id or relation.right_avatar_id == right_avatar_id
            ):
                result.append(relation)
        return result

    def has_avatar_relation(
        self,
        relation_id: str | None = None,
        left_avatar_id: str | None = None,
        right_avatar_id: str | None = None,
    ) -> bool:
        return (
            self.get_avatar_relation_opt(
                relation_id=relation_id,
                left_avatar_id=left_avatar_id,
                right_avatar_id=right_avatar_id,
            )
            is not None
        )

    def get_avatar_relation_opt(
        self,
        relation_id: str | None = None,
        left_avatar_id: str | None = None,
        right_avatar_id: str | None = None,
    ) -> AvatarRelation | None:
        for relation in self._dataset.data.avatar_relations:
            if (
                relation_id is not None
                and relation.id == relation_id
                or (relation.left_avatar_id == left_avatar_id and relation.right_avatar_id == right_avatar_id)
            ):
                return relation
        return None

    def get_avatar_relation_strict(
        self,
        relation_id: str | None = None,
        left_avatar_id: str | None = None,
        right_avatar_id: str | None = None,
    ) -> AvatarRelation:
        relation = self.get_avatar_relation_opt(
            relation_id=relation_id,
            left_avatar_id=left_avatar_id,
            right_avatar_id=right_avatar_id,
        )
        if relation is None:
            raise exc.AvatarRelationNotFound(f"Unknown avatar relation: {relation_id, left_avatar_id, right_avatar_id}")
        return relation

    def get_obligatory_filter_list(self) -> list[ObligatoryFilter]:
        return self._dataset.data.obligatory_filters

    def has_obligatory_filter(
        self,
        obfilter_id: str | None = None,
        field_guid: str | None = None,
    ) -> bool:
        return (
            self.get_obligatory_filter_opt(
                obfilter_id=obfilter_id,
                field_guid=field_guid,
            )
            is not None
        )

    def get_obligatory_filter_opt(
        self,
        obfilter_id: str | None = None,
        field_guid: str | None = None,
    ) -> ObligatoryFilter | None:
        if obfilter_id is not None:
            for filter_object in self._dataset.data.obligatory_filters:
                if filter_object.id == obfilter_id:
                    return filter_object
        elif field_guid is not None:
            for filter_object in self._dataset.data.obligatory_filters:
                if filter_object.field_guid == field_guid:
                    return filter_object
        else:
            raise ValueError('"id" and "field_guid" parameters can not be None together')

        return None

    def get_obligatory_filter_strict(
        self,
        obfilter_id: str | None = None,
        field_guid: str | None = None,
    ) -> ObligatoryFilter:
        obfilter = self.get_obligatory_filter_opt(obfilter_id=obfilter_id, field_guid=field_guid)
        if obfilter is None:
            raise exc.ObligatoryFilterNotFound(f"Unknown obligatory filter: {obfilter_id, field_guid}")
        return obfilter

    def get_parameter_values(self) -> dict[str, BIValue]:
        result = {
            field.title: field.default_value
            for field in self._dataset.result_schema.fields
            if field.calc_mode == CalcMode.parameter and field.default_value is not None and field.template_enabled
        }

        return result

    def get_parameter_values_from_specs(
        self,
        parameter_value_specs: list[ParameterValueSpec],
    ) -> dict[str, BIValue]:
        result = {}
        for parameter_value_spec in parameter_value_specs:
            field = self._dataset.result_schema.by_guid(parameter_value_spec.field_id)
            if field.template_enabled:
                assert field.default_value is not None
                result[field.title] = attr.evolve(field.default_value, value=parameter_value_spec.value)
        return result

    def get_template_enabled(self) -> bool:
        return self._dataset.template_enabled
