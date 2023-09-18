from __future__ import annotations

import attr

from dl_core.components.dependencies.avatar_tree import AvatarTreeResolver
from dl_core.components.dependencies.factory_base import ComponentDependencyManagerFactoryBase
from dl_core.components.dependencies.field_avatar import FieldAvatarDependencyManager
from dl_core.components.dependencies.field_deep import FieldDeepInterDependencyManager
from dl_core.components.dependencies.field_shallow import FieldShallowInterDependencyManager
from dl_core.components.dependencies.relation_avatar import RelationAvatarDependencyManager
from dl_core.us_dataset import Dataset


@attr.s
class ComponentDependencyManagerFactory(ComponentDependencyManagerFactoryBase):  # noqa
    _dataset: Dataset = attr.ib(kw_only=True)

    def _make_field_shallow_inter_dependency_manager(self) -> FieldShallowInterDependencyManager:
        return FieldShallowInterDependencyManager(
            inter_dep_info=self._dataset.data.result_schema_aux.inter_dependencies,
        )

    def _make_field_deep_inter_dependency_manager(self) -> FieldDeepInterDependencyManager:
        return FieldDeepInterDependencyManager(
            shallow=self.get_field_shallow_inter_dependency_manager(),
        )

    def _make_field_avatar_dependency_manager(self) -> FieldAvatarDependencyManager:
        return FieldAvatarDependencyManager(
            result_schema=self._dataset.result_schema,
            deep=self.get_field_deep_inter_dependency_manager(),
        )

    def _make_relation_avatar_dependency_manager(self) -> RelationAvatarDependencyManager:
        return RelationAvatarDependencyManager(
            avatar_relations=self._dataset.data.avatar_relations,
            field_avatar_mgr=self.get_field_avatar_dependency_manager(),
        )

    def _make_avatar_tree_resolver(self) -> AvatarTreeResolver:
        return AvatarTreeResolver(
            dataset=self._dataset, relation_avatar_dep_mgr=self.get_relation_avatar_dependency_manager()
        )
