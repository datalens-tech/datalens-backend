from __future__ import annotations

import abc
from typing import Optional

import attr

from dl_core.components.dependencies.avatar_tree_base import AvatarTreeResolverBase
from dl_core.components.dependencies.field_avatar_base import FieldAvatarDependencyManagerBase
from dl_core.components.dependencies.field_deep_base import FieldDeepInterDependencyManagerBase
from dl_core.components.dependencies.field_shallow_base import FieldShallowInterDependencyManagerBase
from dl_core.components.dependencies.relation_avatar_base import RelationAvatarDependencyManagerBase


@attr.s
class ComponentDependencyManagerFactoryBase(abc.ABC):
    _field_shallow_inter_dependency_manager: Optional[FieldShallowInterDependencyManagerBase] = attr.ib(
        kw_only=True, default=None
    )
    _field_deep_inter_dependency_manager: Optional[FieldDeepInterDependencyManagerBase] = attr.ib(
        kw_only=True, default=None
    )
    _field_avatar_dependency_manager: Optional[FieldAvatarDependencyManagerBase] = attr.ib(kw_only=True, default=None)
    _relation_avatar_dependency_manager: Optional[RelationAvatarDependencyManagerBase] = attr.ib(
        kw_only=True, default=None
    )
    _avatar_tree_resolver: Optional[AvatarTreeResolverBase] = attr.ib(kw_only=True, default=None)

    @abc.abstractmethod
    def _make_field_shallow_inter_dependency_manager(self) -> FieldShallowInterDependencyManagerBase:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_field_deep_inter_dependency_manager(self) -> FieldDeepInterDependencyManagerBase:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_field_avatar_dependency_manager(self) -> FieldAvatarDependencyManagerBase:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_relation_avatar_dependency_manager(self) -> RelationAvatarDependencyManagerBase:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_avatar_tree_resolver(self) -> AvatarTreeResolverBase:
        raise NotImplementedError

    def get_field_shallow_inter_dependency_manager(self) -> FieldShallowInterDependencyManagerBase:
        if self._field_shallow_inter_dependency_manager is None:
            self._field_shallow_inter_dependency_manager = self._make_field_shallow_inter_dependency_manager()
        assert self._field_shallow_inter_dependency_manager is not None
        return self._field_shallow_inter_dependency_manager

    def get_field_deep_inter_dependency_manager(self) -> FieldDeepInterDependencyManagerBase:
        if self._field_deep_inter_dependency_manager is None:
            self._field_deep_inter_dependency_manager = self._make_field_deep_inter_dependency_manager()
        assert self._field_deep_inter_dependency_manager is not None
        return self._field_deep_inter_dependency_manager

    def get_field_avatar_dependency_manager(self) -> FieldAvatarDependencyManagerBase:
        if self._field_avatar_dependency_manager is None:
            self._field_avatar_dependency_manager = self._make_field_avatar_dependency_manager()
        assert self._field_avatar_dependency_manager is not None
        return self._field_avatar_dependency_manager

    def get_relation_avatar_dependency_manager(self) -> RelationAvatarDependencyManagerBase:
        if self._relation_avatar_dependency_manager is None:
            self._relation_avatar_dependency_manager = self._make_relation_avatar_dependency_manager()
        assert self._relation_avatar_dependency_manager is not None
        return self._relation_avatar_dependency_manager

    def get_avatar_tree_resolver(self) -> AvatarTreeResolverBase:
        if self._avatar_tree_resolver is None:
            self._avatar_tree_resolver = self._make_avatar_tree_resolver()
        assert self._avatar_tree_resolver is not None
        return self._avatar_tree_resolver
