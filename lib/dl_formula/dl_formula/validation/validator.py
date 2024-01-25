from __future__ import annotations

import abc
from contextlib import contextmanager
from typing import (
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
)

import dl_formula.core.exc as exc
from dl_formula.core.message_ctx import FormulaErrorCtx
import dl_formula.core.nodes as nodes
from dl_formula.validation.env import (
    ErrInfo,
    ValidationEnvironment,
)


class Validator:
    def __init__(self, env: ValidationEnvironment, collect_errors: bool):
        self._env = env
        self._collect_errors = collect_errors

    @contextmanager
    def handle_error(self, checker_cls: Type["Checker"], node: nodes.FormulaItem):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        try:
            yield
        except exc.ValidationError as err:
            info = self.get_from_cache(checker_cls=checker_cls, node=node)
            if info is not None and info.exception is not None:
                # There already is an exception with errors for this node. Combine them
                errors = []
                errors.extend(info.exception.errors)
                errors.extend(err.errors)
                err = exc.ValidationError(*errors)

            self._env.generic_cache_valid[checker_cls].add(node, value=ErrInfo(is_error=True, exception=err))

            if not self._collect_errors:
                raise err

    def get_from_cache(self, checker_cls: Type["Checker"], node: nodes.FormulaItem) -> Optional[ErrInfo]:
        return self._env.generic_cache_valid[checker_cls].get(node)

    def mark_as_ok_in_cache(self, checker_cls: Type["Checker"], node: nodes.FormulaItem) -> None:
        return self._env.generic_cache_valid[checker_cls].add(node, value=ErrInfo(is_error=False, exception=None))

    def proxy_for(self, checker: "Checker") -> "ValidatorProxy":
        return ValidatorProxy(validator=self, checker_cls=type(checker))

    def get_all_errors(self, node: nodes.FormulaItem) -> List[FormulaErrorCtx]:
        errors = []  # type: ignore  # TODO: fix
        for error_cache in self._env.generic_cache_valid.values():
            info = error_cache.get(node)
            if info is None:
                continue
            if info.is_error and info.exception is not None:  # type: ignore  # TODO: fix
                errors.extend(info.exception.errors)  # type: ignore  # TODO: fix
        return errors


class ValidatorProxy:
    def __init__(self, validator: Validator, checker_cls: Type["Checker"]):
        self._validator = validator
        self._checker_cls = checker_cls

    @contextmanager
    def handle_error(self, node: nodes.FormulaItem):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        with self._validator.handle_error(checker_cls=self._checker_cls, node=node):
            yield

    def get_from_cache(self, node: nodes.FormulaItem) -> Optional[ErrInfo]:
        return self._validator.get_from_cache(checker_cls=self._checker_cls, node=node)

    def mark_as_ok_in_cache(self, node: nodes.FormulaItem) -> None:
        self._validator.mark_as_ok_in_cache(checker_cls=self._checker_cls, node=node)


class Checker(abc.ABC):
    def check_node(
        self,
        validator: ValidatorProxy,
        node: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> None:
        """
        Check the validity of a node recursively an either exit or raise an error.
        First check whether the node has already been checked by this ``Checker``.
        Is so, then skip the check and add errors to the list if any are available in the cache.

        This method does not implement the check itself - that is delegated to ``self.perform_node_check(...)``.
        """

        from_cache = validator.get_from_cache(node)
        if from_cache is not None:
            if from_cache.exception is not None:
                raise from_cache.exception
            return

        self.perform_node_check(validator=validator, node=node, parent_stack=parent_stack)

        from_cache = validator.get_from_cache(node)
        if from_cache is not None and from_cache.exception:
            raise from_cache.exception
        else:
            # no errors in this node and its children
            validator.mark_as_ok_in_cache(node)

    @abc.abstractmethod
    def perform_node_check(
        self,
        validator: ValidatorProxy,
        node: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> None:
        """Method checks the validity of a node recursively and either exits or raises an error"""

        raise NotImplementedError


def validate(
    node: nodes.FormulaItem,
    env: ValidationEnvironment,
    checkers: Iterable[Checker] = (),
    collect_errors: bool = False,
) -> None:
    """
    Apply multiple checkers to a formula item tree.
    Either exit quietly or raise a ``ValidationError``.
    """

    validator = Validator(env=env, collect_errors=collect_errors)
    for checker in checkers:
        try:
            checker.check_node(validator=validator.proxy_for(checker), node=node, parent_stack=())
        except exc.ValidationError:
            if not collect_errors:
                raise

    errors = validator.get_all_errors(node)
    if errors:
        raise exc.ValidationError(*errors)
