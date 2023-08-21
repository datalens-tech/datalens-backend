from __future__ import annotations

import inspect
from functools import wraps
from typing import Any, Callable, ClassVar, Mapping, Optional, overload

import attr
import pytest

_FUNC_META_ATTR = '_regulated_test_function_meta'


@attr.s(frozen=True)
class Feature:
    feature_name: str = attr.ib()


@attr.s(frozen=True)
class RegulatedTestFunctionMeta:
    features: frozenset[Feature] = attr.ib(kw_only=True, default=frozenset())


@attr.s(frozen=True)
class RegulatedTestParams:
    mark_tests_skipped: Optional[Mapping[Callable, Optional[str]]] = attr.ib(kw_only=True, default=None)
    mark_tests_failed: Optional[Mapping[Callable, Optional[str]]] = attr.ib(kw_only=True, default=None)
    mark_features_skipped: Optional[Mapping[Feature, Optional[str]]] = attr.ib(kw_only=True, default=None)


def _is_test_func(func: Callable) -> bool:
    return callable(func) and func.__name__.startswith('test_')


def _is_test_func_for_features_with_reason(
        func: Callable, features: Mapping[Feature, Optional[str]],
) -> tuple[bool, str]:
    func_meta = getattr(func, _FUNC_META_ATTR, RegulatedTestFunctionMeta())
    found_features = func_meta.features & frozenset(features)
    reasons = sorted(
        (features[feature] or '')
        for feature in found_features if features[feature]
    )
    reason = '; '.join(reasons)
    return bool(found_features), reason


def _mark_as_test_func(func: Callable) -> Callable:
    if not hasattr(func, _FUNC_META_ATTR):
        setattr(func, _FUNC_META_ATTR, RegulatedTestFunctionMeta())

    return func


def _mark_skipped(func: Callable, reason: Optional[str]) -> Callable:
    assert _is_test_func(func)

    @wraps(func)
    @pytest.mark.skip(reason=reason or '')
    def func_replacement(*args: Any, **kwargs: Any) -> None:
        pass

    return func_replacement


def _mark_failed(func: Callable, reason: Optional[str]) -> Callable:
    assert _is_test_func(func)
    func_replacement = pytest.mark.xfail(reason=reason or '', strict=True)(func)
    return func_replacement


def _mark_functions(
        funcs: Mapping[Callable, Optional[str]],
        marker: Callable[[Callable, Optional[str]], Callable],
) -> Mapping[str, Callable]:
    result: dict[str, Callable] = {}
    for func, reason in funcs.items():
        result[func.__name__] = marker(func, reason)

    return result


def _resolve_test_funcs(bases: tuple[Any, ...]) -> Mapping[str, Callable]:
    return {
        name: func
        for base in bases
        for name, func in inspect.getmembers(base)
        if _is_test_func(func)
    }


def _patch_attrs_for_regulated_test_class(
        bases: tuple[Any, ...], new_attrs: Optional[Mapping],
        test_params: RegulatedTestParams,
) -> dict[str, Any]:

    new_attrs = dict(new_attrs or {})
    test_funcs = _resolve_test_funcs(bases=bases)

    # Decorate base class functions that require attention
    new_attrs |= _mark_functions(test_params.mark_tests_skipped or {}, _mark_skipped)
    new_attrs |= _mark_functions(test_params.mark_tests_failed or {}, _mark_failed)

    # Find and decorate test functions for skipped features
    mark_tests_skipped_for_features: dict[Callable, str] = {}
    for name, func in test_funcs.items():
        tests_feature, reason = _is_test_func_for_features_with_reason(func, test_params.mark_features_skipped or {})
        if tests_feature:
            mark_tests_skipped_for_features[func] = reason
    new_attrs |= _mark_functions(mark_tests_skipped_for_features or {}, _mark_skipped)

    return new_attrs


def _make_regulated_test_class(
        mcs: type, name: str, bases: tuple[Any, ...],
        test_params: RegulatedTestParams,
        attrs: Optional[Mapping] = None,
) -> type:
    # Mark all test functions as such
    attrs = attrs or {}
    assert attrs is not None
    attrs = dict(attrs, **{name: _mark_as_test_func(func) for name, func in attrs.items() if _is_test_func(func)})
    attrs = _patch_attrs_for_regulated_test_class(
        bases=bases, new_attrs=attrs, test_params=test_params,
    )

    new_cls = type.__new__(mcs, name, bases, attrs)  # type: ignore
    return new_cls


def _wrap_as_regulated_test_case(test_cls: type, *, test_params: RegulatedTestParams) -> type:
    return _make_regulated_test_class(
        mcs=type(test_cls), name=test_cls.__name__, bases=(test_cls,), test_params=test_params,
    )


def _patch_as_regulated_test_case(test_cls: type, *, test_params: RegulatedTestParams) -> type:
    attrs = _patch_attrs_for_regulated_test_class(
        bases=(test_cls,), new_attrs={}, test_params=test_params,
    )
    for name, value in attrs.items():
        setattr(test_cls, name, value)
    return test_cls


def for_features(*args: Feature) -> Callable[[Callable], Callable]:
    """Decorator for test functions to mark them as feature tests"""

    def decorator(func: Callable) -> Callable:
        if not hasattr(func, _FUNC_META_ATTR):
            setattr(
                func, _FUNC_META_ATTR,
                RegulatedTestFunctionMeta(
                    features=frozenset(args),
                ),
            )
        return func

    return decorator


class RegulatedTestCase:
    """
    Regulated test case version that patches its own subclasses
    after their creation - in `__init_subclass__`.
    """
    test_params: ClassVar[RegulatedTestParams] = RegulatedTestParams()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Patch subclass with test wrappers"""

        super().__init_subclass__()  # This is required so that nothing breaks

        # Apply the method patches
        _patch_as_regulated_test_case(cls, test_params=cls.test_params)


@overload
def regulated_test_case(test_cls: type, /) -> type:
    ...


@overload
def regulated_test_case(*, test_params: RegulatedTestParams = RegulatedTestParams()) -> Callable[[type], type]:
    ...


def regulated_test_case(*args: Any, **kwargs: Any) -> Any:
    """Decorator that makes a regulated test case."""

    if len(args) == 1 and not kwargs and isinstance(args[0], type):
        # Used as a plain decorator (class as first and only arg)
        test_cls = args[0]
        return _patch_as_regulated_test_case(test_cls, test_params=RegulatedTestParams())

    assert not args
    # Used as a parameterized decorator

    def decorator(test_cls: type) -> type:
        assert len(kwargs) <= 1
        test_params = kwargs['test_params']
        return _patch_as_regulated_test_case(test_cls, test_params=test_params)

    return decorator
