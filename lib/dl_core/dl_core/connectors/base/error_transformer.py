from __future__ import annotations

import abc
import asyncio
import re
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    Optional,
    Type,
    TypedDict,
)

import attr
from sqlalchemy import exc as sa_exc

import dl_core.exc as exc
from dl_core.exc import DatabaseQueryError


class GeneratedException(Exception):
    """Exception for use in ErrorTransformer for exceptions generated without a source exception"""


class DBExcKWArgs(TypedDict, total=False):
    db_message: Optional[str]
    query: Optional[str]
    orig: Optional[Exception]
    details: dict[str, Any]


@attr.s(frozen=True)
class ExceptionInfo:
    exc_cls: Type[exc.DatabaseQueryError] = attr.ib(kw_only=True)
    exc_kwargs: DBExcKWArgs = attr.ib(kw_only=True)

    def clone(self, **kwargs: Any) -> ExceptionInfo:
        return attr.evolve(self, **kwargs)


class DbErrorTransformer(abc.ABC):
    _DEFAULT_EXC_CLS: ClassVar[Type[exc.DatabaseQueryError]] = exc.DatabaseQueryError

    def make_bi_error_parameters(
        self,
        wrapper_exc: Exception,
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> ExceptionInfo:
        kw: DBExcKWArgs = DBExcKWArgs(
            db_message=message or str(wrapper_exc),
            query=debug_compiled_query,
            orig=wrapper_exc,
            details={},
        )
        exc_info = ExceptionInfo(exc_cls=self._DEFAULT_EXC_CLS, exc_kwargs=kw)
        return exc_info

    def make_bi_error(
        self,
        wrapper_exc: Exception,
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> exc.DatabaseQueryError:
        exc_info = self.make_bi_error_parameters(
            wrapper_exc=wrapper_exc,
            orig_exc=orig_exc,
            debug_compiled_query=debug_compiled_query,
            message=message,
        )
        return exc_info.exc_cls(**exc_info.exc_kwargs)


ExcMatchCondition = Callable[[Exception], bool]


@attr.s(frozen=True, kw_only=True)
class ErrorTransformerRule(abc.ABC):
    """Definition of a rule for transforming exceptions raised by a connector
    to their BI-domain exception counterparts. See `ChainedDbErrorTransformer` for more details.

    `when` - is a condition that is evaluated for the exception raised by a connector.
    If a condition is matched then `then_raise` exception will be raised

    `then_raise` - is a BI-domain exception that will be raised if `when` condition is matched
    """

    when: ExcMatchCondition = attr.ib()
    then_raise: Type[exc.DatabaseQueryError] = attr.ib()

    def get_bi_error_class(
        self,
        wrapper_exc: Exception,
    ) -> Optional[Type[exc.DatabaseQueryError]]:
        if self.when(wrapper_exc):
            return self.then_raise
        else:
            return None


def wrapper_exc_is(wrapper_exc_cls: Type[Exception]) -> ExcMatchCondition:
    def _(wrapper_exc: Exception) -> bool:
        return isinstance(wrapper_exc, wrapper_exc_cls)

    return _


def orig_exc_is(orig_exc_cls: Type[Exception]) -> ExcMatchCondition:
    def _(wrapper_exc: Exception) -> bool:
        orig_exc = getattr(wrapper_exc, "orig", None)
        return isinstance(orig_exc, orig_exc_cls)

    return _


def wrapper_exc_is_and_matches_re(wrapper_exc_cls: Type[Exception], err_regex_str: str) -> ExcMatchCondition:
    pattern = re.compile(err_regex_str)

    def _(wrapper_exc: Exception) -> bool:
        return isinstance(wrapper_exc, wrapper_exc_cls) and pattern.search(str(wrapper_exc)) is not None

    return _


@attr.s(frozen=True)
class ChainedDbErrorTransformer(DbErrorTransformer):
    """Transformer for exceptions raised by a connector
    to their BI-domain exception counterparts that works as a part of a chain of rules.
    The rules are applied one-by-one in a chain.
    The first rule in a chain with a matching condition will result in raising
    corresponding BI-domain exception.
    If none of the rules match the condition a fallback exception will be raised.

    `rule_chain` - is a sequence of `ErrorTransformerRule` that are applied one-by-one.
    If a rule condition is matched it produces a BI-domain exception that will be raised.

    `fallback_exc` - is a BI-domain exception that will be raised if none of the rule conditions were matched
    """

    rule_chain: Iterable[ErrorTransformerRule] = attr.ib()
    fallback_exc: Type[exc.DatabaseQueryError] = attr.ib(default=exc.DatabaseQueryError)

    def make_bi_error(
        self,
        wrapper_exc: Exception,
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> DatabaseQueryError:
        transformed_exc_cls: Type[DatabaseQueryError] = self._get_bi_error_cls(wrapper_exc)
        kw: DBExcKWArgs = self._get_error_kw(
            debug_compiled_query=debug_compiled_query,
            orig_exc=orig_exc,
            wrapper_exc=wrapper_exc,
            message=message,
        )
        return transformed_exc_cls(**kw)

    def make_bi_error_parameters(
        self,
        wrapper_exc: Exception,
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> ExceptionInfo:
        transformed_exc_cls: Type[DatabaseQueryError] = self._get_bi_error_cls(wrapper_exc)
        if orig_exc is None:  # TODO: get rid of this crutch in favor of passing orig_exc down as an arg
            orig_exc = getattr(wrapper_exc, "orig", None)
        kw: DBExcKWArgs = self._get_error_kw(
            debug_compiled_query=debug_compiled_query,
            orig_exc=orig_exc,
            wrapper_exc=wrapper_exc,
            message=message,
        )
        exc_info = ExceptionInfo(exc_cls=transformed_exc_cls, exc_kwargs=kw)
        return exc_info

    def _get_bi_error_cls(
        self,
        wrapper_exc: Exception,
    ) -> Type[exc.DatabaseQueryError]:
        transformed_exc_class_generator = (t.get_bi_error_class(wrapper_exc) for t in self.rule_chain)
        return next(
            (exc_cls for exc_cls in transformed_exc_class_generator if exc_cls is not None),
            DbErrorTransformer._DEFAULT_EXC_CLS,
        )

    @staticmethod
    def _get_error_kw(
        debug_compiled_query: Optional[str],
        orig_exc: Optional[Exception],
        wrapper_exc: Exception,
        message: Optional[str] = None,
    ) -> DBExcKWArgs:
        return dict(
            db_message=message or (str(orig_exc) if orig_exc else str(wrapper_exc)),
            query=debug_compiled_query,
            orig=orig_exc,
            details={},
        )


default_error_transformer_rules = (
    ErrorTransformerRule(when=orig_exc_is(GeneratedException), then_raise=DatabaseQueryError),
    ErrorTransformerRule(
        when=orig_exc_is(sa_exc.NoSuchTableError),  # FIXME: not an exc.DatabaseQueryError
        then_raise=exc.SourceDoesNotExist,
    ),
    ErrorTransformerRule(when=wrapper_exc_is(sa_exc.OperationalError), then_raise=exc.DatabaseOperationalError),
    ErrorTransformerRule(when=wrapper_exc_is(asyncio.TimeoutError), then_raise=exc.SourceTimeout),
)


def make_default_transformer_with_custom_rules(*custom_rules: ErrorTransformerRule) -> ChainedDbErrorTransformer:
    return ChainedDbErrorTransformer(custom_rules + default_error_transformer_rules)


def make_rule_from_descr(
    descr: tuple[
        tuple[Type[Exception], Optional[str]],
        Type[exc.DatabaseQueryError],
    ]
) -> ErrorTransformerRule:
    (wrapper_exc, re_pattern), then_raise = descr
    if re_pattern is None:
        return ErrorTransformerRule(when=wrapper_exc_is(wrapper_exc), then_raise=then_raise)
    else:
        return ErrorTransformerRule(when=wrapper_exc_is_and_matches_re(wrapper_exc, re_pattern), then_raise=then_raise)
