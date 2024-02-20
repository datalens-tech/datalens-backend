from __future__ import annotations

import abc
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
    Union,
)

import attr
from sqlalchemy.sql.elements import (
    ClauseElement,
    ClauseList,
    FunctionFilter,
    TypeCoerce,
)
from sqlalchemy.sql.functions import Function as SAFunction

from dl_formula.core import (
    exc,
    nodes,
)
from dl_formula.core.datatype import (
    DataType,
    DataTypeParams,
)
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import (
    ArgFlagDispenser,
    ArgTypeMatcher,
)
from dl_formula.definitions.flags import ContextFlags
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import (
    FromArgs,
    ParamsEmpty,
)


if TYPE_CHECKING:
    from dl_formula.definitions.type_strategy import (
        TypeParamsStrategy,
        TypeStrategy,
    )
    from dl_formula.translation.context import TranslationCtx
    from dl_formula.translation.env import TranslationEnvironment


_VARIANT_OF_TV = TypeVar("_VARIANT_OF_TV")
_VARIANT_TV = TypeVar("_VARIANT_TV", bound="ValueVariant")


class ValueVariant(Generic[_VARIANT_OF_TV]):
    """
    Wrapper for a value to be chosen from a list by dialect.

    Method ``.match(dialect)`` should be called to check whether this value can be used for given dialect
    """

    __slots__ = ("dialects", "_value")

    def __init__(self, dialects: DialectCombo, value: _VARIANT_OF_TV):
        self.dialects: DialectCombo = dialects
        self._value: _VARIANT_OF_TV = value

    def clone(self: _VARIANT_TV, **kwargs) -> _VARIANT_TV:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        copy_kwargs = dict(
            dialects=self.dialects,
            value=self._value,
        )
        copy_kwargs.update(kwargs)
        return type(self)(**copy_kwargs)

    @property
    def value(self) -> _VARIANT_OF_TV:
        return self._value

    def match(self, dialect: DialectCombo) -> bool:
        """Return True if this variant matches the given dialect"""
        return self.dialects == D.ANY or self.dialects & dialect == dialect


TransCallResult = Union[ClauseElement, nodes.FormulaItem]
TranslateCallback = Callable[[nodes.FormulaItem], TransCallResult]


_TRANS_IMPL_TV = TypeVar("_TRANS_IMPL_TV", bound="FuncTranslationImplementationBase")


@attr.s  # FIXME: frozen=True ?
class FuncTranslationImplementationBase(abc.ABC):
    unwrap_args: bool = attr.ib(default=True)  # TODO: make an args-unwrapping subclass instead

    def clone(self: _TRANS_IMPL_TV, **kwargs: Any) -> _TRANS_IMPL_TV:
        return attr.evolve(**kwargs)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "evolve" has incompatible type "dict[str, Any]"; expected an attrs class  [misc]

    @abc.abstractmethod
    def translate(
        self,
        *raw_args: TranslationCtx,
        translator_cb: TranslateCallback,
        partition_by: Optional[ClauseList] = None,
        default_order_by: Optional[ClauseList] = None,
        translation_ctx: Optional[TranslationCtx] = None,
        translation_env: Optional[TranslationEnvironment] = None,
    ) -> ClauseElement:
        """
        Attempts to translate the node by calling it with the given ``args``.
        If the resulting value is a ``nodes.FormulaItem``,
        then call the translator callback to re-translate the node.
        Keep doing this until a ``ClauseElement`` is received.

        This callback can also be used for compiling complex SQLA objects
        that might require re-translation of intermediate results (e.g. window functions).
        """
        raise NotImplementedError

    @staticmethod
    def _await_conversion_to_sa(obj: TransCallResult, translator_cb: TranslateCallback) -> ClauseElement:
        """
        Wait until a node object becaomes an SQLAlchemy ``ClauseElement``
        by calling the translator callback in a cycle.
        """

        max_recursion_depth = 100
        for _ in range(max_recursion_depth):
            if isinstance(obj, nodes.FormulaItem):
                # got a nodes.FormulaItem that has to be translated again
                obj = translator_cb(obj)
            elif isinstance(obj, ClauseElement):
                # got sa object -> time to exit
                return obj
            else:
                raise TypeError(f"Invalid object type {type(obj)} for translation cycle")

        raise RuntimeError(f"Maximum translation recursion depth {max_recursion_depth} reached")

    @classmethod
    def _unwrap_translation_ctx_args(cls, args: Iterable[TranslationCtx]) -> list[ClauseElement]:
        result = []
        for arg in args:
            assert arg.expression is not None
            result.append(arg.expression)
        return result

    def _handle_args(
        self,
        args: tuple[TranslationCtx, ...],
        *,
        translation_func: Callable,
        translation_ctx: Optional[TranslationCtx] = None,
        translation_env: Optional[TranslationEnvironment] = None,
    ) -> tuple[Iterable[Union[TranslationCtx, ClauseElement]], dict[str, Any]]:
        handled_args: Iterable[Union[TranslationCtx, ClauseElement]] = args
        if self.unwrap_args:
            handled_args = self._unwrap_translation_ctx_args(args)
        # some mandatory unwrapping:
        # remove TypeCoerce wrapper because of bug in SA
        # that does not put parentheses around such expressions
        handled_args = tuple(arg.clause if isinstance(arg, TypeCoerce) else arg for arg in handled_args)

        handled_kwargs = {}
        possible_kwargs = {"_ctx": translation_ctx, "_env": translation_env}
        # NOTE: using `unwrap` is slightly dubious (it unwraps the
        # `functools.wraps` through `func.__wrapped__`)
        argspec = inspect.getfullargspec(inspect.unwrap(translation_func))
        for key, value in possible_kwargs.items():
            if value is None:
                continue
            # NOTE: passing to all `**kwargs`-functions too.
            if key in argspec.args or key in argspec.kwonlyargs or argspec.varkw:
                handled_kwargs[key] = value

        return handled_args, handled_kwargs


@attr.s
class FuncTranslationImplementation(FuncTranslationImplementationBase):  # noqa
    """
    Basic function implementation class.
    Consists of a single "main" translatable part that is generated
    by a ``self.translation_main(*args)`` call.
    """

    translation_main: Callable[..., TransCallResult] = attr.ib(kw_only=True)

    def translate(
        self,
        *raw_args: TranslationCtx,
        translator_cb: TranslateCallback,
        partition_by: Optional[ClauseList] = None,
        default_order_by: Optional[ClauseList] = None,
        translation_ctx: Optional[TranslationCtx] = None,
        translation_env: Optional[TranslationEnvironment] = None,
    ) -> ClauseElement:
        translation_main = self.translation_main
        assert translation_main is not None
        args, kwargs = self._handle_args(
            raw_args,
            translation_func=translation_main,
            translation_ctx=translation_ctx,
            translation_env=translation_env,
        )
        # generate the result
        return self._await_conversion_to_sa(translation_main(*args, **kwargs), translator_cb=translator_cb)


WinRangeTuple = tuple[Optional[int], Optional[int]]


@attr.s
class WindowFunctionImplementation(FuncTranslationImplementation):
    """
    Class for window function implementations.
    Here the sa clause is compiled of the "main" and "order_by" parts
    that are generated separately.
    """

    translation_order_by: Optional[Callable[..., TransCallResult]] = attr.ib(default=None)
    translation_range: Optional[Callable[..., WinRangeTuple]] = attr.ib(default=None)
    translation_rows: Optional[Callable[..., WinRangeTuple]] = attr.ib(default=None)

    def translate(
        self,
        *raw_args: TranslationCtx,
        translator_cb: TranslateCallback,
        partition_by: Optional[ClauseList] = None,
        default_order_by: Optional[ClauseList] = None,
        translation_ctx: Optional[TranslationCtx] = None,
        translation_env: Optional[TranslationEnvironment] = None,
    ) -> ClauseElement:
        translation_main = self.translation_main
        assert translation_main is not None
        # Constructing args for `translation_main`; for simplicity, for now,
        # not reconstructing them for other parts of the window expression.
        args, kwargs = self._handle_args(
            raw_args,
            translation_func=translation_main,
            translation_ctx=translation_ctx,
            translation_env=translation_env,
        )

        # generate the result
        func_part = self._await_conversion_to_sa(translation_main(*args, **kwargs), translator_cb=translator_cb)
        assert isinstance(func_part, (SAFunction, FunctionFilter))

        order_by_part = default_order_by
        if self.translation_order_by is not None:
            order_by_part = self._await_conversion_to_sa(  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "ClauseElement", variable has type "ClauseList | None")  [assignment]
                self.translation_order_by(*args),  # NOTE: not passing the `kwargs` here at the moment.
                translator_cb=translator_cb,
            )
            assert isinstance(order_by_part, ClauseList)

        range_part = None
        if self.translation_range is not None:
            range_part = self.translation_range(*args)  # NOTE: not passing the `kwargs` here at the moment.

        rows_part = None
        if self.translation_rows is not None:
            rows_part = self.translation_rows(*args)  # NOTE: not passing the `kwargs` here at the moment.

        # Note that an `Over` object cannot be simply reconstructed from its parts
        # as attributes of the existing `Over` instance.
        # RANGE_UNBOUNDED has to be replaced with None and RANGE_CURRENT with 0.
        return func_part.over(partition_by=partition_by, order_by=order_by_part, range_=range_part, rows=rows_part)


_TRANS_VAR_TV = TypeVar("_TRANS_VAR_TV", bound="TranslationVariant")


class TranslationVariant(ValueVariant[FuncTranslationImplementationBase]):
    """
    A ``ValueVariant`` for function translations.
    Ties together dialect and function implementation
    """

    __slots__ = ()
    _repr_attrs = ("dialects",)

    @classmethod
    def make(
        cls: Type[_TRANS_VAR_TV],
        dialects: DialectCombo,
        translation: Callable[..., TransCallResult],
        unwrap_args: bool = True,
        as_winfunc: bool = False,
        translation_order_by: Optional[Callable[..., TransCallResult]] = None,
        translation_range: Optional[Callable[..., WinRangeTuple]] = None,
        translation_rows: Optional[Callable[..., WinRangeTuple]] = None,
    ) -> _TRANS_VAR_TV:
        translation_impl: FuncTranslationImplementationBase  # noqa
        if as_winfunc:
            translation_impl = WindowFunctionImplementation(
                translation_main=translation,
                unwrap_args=unwrap_args,
                translation_order_by=translation_order_by,
                translation_range=translation_range,
                translation_rows=translation_rows,
            )
        else:
            translation_impl = FuncTranslationImplementation(
                translation_main=translation,
                unwrap_args=unwrap_args,
            )

        instance = cls(dialects=dialects, value=translation_impl)
        return instance

    def __repr__(self) -> str:
        _c0 = "\x1b[0m"
        _c1 = "\x1b[038;5;118m"
        return "<{}({})>{}".format(
            self.__class__.__name__,
            ",     ".join(f"{_c1}{key}{_c0}={getattr(self, key)!r}" for key in self._repr_attrs),
            _c0,
        )

    def translate(  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        self,
        *args,
        translator_cb: TranslateCallback,
        partition_by: Optional[ClauseList] = None,
        default_order_by: Optional[ClauseList] = None,
        translation_ctx: Optional[TranslationCtx] = None,
        translation_env: Optional[TranslationEnvironment] = None,
    ) -> ClauseElement:
        return self.value.translate(
            *args,
            translator_cb=translator_cb,
            partition_by=partition_by,
            default_order_by=default_order_by,
            translation_ctx=translation_ctx,
            translation_env=translation_env,
        )

    def match(self, dialect: DialectCombo) -> bool:
        return super().match(dialect)


_TRANS_VAR_WR_TV = TypeVar("_TRANS_VAR_WR_TV", bound="TranslationVariantWrapped")


# TODO: similar class for `as_winfunc`
# TODO: Do this more properly.
class TranslationVariantWrapped(TranslationVariant):
    """
    Variant subclass that passes `TranslationCtx` rather than SA
    `ClauseElement` to the translation.
    """

    @classmethod
    def make(cls: Type[_TRANS_VAR_WR_TV], *args: Any, **kwargs: Any) -> _TRANS_VAR_WR_TV:
        return super().make(*args, unwrap_args=False, **kwargs)  # type: ignore  # TODO: fix


class TranslationResult(NamedTuple):
    impl_callable: Callable[..., ClauseElement]
    transformed_args: list[TranslationCtx]
    data_type: DataType
    data_type_params: DataTypeParams
    context_flags: int
    postprocess_args: bool


class PartialTranslationResult(NamedTuple):
    impl_callable: Optional[Callable[..., ClauseElement]] = None
    transformed_args: Optional[list[TranslationCtx]] = None
    data_type: Optional[DataType] = None
    data_type_params: Optional[DataTypeParams] = None
    context_flags: Optional[int] = None


class ArgTransformer:
    def transform_args(
        self,
        env: TranslationEnvironment,
        args: list[TranslationCtx],
        arg_types: list[DataType],
    ) -> list[TranslationCtx]:
        """Perform custom transformations of arguments"""
        return args


_NODE_TRANS_TV = TypeVar("_NODE_TRANS_TV", bound="NodeTranslation")


class NodeTranslation:
    __slots__ = ()

    # the following pair acts as a sort of identifier for the function/operator translation
    name: ClassVar[Optional[str]] = None
    arg_cnt: ClassVar[Optional[int]] = None

    is_function: ClassVar[bool] = True
    is_aggregation: ClassVar[bool] = False
    is_window: ClassVar[bool] = False
    uses_default_ordering: ClassVar[bool] = False
    supports_grouping: ClassVar[bool] = False
    supports_ordering: ClassVar[bool] = False
    supports_lod: ClassVar[bool] = False
    supports_ignore_dimensions: ClassVar[bool] = False
    supports_bfb: ClassVar[bool] = False

    scopes: ClassVar[int] = Scope.STABLE | Scope.EXPLICIT_USAGE | Scope.SUGGESTED | Scope.DOCUMENTED

    arg_names: ClassVar[Optional[list[str]]] = None

    @classmethod
    def match_types(cls, arg_types: list[DataType]) -> bool:
        raise NotImplementedError

    @classmethod
    def get_return_type(cls, arg_types: list[DataType]) -> DataType:
        raise NotImplementedError

    @classmethod
    def _get_return_type_info(cls, arg_types: list[DataType]) -> DataType:
        raise NotImplementedError

    def match_dialect(self, dialect: DialectCombo) -> bool:
        raise NotImplementedError

    def transform_args(
        self,
        env: TranslationEnvironment,
        args: list[TranslationCtx],
        arg_types: list[DataType],
    ) -> list[TranslationCtx]:
        """Perform custom transformations of arguments"""
        return args

    def translate(
        self,
        env: TranslationEnvironment,
        args: list[TranslationCtx],
    ) -> TranslationResult:
        raise NotImplementedError

    def supported_dialects(self) -> DialectCombo:
        raise NotImplementedError

    @classmethod
    def for_trans_var(cls: Type[_NODE_TRANS_TV], variant: TranslationVariant) -> _NODE_TRANS_TV:
        raise NotImplementedError

    @classmethod
    def for_dialect(
        cls: Type[_NODE_TRANS_TV],
        dialects: DialectCombo,
        arg_transformer: Optional[ArgTransformer] = None,
    ) -> _NODE_TRANS_TV:
        raise NotImplementedError


_MULTI_NODE_TRANS_TV = TypeVar("_MULTI_NODE_TRANS_TV", bound="MultiVariantTranslation")


class MultiVariantTranslation(NodeTranslation):
    __slots__ = ("_inst_variants", "_inst_arg_transformer")

    variants: ClassVar[list[TranslationVariant]] = []
    argument_types: ClassVar[Optional[list[ArgTypeMatcher]]] = None
    argument_flags: ClassVar[Optional[ArgFlagDispenser]] = None
    return_type: ClassVar[TypeStrategy] = FromArgs()
    return_type_params: ClassVar[TypeParamsStrategy] = ParamsEmpty()
    return_flags: ClassVar[ContextFlags] = 0
    postprocess_args: ClassVar[bool] = True
    arg_transformer: ClassVar[ArgTransformer] = ArgTransformer()

    # Instance vars
    _inst_variants: list[TranslationVariant]
    _inst_arg_transformer: ArgTransformer

    def __init__(
        self,
        variants: Optional[list[TranslationVariant]] = None,
        arg_transformer: Optional[ArgTransformer] = None,
    ):
        if variants is None:
            variants = self.variants
        assert variants is not None
        self._inst_variants = variants

        if arg_transformer is None:
            arg_transformer = self.arg_transformer
        assert arg_transformer is not None
        self._inst_arg_transformer = arg_transformer

        super().__init__()

    def get_variants(self) -> list[TranslationVariant]:
        """Override point (to avoid making a class property or a metaclass)"""
        return self._inst_variants

    def transform_args(
        self,
        env: TranslationEnvironment,
        args: list[TranslationCtx],
        arg_types: list[DataType],
    ) -> list[TranslationCtx]:
        """Perform custom transformations of arguments"""
        return self._inst_arg_transformer.transform_args(env=env, args=args, arg_types=arg_types)

    @classmethod
    def match_types(cls, arg_types: list[DataType]) -> bool:
        """
        Check whether data types of arguments match the required ones.
        Raise error if they don't.
        """
        if cls.argument_types is None:
            return True

        return any(arg_matcher.match_arg_types(arg_types) for arg_matcher in cls.argument_types)

    # TODO?: rename.
    @classmethod
    def get_return_type(cls, arg_types: list[DataType]) -> DataType:
        """NOTE: only for auto-documentation"""
        return cls.return_type.get_from_args(arg_types=arg_types)

    def match_dialect(self, dialect: DialectCombo) -> bool:
        return any(variant.match(dialect=dialect) for variant in self.get_variants())

    @classmethod
    def _get_argument_flag_dispenser(cls, dialect: DialectCombo) -> Optional[ArgFlagDispenser]:
        return cls.argument_flags

    @classmethod
    def _get_return_flags(cls, dialect: DialectCombo) -> ContextFlags:
        return cls.return_flags

    @classmethod
    def _get_return_type_info(cls, args):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        arg_types = [arg.data_type for arg in args]
        data_type = cls.return_type.get_from_args(arg_types)
        data_type_params = cls.return_type_params.get_from_arg_values(args)
        return data_type, data_type_params

    def translate(
        self,
        env: TranslationEnvironment,
        args: list[TranslationCtx],
    ) -> TranslationResult:
        """
        Find a translation variant that matches function name and argument types.
        Return a ``TranslationResult`` instance that consists of:
            0. result of the translation, which is either:
                a) a callable that should be called with transformed arguments
                b) a ``nodes.FormulaItem`` instance that has to be translated once more
            1. transformed list of arguments (most common transformation is changing of their order
                to precisely match the translation's signature in case of commutative operations)
            2. function result's data type (``datatype.DataType`` member)
            3. function result's data type parameters (e.g. timezone, precision, ...)
            4. function result's context flags (a bitwise combination of ``flags.ContextFlag`` members)
        """

        arg_types = []
        for arg in args:
            assert arg.data_type is not None
            arg_types.append(arg.data_type)

        args = self.transform_args(env=env, args=args, arg_types=arg_types)

        for variant in self.get_variants():
            if variant.match(dialect=env.dialect):
                # apply argument flags to args
                flag_dispenser = self._get_argument_flag_dispenser(env.dialect)
                if flag_dispenser is not None:
                    # found a match for dialect
                    for pos, arg in enumerate(args):
                        arg_flags = flag_dispenser.get_flags_for_pos(pos, total=len(args))
                        if arg_flags:
                            arg.set_flags(arg_flags)

                # result
                return_flags = self._get_return_flags(env.dialect)
                data_type, data_type_params = self._get_return_type_info(args)
                return TranslationResult(
                    impl_callable=variant.translate,
                    transformed_args=args,
                    data_type=data_type,
                    data_type_params=data_type_params,
                    context_flags=return_flags,
                    postprocess_args=self.postprocess_args,
                )

        dialect_bits = "|".join([basic_dialect.single_bit.name.name for basic_dialect in env.dialect.to_list()])
        raise exc.TranslationUnknownFunctionError(
            f'Function "{self.name}" is not implemented '
            f"for {dialect_bits} dialect and given arguments "
            f'({"|".join(t.name for t in arg_types)})'
        )

    def supported_dialects(self) -> DialectCombo:
        dialects: DialectCombo = D.EMPTY
        for variant in self.get_variants():
            dialects |= variant.dialects
        return dialects

    @classmethod
    def for_trans_var(cls: Type[_MULTI_NODE_TRANS_TV], variant: TranslationVariant) -> _MULTI_NODE_TRANS_TV:
        return cls(variants=[variant])

    @classmethod
    def for_dialect(
        cls: Type[_MULTI_NODE_TRANS_TV],
        dialects: DialectCombo,
        arg_transformer: Optional[ArgTransformer] = None,
    ) -> _MULTI_NODE_TRANS_TV:
        assert len(cls.variants) == 1
        variant = next(iter(cls.variants))
        patched_variant = variant.clone(dialects=dialects)
        return cls(variants=[patched_variant], arg_transformer=arg_transformer)

    def for_another_dialect(
        self,
        dialects: DialectCombo,
        func_class: Type[MultiVariantTranslation] | SingleVariantTranslationBase,
        arg_transformer: Optional[ArgTransformer] = None,
    ) -> _MULTI_NODE_TRANS_TV:
        assert len(self.get_variants()) == 1
        variant = next(iter(self.get_variants()))
        patched_variant = variant.clone(dialects=dialects)
        return (
            func_class(variants=[patched_variant], arg_transformer=arg_transformer)
            if not issubclass(func_class, SingleVariantTranslationBase)
            else func_class(dialects=dialects, arg_transformer=arg_transformer)
        )


_SINGLE_NODE_TRANS_TV = TypeVar("_SINGLE_NODE_TRANS_TV", bound="SingleVariantTranslationBase")


class SingleVariantTranslationBase(MultiVariantTranslation):
    """
    'One' is a subset of 'many';
    this class implements a more convenient and extended form of defining an
    implementation for only one variant,
    for cases where databases behave too differently.
    """

    __slots__ = ("_inst_dialects",)

    dialects: ClassVar[Optional[DialectCombo]] = None

    # Instance vars
    _inst_dialects: Optional[DialectCombo]

    def __init__(
        self,
        dialects: Optional[DialectCombo] = None,
        arg_transformer: Optional[ArgTransformer] = None,
    ):
        if dialects is None:
            dialects = self.dialects

        assert dialects is not None
        self._inst_dialects = dialects
        super().__init__(arg_transformer=arg_transformer)

    def _translate_main(cls, *args):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        """Variant implementation"""
        raise NotImplementedError

    def get_variants(self) -> list[TranslationVariant]:
        """
        Semi-mockup for all supporting methods
        (e.g `match_dialect`, `variant.match`, `supported_dialects`).
        """
        return [TranslationVariantWrapped.make(self._inst_dialects, self._translate_main)]

    @classmethod
    def for_trans_var(cls: Type[_SINGLE_NODE_TRANS_TV], variant: TranslationVariant) -> _SINGLE_NODE_TRANS_TV:
        raise NotImplementedError

    @classmethod
    def for_dialect(
        cls: Type[_SINGLE_NODE_TRANS_TV],
        dialects: DialectCombo,
        arg_transformer: Optional[ArgTransformer] = None,
    ) -> _SINGLE_NODE_TRANS_TV:
        return cls(dialects=dialects)


class SingleVariantFullOverrideTranslationBase(SingleVariantTranslationBase):
    """
    A version of SingleVariantTranslationBase that allows overriding all parts of
    TranslationResult in the variant implementation.

    In the best case, this will never be used, and will only ever be an example.
    """

    __slots__ = ()

    @classmethod
    def _translate_all(cls, *args, env=None) -> PartialTranslationResult:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        """Main overridable method"""
        raise NotImplementedError

    @classmethod
    def _translate_main(cls, *args, **kwargs):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        raise Exception(
            "SingleVariantFullOverrideTranslation._translate_main dummy method was called",
            dict(cls=cls, args=args, kwargs=kwargs),
        )

    def translate(self, env, args) -> TranslationResult:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        sup_result: TranslationResult = super().translate(env=env, args=args)
        var_result: PartialTranslationResult = self._translate_all(*args, env=env)
        return sup_result._replace(**{key: val for key, val in var_result._asdict().items() if val is not None})


class Function(MultiVariantTranslation):
    is_function = True
