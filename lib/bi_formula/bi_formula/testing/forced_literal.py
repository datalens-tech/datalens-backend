from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

import pytest

from bi_formula.definitions.type_strategy import FromArgs
from bi_formula.core.dialect import StandardDialect as D
from bi_formula.definitions.registry import OPERATION_REGISTRY
from bi_formula.definitions.base import MultiVariantTranslation, TranslationVariant


V = TranslationVariant.make


class ForcedLiteral(MultiVariantTranslation):
    """
    A hack wrapper function to avoid a value being recognized as a constant.
    Originally it wrapped the constant value in a ``Literal`` node, hence the name.
    Now it just wraps it in a function call, so the value loses its ``CONST_`` quality.
    TODO: rename
    """

    name = '__lit__'
    arg_cnt = 1
    variants = [V(D.ANY, lambda x: x)]
    return_type = FromArgs()  # will automatically convert from const to non-const type

    @classmethod
    @contextmanager
    def use(cls):
        def_item = cls()
        OPERATION_REGISTRY.register(def_item)
        try:
            yield
        finally:
            OPERATION_REGISTRY.unregister(def_item)


@pytest.yield_fixture
def forced_literal_use() -> Generator[None, None, None]:
    with ForcedLiteral.use():
        yield None
