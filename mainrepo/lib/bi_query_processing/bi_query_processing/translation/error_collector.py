from __future__ import annotations

from typing import Any, Optional, Type

import attr

import bi_formula.core.exc as formula_exc
from bi_formula.core.message_ctx import FormulaErrorCtx

import bi_query_processing.exc


FEATURE_ERROR_CODE = formula_exc.TranslationError.default_code + ('FEATURE_UNAVAILABLE', )


@attr.s
class FormulaErrorCollector:
    _errors: list[FormulaErrorCtx] = attr.ib(init=False, factory=list)

    def get_errors(self, feature_errors: bool = True) -> list[FormulaErrorCtx]:
        return [err for err in self._errors if feature_errors or err.code != FEATURE_ERROR_CODE]

    def __enter__(self) -> 'FormulaErrorCollector':
        return self

    def __exit__(
            self, exc_type: Optional[Type[Exception]],
            exc_val: Optional[Exception], exc_tb: Any
    ) -> bool:
        if isinstance(exc_val, bi_query_processing.exc.FormulaHandlingError):
            self._errors += exc_val.errors
            return True
        return False
