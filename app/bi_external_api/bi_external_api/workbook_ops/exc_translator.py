import functools
from typing import Optional, Sequence

import attr

from bi_external_api.converter import converter_exc
from bi_external_api.converter.converter_exc import CompositeConverterError
from bi_external_api.domain import external as ext
from bi_external_api.internal_api_clients.exc_api import DatasetValidationError, WorkbookNotFound
from bi_external_api.workbook_ops.private_exceptions import OperationTerminationError


@attr.s()
class ConverterExcTranslator:
    path: Optional[Sequence[str]] = attr.ib(default=None)
    do_add_exc_message: bool = attr.ib(default=True)

    def _make_ext_common_error(
            self,
            user_message: str,
            exc: Exception,
    ) -> ext.CommonError:
        path = self.path
        return ext.CommonError(
            path=".".join(path) if path else None,
            message=user_message,
            exc_message=str(exc) if self.do_add_exc_message else None,
        )

    @functools.singledispatchmethod
    def translate_error(self, exc: converter_exc.ConverterError) -> ext.CommonError:
        return self._make_ext_common_error("Unexpected conversion error. Please contact support for details.", exc)

    @translate_error.register
    def translate_constraint_violation(self, exc: converter_exc.ConstraintViolationError) -> ext.CommonError:
        return self._make_ext_common_error(f"Constraint violation: {exc}", exc)

    @translate_error.register
    def translate_not_supported(self, exc: converter_exc.NotSupportedYet) -> ext.CommonError:
        return self._make_ext_common_error(f"Feature is not yet supported: {exc}", exc)

    @translate_error.register
    def translate_wb_entry_not_found(self, exc: converter_exc.WorkbookEntryNotFound) -> ext.CommonError:
        return self._make_ext_common_error(
            f"Referenced entry not found in workbook: {exc}.",
            exc,
        )

    @translate_error.register
    def translate_malformed_entry_config(self, exc: converter_exc.MalformedEntryConfig) -> ext.CommonError:
        return self._make_ext_common_error(
            "Internal config of entry is malformed. Please contact support for details.",
            exc,
        )

    @translate_error.register
    def translate_dataset_field_not_found(self, exc: converter_exc.DatasetFieldNotFound) -> ext.CommonError:
        return self._make_ext_common_error(
            f"Dataset field not found: {exc}",
            exc,
        )


@attr.s()
class GenericExcTranslator:
    converter_translator: ConverterExcTranslator = attr.ib()
    do_add_exc_message: bool = attr.ib(default=True)

    def _make_ext_common_error(
            self,
            user_message: str,
            exc: Exception,
    ) -> ext.CommonError:
        return ext.CommonError(
            path=None,
            message=user_message,
            exc_message=str(exc) if self.do_add_exc_message else None,
        )

    @functools.singledispatchmethod
    def translate_error(self, exc: Exception) -> ext.CommonError:
        return self._make_ext_common_error("Unexpected error. Please contact support for details.", exc)

    @translate_error.register
    def translate_converter_error(self, exc: converter_exc.ConverterError) -> ext.CommonError:
        return self.converter_translator.translate_error(exc)

    @translate_error.register
    def translate_flow_break_error(self, exc: OperationTerminationError) -> ext.CommonError:
        return self._make_ext_common_error(exc.data.user_message, exc)

    @translate_error.register
    def translate_workbook_not_found(self, exc: WorkbookNotFound) -> ext.CommonError:
        return self._make_ext_common_error("Requested workbook not found", exc)

    @classmethod
    def create(cls, path: Optional[Sequence[str]], do_add_exc_message: bool) -> 'GenericExcTranslator':
        return cls(
            converter_translator=ConverterExcTranslator(
                path=path,
                do_add_exc_message=do_add_exc_message,
            ),
            do_add_exc_message=do_add_exc_message,
        )


@attr.s()
class CompositeExcTranslator:
    path: Optional[Sequence[str]] = attr.ib(default=None)
    do_add_exc_message: bool = attr.ib(default=True)

    def _make_ext_common_error(
            self,
            user_message: str,
            exc: Optional[Exception] = None,
    ) -> ext.CommonError:
        return ext.CommonError(
            path=".".join(self.path) if self.path else None,
            message=user_message,
            exc_message=str(exc) if exc and self.do_add_exc_message else None,
        )

    def make_common_error_for_unexpected_error(self, exc: Exception, message: Optional[str] = None) -> ext.CommonError:
        return self._make_ext_common_error(
            user_message=message or "Unexpected error. Please contact support for details.",
            exc=exc,
        )

    @functools.singledispatchmethod
    def translate_error(self, exc: Exception) -> Sequence[ext.CommonError]:
        # In case of unexpected exception assuming a scalar error
        return [
            GenericExcTranslator.create(
                path=self.path,
                do_add_exc_message=self.do_add_exc_message,
            ).translate_error(exc)
        ]

    @translate_error.register
    def translate_data_validation_error(self, exc: DatasetValidationError) -> Sequence[ext.CommonError]:
        ds = exc.data.dataset
        if ds is None:
            return [self.make_common_error_for_unexpected_error(exc, "Missing dataset")]
        result = []

        for ce in ds.component_errors.items:
            for err in ce.errors:
                result.append(
                    ext.CommonError(
                        path=f"{ce.type}.{ce.id}",
                        message=f"{err.message}, code: {err.code}"
                    )
                )
        return result

    @translate_error.register
    def translate_composite_converter_error(self, exc: CompositeConverterError) -> Sequence[ext.CommonError]:
        return [
            GenericExcTranslator.create(
                path=list(self.path or []) + list(path),
                do_add_exc_message=self.do_add_exc_message,
            ).translate_error(sub_exc)
            for path, sub_exc in exc.data.map_path_exc.items()
        ]

    @classmethod
    def create(cls, do_add_exc_message: bool, path: Optional[Sequence[str]] = None) -> 'CompositeExcTranslator':
        return cls(
            path=path,
            do_add_exc_message=do_add_exc_message,
        )
