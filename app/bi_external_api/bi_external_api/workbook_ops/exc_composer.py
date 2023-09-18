from collections import defaultdict
from contextlib import contextmanager
import logging
from typing import (
    Iterator,
    Optional,
    Sequence,
)

import attr

from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.exc_tooling import (
    ExcComposer,
    SimpleCollectingExcComposer,
)
from bi_external_api.internal_api_clients.constants import DatasetOpCode
from bi_external_api.workbook_ops.exc_translator import CompositeExcTranslator
from bi_external_api.workbook_ops.private_exceptions import (
    WorkbookClusterizationErrorInfo,
    WorkbookClustezationPrivateError,
    WorkbookReadBasePrivateError,
    WorkbookReadErrorInfo,
    WorkbookReadPrivateError,
)
from bi_external_api.workbook_ops.public_exceptions import WorkbookOperationException
from dl_api_commons.exc import (
    AccessDeniedErr,
    ExceptionWithData,
)
from dl_us_client.constants import OpCode as UsOpCode

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class WbErrHandlingCtx:
    entry_name: str = attr.ib()


@attr.s(kw_only=True)
class WorkbookReadExcComposer(ExcComposer[WbErrHandlingCtx]):
    _exc_data_accumulator: WorkbookReadErrorInfo = attr.ib(factory=WorkbookReadErrorInfo)

    def set_initial_wb_context(self, initial_wb_ctx: WorkbookContext) -> None:
        self._exc_data_accumulator.wb_ctx = initial_wb_ctx
        if initial_wb_ctx.load_fail_info_collection:
            self.set_armed()

    def set_workbook_partial(self, wb: ext.WorkBook) -> None:
        self._exc_data_accumulator.partial_workbook = wb

    @classmethod
    def create_ctx(cls, entry_name: str) -> WbErrHandlingCtx:
        return WbErrHandlingCtx(entry_name=entry_name)

    # Interface methods
    def reduce_exc(self) -> Exception:
        return WorkbookReadPrivateError(self._exc_data_accumulator)

    def handle_postponed_exc(self, exception: Exception, extra: WbErrHandlingCtx) -> None:
        exc_lst = self._exc_data_accumulator.conversion_errors.setdefault(extra.entry_name, [])
        exc_lst.append(exception)

    def handle_unexpected_exc(self, exception: Exception) -> None:
        self._exc_data_accumulator.unexpected_errors.append(exception)


@attr.s(kw_only=True)
class WorkbookClusterizeExcComposer(WorkbookReadExcComposer):
    _exc_data_accumulator: WorkbookClusterizationErrorInfo = attr.ib(factory=WorkbookClusterizationErrorInfo)

    def set_post_clusterization_name_map(self, original_names: Sequence[ext.NameMapEntry]) -> None:
        self._exc_data_accumulator.post_clusterization_name_map = list(original_names)

    def reduce_exc(self) -> Exception:
        return WorkbookClustezationPrivateError(self._exc_data_accumulator)


@attr.s(kw_only=True)
class WorkbookOperationErrorHandler:
    """
    This is a root error handler for any API operation.
    It converts private exceptions into public ones.
    Log critical if caught exception was not from know list.
    No any details will be provided in public exception will be filled in this case.
    If caught exception is already public - it just pass-throw it.
    Literally - last resort converter to public exception.
    """

    _message: str = attr.ib()
    _do_add_exc_message: bool = attr.ib(default=True)

    @contextmanager
    def err_handler(self) -> Iterator[None]:
        try:
            yield
        except Exception as exc:
            normalized_exc: WorkbookOperationException

            try:
                normalized_exc = self.normalize_exc(exc)
            except Exception as normalization_exc:  # noqa
                LOGGER.critical("Exception occurred during WB API operation exception normalization", exc_info=True)
                normalized_exc = self.last_resort_exc()

            if normalized_exc is exc:
                raise exc
            else:
                raise normalized_exc from exc

    #
    # Helpers
    #
    def last_resort_exc(self) -> WorkbookOperationException:
        return WorkbookOperationException(
            ext.ErrWorkbookOp(
                message=self._message,
                common_errors=[ext.CommonError(message="Unexpected error occurred")],
                entry_errors=(),
                partial_workbook=None,
            )
        )

    def convert_exc(self, entry_name: Optional[str], exc: Exception) -> ext.CommonError:
        msg: str
        if entry_name is None:
            msg = f"Error occurred {type(exc)}"
        else:
            msg = f"Error during handling entry {entry_name}: {type(exc)}"

        if self._do_add_exc_message:
            return ext.CommonError(
                message=msg,
                exc_message=str(exc),
                # TODO FIX: Format stacktrace Stacktrace
                # stacktrace=traceback.format_exc(),
                stacktrace=None,
            )
        else:
            return ext.CommonError(
                message=msg,
                exc_message=None,
                stacktrace=None,
            )

    @staticmethod
    def _detect_wb_access_denied(exc: Exception) -> Optional[WorkbookOperationException]:
        if getattr(exc, "data", None) is None:
            return None

        assert isinstance(exc, ExceptionWithData)
        if getattr(exc.data, "unexpected_errors", None) is None:
            return None

        for e in exc.data.unexpected_errors:
            if isinstance(e, AccessDeniedErr) and e.data.operation in [
                DatasetOpCode.WORKBOOK_INFO_GET,
                UsOpCode.WB_INFO_GET,
            ]:
                common_errors = [ext.CommonError(message="No access to workbook")]
                return WorkbookOperationException(
                    ext.ErrWorkbookOp(
                        message="No access to workbook",
                        common_errors=common_errors,
                        entry_errors=[],
                        partial_workbook=exc.data.partial_workbook,
                    )
                )

        return None  # mypy

    def normalize_exc(self, main_exc: Exception) -> WorkbookOperationException:
        entry_errors: list[ext.EntryError]
        common_errors: list[ext.CommonError]
        partial_workbook: Optional[ext.WorkBook]
        effective_message: str

        if exc := self._detect_wb_access_denied(main_exc):
            return exc

        if isinstance(main_exc, WorkbookReadBasePrivateError):
            effective_message = f"{self._message}: can not convert workbook"
            map_entry_name_exc: dict[str, list[Exception]] = {}

            for entry_name, exc_lst in main_exc.data.conversion_errors.items():
                local_exc_lst = map_entry_name_exc.setdefault(entry_name, [])
                local_exc_lst.extend(exc_lst)

            wb_ctx = main_exc.data.wb_ctx

            if wb_ctx is not None:
                for load_fail_info in wb_ctx.load_fail_info_collection:
                    local_exc_lst = map_entry_name_exc.setdefault(load_fail_info.summary.name, [])
                    local_exc_lst.append(load_fail_info.exception)

            entry_errors = [
                ext.EntryError(
                    name=entry_name,
                    errors=[self.convert_exc(entry_name, per_entry_exc) for per_entry_exc in entry_exc_list],
                )
                for entry_name, entry_exc_list in map_entry_name_exc.items()
            ]
            common_errors = [self.convert_exc(None, exc) for exc in main_exc.data.unexpected_errors]

            partial_workbook = main_exc.data.partial_workbook
        elif isinstance(main_exc, WorkbookOperationException):
            return main_exc
        else:
            LOGGER.critical("Got unexpected exception in during operation execution", exc_info=True)
            common_errors = [self.convert_exc(None, main_exc)]
            entry_errors = []
            partial_workbook = None
            effective_message = self._message

        exc_data: ext.ErrWorkbookOp

        if isinstance(main_exc, WorkbookClustezationPrivateError):
            exc_data = ext.ErrWorkbookOpClusterization(
                message=effective_message,
                common_errors=common_errors,
                entry_errors=entry_errors,
                partial_workbook=partial_workbook,
                name_map=main_exc.data.post_clusterization_name_map,
            )
        else:
            exc_data = ext.ErrWorkbookOp(
                message=effective_message,
                common_errors=common_errors,
                entry_errors=entry_errors,
                partial_workbook=partial_workbook,
            )

        return WorkbookOperationException(exc_data)


@attr.s(kw_only=True)
class WorkbookModificationExcComposer(SimpleCollectingExcComposer[WbErrHandlingCtx]):
    def reduce_exc(self) -> Exception:
        map_entry_name_errors: dict[str, list[ext.CommonError]] = defaultdict(list)
        composite_translator = CompositeExcTranslator.create(do_add_exc_message=self._do_add_exc_message)

        for ctx, exc in self.collected_postponed_errors:
            map_entry_name_errors[ctx.entry_name].extend(composite_translator.translate_error(exc))

        entry_errors = sorted(
            [ext.EntryError(name=name, errors=err_list) for name, err_list in map_entry_name_errors.items()],
            key=lambda entry_err: entry_err.name,
        )

        common_errors: list[ext.CommonError] = []
        unexpected_exc = self.collected_unexpected_error

        if unexpected_exc is not None:
            common_errors.extend(composite_translator.translate_error(unexpected_exc))

        exc_data = ext.ErrWorkbookOp(
            message="Error writing workbook",
            common_errors=common_errors,
            entry_errors=entry_errors,
            partial_workbook=None,
        )
        return WorkbookOperationException(exc_data)
