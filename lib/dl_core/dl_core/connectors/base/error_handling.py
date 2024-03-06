import contextlib
import logging
from typing import (
    Callable,
    Generator,
    Optional,
    Type,
)

import attr
from sqlalchemy import exc as sa_exc

from dl_core.connectors.base.error_transformer import (
    DbErrorTransformer,
    ExceptionInfo,
    GeneratedException,
)
import dl_core.exc as exc


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class ExceptionMaker:  # TODO: merge with ErrorTransformer
    """
    ErrorTransformer-based exception handling
    """

    _error_transformer: DbErrorTransformer = attr.ib(kw_only=True)
    _extra_exception_classes: tuple[Type[Exception], ...] = attr.ib(kw_only=True, default=())

    # TODO CONSIDER: Pass DBAdapterQuery to be able to add some context logging
    @contextlib.contextmanager
    def handle_execution_error(
        self,
        debug_compiled_query: Optional[str],
        # This function will be called after converting DatabaseQueryError
        # or in case if DatabaseQueryError will be caught by this handler
        exc_post_processor: Optional[Callable[[exc.DatabaseQueryError], None]] = None,
    ) -> Generator[None, None, None]:
        exc_clses_to_catch: tuple[Type[Exception], ...] = (
            sa_exc.DatabaseError,
            sa_exc.InvalidRequestError,
        ) + self._extra_exception_classes

        def post_process_exc_ignore_errors(exc_to_post_process: exc.DatabaseQueryError) -> None:
            if exc_post_processor is not None:
                try:
                    exc_post_processor(exc_to_post_process)
                except Exception:  # noqa
                    LOGGER.exception("Error during postprocessing DatabaseQueryError exception")

        try:
            yield

        except exc.DatabaseQueryError as already_converted_err:
            post_process_exc_ignore_errors(already_converted_err)
            raise

        except exc_clses_to_catch as err:
            LOGGER.info(
                f"Got DB exception in {self.__class__.__name__}.handle_execution_error()",
                exc_info=True,
            )

            orig_exc = getattr(err, "orig", None)

            new_exc = self.make_exc(
                wrapper_exc=err,
                orig_exc=orig_exc,
                debug_compiled_query=debug_compiled_query,
            )
            try:
                raise new_exc from err
            except exc.DatabaseQueryError as just_raised:
                post_process_exc_ignore_errors(just_raised)
                raise

        except Exception:
            LOGGER.info(
                f"Got unexpected exception in {self.__class__.__name__}.handle_execution_error()",
                exc_info=True,
            )
            raise

    def make_exc_info(
        self,
        wrapper_exc: Exception = GeneratedException(),
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> ExceptionInfo:
        exc_info = self._error_transformer.make_bi_error_parameters(
            wrapper_exc=wrapper_exc,
            debug_compiled_query=debug_compiled_query,
            orig_exc=orig_exc,
            message=message,
        )
        return exc_info

    def make_exc(
        self,
        wrapper_exc: Exception = GeneratedException(),
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> exc.DatabaseQueryError:
        exc_info = self.make_exc_info(
            wrapper_exc=wrapper_exc,
            debug_compiled_query=debug_compiled_query,
            orig_exc=orig_exc,
            message=message,
        )
        return exc_info.exc_cls(**exc_info.exc_kwargs)
