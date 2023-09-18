import contextlib
import logging
from typing import (
    Callable,
    ClassVar,
    Generator,
    Optional,
    Type,
)

from sqlalchemy import exc as sa_exc

from dl_core.connectors.base.error_transformer import (
    DbErrorTransformer,
    DBExcKWArgs,
)
import dl_core.exc as exc

LOGGER = logging.getLogger(__name__)


class ErrorHandlerMixin:
    # TODO: Switch to nested mode from mixin
    EXTRA_EXC_CLS: ClassVar[tuple[Type[Exception], ...]] = ()

    # TODO FIX: Try to implement overrides in more functional style
    @classmethod
    def make_exc(
        cls, wrapper_exc: Exception, orig_exc: Optional[Exception], debug_compiled_query: Optional[str]
    ) -> tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        raise NotImplementedError()

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
        ) + self.EXTRA_EXC_CLS

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
            LOGGER.info("Got DB exception in DBA.handle_execution_error()", exc_info=True)

            orig_exc = getattr(err, "orig", None)

            exc_cls, exc_kwargs = self.make_exc(
                wrapper_exc=err, orig_exc=orig_exc, debug_compiled_query=debug_compiled_query
            )
            try:
                raise exc_cls(**exc_kwargs) from err
            except exc.DatabaseQueryError as just_raised:
                post_process_exc_ignore_errors(just_raised)
                raise

        except Exception:
            LOGGER.info("Got unexpected exception in DBA.handle_execution_error()", exc_info=True)
            raise


class ETBasedExceptionMaker(ErrorHandlerMixin):
    """
    ErrorTransformer-based exception handling
    """

    _error_transformer: ClassVar[DbErrorTransformer]

    @classmethod
    def make_exc(
        cls, wrapper_exc: Exception, orig_exc: Optional[Exception], debug_compiled_query: Optional[str]
    ) -> tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        trans_exc_cls, kw = cls._error_transformer.make_bi_error_parameters(
            wrapper_exc=wrapper_exc,
            debug_compiled_query=debug_compiled_query,
        )

        return trans_exc_cls, kw
