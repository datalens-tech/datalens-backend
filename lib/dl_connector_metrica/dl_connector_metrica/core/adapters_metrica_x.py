from __future__ import annotations

from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
)
from urllib.parse import (
    quote_plus,
    urlencode,
)

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from dl_core import exc
from dl_core.connection_executors.adapters.adapters_base_sa import BaseSAAdapter
from dl_core.connection_models import DBIdent
from dl_core.connectors.base.error_handling import ExceptionMaker
from dl_core.connectors.base.error_transformer import (
    DBExcKWArgs,
    ExceptionInfo,
    GeneratedException,
    make_default_transformer_with_custom_rules,
)
from dl_sqlalchemy_metrica_api import exceptions as sqla_metrika_exc

from dl_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
)
from dl_connector_metrica.core.exc import MetricaAPIDatabaseQueryError
from dl_connector_metrica.core.target_dto import (
    AppMetricaAPIConnTargetDTO,
    MetricaAPIConnTargetDTO,
)


_M_CONN_T_DTO_TV = TypeVar("_M_CONN_T_DTO_TV", bound=MetricaAPIConnTargetDTO)


class MetricaExceptionMaker(ExceptionMaker):
    # FIXME: Move customization to ErrorTransformer subclass instead of ExceptionMaker
    def make_exc_info(
        self,
        wrapper_exc: Exception = GeneratedException(),
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> ExceptionInfo:
        exc_info = super().make_exc_info(
            wrapper_exc=wrapper_exc,
            orig_exc=orig_exc,
            debug_compiled_query=debug_compiled_query,
            message=message,
        )

        if isinstance(
            orig_exc,
            (
                sqla_metrika_exc.MetrikaHttpApiException,
                sqla_metrika_exc.NotSupportedError,
                sqla_metrika_exc.ProgrammingError,
            ),
        ):
            exc_info = exc_info.clone(exc_cls=MetricaAPIDatabaseQueryError)

        return exc_info


class MetricaAPIDefaultAdapter(BaseSAAdapter[_M_CONN_T_DTO_TV]):
    conn_type = CONNECTION_TYPE_METRICA_API

    def _make_exception_maker(self) -> ExceptionMaker:
        return MetricaExceptionMaker(
            error_transformer=make_default_transformer_with_custom_rules(),
        )

    def _get_db_engine(self, db_name: str, disable_streaming: bool = False) -> Engine:
        if disable_streaming:
            raise Exception("`disable_streaming` is not applicable here")
        dsn = "{dialect}://:{token}@/{db_name}".format(
            dialect=self.get_dialect_str(),
            token=quote_plus(self._target_dto.token),
            db_name=db_name,
        )
        dsn_params: Dict[str, Any] = {}
        if self._target_dto.accuracy is not None:
            dsn_params.update(accuracy=self._target_dto.accuracy)

        if dsn_params:
            dsn += "?" + urlencode(dsn_params)

        return sa.create_engine(dsn).execution_options(compiled_cache=None)

    def get_default_db_name(self) -> Optional[str]:
        return None

    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return ""


class AppMetricaAPIDefaultAdapter(MetricaAPIDefaultAdapter[AppMetricaAPIConnTargetDTO]):  # type: ignore  # 2024-01-30 # TODO: Type argument "AppMetricaAPIConnTargetDTO" of "MetricaAPIDefaultAdapter" must be a subtype of "MetricaAPIConnTargetDTO"  [type-var]
    conn_type = CONNECTION_TYPE_APPMETRICA_API
