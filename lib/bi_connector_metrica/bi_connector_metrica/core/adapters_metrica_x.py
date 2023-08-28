from __future__ import annotations

from typing import Optional, TypeVar, Any, Dict, Tuple, Type
from urllib.parse import quote_plus, urlencode

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy_metrika_api import exceptions as sqla_metrika_exc  # type: ignore

from bi_core import exc
from bi_core.connection_executors.adapters.adapters_base_sa import BaseSAAdapter
from bi_core.connectors.base.error_transformer import DBExcKWArgs
from bi_core.connection_models import DBIdent

from bi_connector_metrica.core.exc import MetricaAPIDatabaseQueryError
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API
from bi_connector_metrica.core.target_dto import MetricaAPIConnTargetDTO, AppMetricaAPIConnTargetDTO

_M_CONN_T_DTO_TV = TypeVar("_M_CONN_T_DTO_TV", bound=MetricaAPIConnTargetDTO)


class MetricaAPIDefaultAdapter(BaseSAAdapter[_M_CONN_T_DTO_TV]):
    conn_type = CONNECTION_TYPE_METRICA_API

    def _get_db_engine(self, db_name: str, disable_streaming: bool = False) -> Engine:
        if disable_streaming:
            raise Exception("`disable_streaming` is not applicable here")
        dsn = '{dialect}://:{token}@/{db_name}'.format(
            dialect=self.get_dialect_str(),
            token=quote_plus(self._target_dto.token),
            db_name=db_name,
        )
        dsn_params: Dict[str, Any] = {}
        if self._target_dto.accuracy is not None:
            dsn_params.update(accuracy=self._target_dto.accuracy)

        if dsn_params:
            dsn += '?' + urlencode(dsn_params)

        return sa.create_engine(dsn).execution_options(compiled_cache=None)

    @classmethod
    def make_exc(  # TODO:  Move to ErrorTransformer
            cls,
            wrapper_exc: Exception,
            orig_exc: Optional[Exception],
            debug_compiled_query: Optional[str]
    ) -> Tuple[Type[exc.DatabaseQueryError], DBExcKWArgs]:
        exc_cls, kw = super().make_exc(wrapper_exc, orig_exc, debug_compiled_query)

        if isinstance(
            orig_exc,
            (
                sqla_metrika_exc.MetrikaHttpApiException,
                sqla_metrika_exc.NotSupportedError,
                sqla_metrika_exc.ProgrammingError,
            )
        ):
            exc_cls = MetricaAPIDatabaseQueryError

        return exc_cls, kw

    def get_default_db_name(self) -> Optional[str]:
        return None

    def _get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return ''


class AppMetricaAPIDefaultAdapter(MetricaAPIDefaultAdapter[AppMetricaAPIConnTargetDTO]):  # type: ignore
    conn_type = CONNECTION_TYPE_APPMETRICA_API
