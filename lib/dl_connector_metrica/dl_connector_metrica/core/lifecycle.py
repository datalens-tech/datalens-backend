from __future__ import annotations

import logging

from dl_core import exc
from dl_core.connectors.base.lifecycle import ConnectionLifecycleManager
from dl_sqlalchemy_metrica_api.exceptions import MetrikaApiAccessDeniedException

from dl_connector_metrica.core.us_connection import MetrikaBaseMixin


LOGGER = logging.getLogger(__name__)


class MetricaConnectionLifecycleManager(ConnectionLifecycleManager[MetrikaBaseMixin]):
    ENTRY_CLS = MetrikaBaseMixin

    # TODO FIX: split into sync and async hooks
    def pre_save_hook(self) -> None:
        super().pre_save_hook()

        if self.entry.counter_id != self.entry._initial_counter_id or not self.entry.data.counter_creation_date:  # type: ignore  # TODO: fix
            LOGGER.info(
                "initial counter_id = %s, current counter_id = %s. "
                "Retrieving current counter creation date from Metrika API",
                self.entry._initial_counter_id,
                self.entry.counter_id,  # noqa
            )
            try:
                self.entry.data.counter_creation_date = self.entry.get_counter_creation_date()  # type: ignore  # TODO: fix
            except MetrikaApiAccessDeniedException as ex:
                raise exc.ConnectionConfigurationError("No access to counter info. Check your OAuth token.") from ex
