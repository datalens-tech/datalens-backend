from importlib import metadata
from typing import Collection, Optional, Type

from bi_sqlalchemy_gsheets.base import GSheetsDialect
from bi_sqlalchemy_bitrix.base import BitrixDialect

import bi_formula as package
from bi_formula.core.dialect import StandardDialect as D
from bi_formula.translation.sa_dialects import register_sa_dialect
from bi_formula.translation.columns import register_column_renderer_cls
from bi_formula.connectors.base.connector import FormulaConnector
from bi_formula.connectors.base.column import DefaultColumnRenderer, UnprefixedColumnRenderer
from bi_formula.connectors.registration import CONN_REG_FORMULA


_CONNECTOR_EP_GROUP = f'{package.__name__}.connectors'


def _get_all_ep_connectors() -> dict[str, Type[FormulaConnector]]:
    entrypoints = list(metadata.entry_points().select(  # type: ignore
        group=_CONNECTOR_EP_GROUP
    ))
    return {ep.name: ep.load() for ep in entrypoints}


def register_all_connectors(connector_ep_names: Optional[Collection[str]] = None) -> None:
    connectors = _get_all_ep_connectors()
    for ep_name, connector_cls in sorted(connectors.items()):
        if connector_ep_names is not None and ep_name not in connector_ep_names:
            continue
        CONN_REG_FORMULA.register_connector(connector_cls)

    # FIXME: Remove all this manual registration of dialects
    register_sa_dialect(dialect=D.GSHEETS, sa_dialect=GSheetsDialect())
    register_sa_dialect(dialect=D.BITRIX, sa_dialect=BitrixDialect())
    register_column_renderer_cls(D.GSHEETS, UnprefixedColumnRenderer)
    register_column_renderer_cls(D.BITRIX, DefaultColumnRenderer)
