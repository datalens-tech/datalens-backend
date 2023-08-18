""" Convenience gathering """
from __future__ import annotations

from .manager_aiopg_base import DLSPGBase  # pylint: disable=unused-import
from .manager_aiopg_private import DLSPGPrivate
from .manager_aiopg_public import DLSPGPublic


__all__ = (
    'DLSPGBase',
    'DLSPGPrivate',
    'DLSPGPublic',
    'DLSPG',
)


class DLSPG(DLSPGPublic, DLSPGPrivate):
    """ ... """
