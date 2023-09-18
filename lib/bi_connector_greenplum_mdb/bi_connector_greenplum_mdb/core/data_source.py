from __future__ import annotations

from dl_connector_greenplum.core.data_source import GreenplumTableDataSource, GreenplumSubselectDataSource


class GreenplumMDBTableDataSource(GreenplumTableDataSource):
    """ MDB Greenplum table """


class GreenplumMDBSubselectDataSource(GreenplumSubselectDataSource):
    """ MDB Greenplum subselect """
