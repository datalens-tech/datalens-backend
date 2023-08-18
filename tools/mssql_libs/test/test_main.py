from __future__ import annotations

import pytest


def test_paths():
    import os
    data = {
        path: os.listdir(path)
        for path in os.environ['LD_LIBRARY_PATH'].split(':')}
    print('LD_LIBRARY_PATH contents:', data)
    assert data


def test_mssql():
    import pyodbc
    dsn = (
        # bi-formula's devenv port is `5025`,
        # but expecting this test to get connrefused.
        'DRIVER={FreeTDS};Server=localhost;Port=15025;Database=formula_test;'
        'UID=datalens;PWD=qweRTY123;TDS_Version=8.0'
    )

    # Failure example:
    #
    #   pyodbc.Error: ('01000', "[01000] [unixODBC][Driver Manager]Can't open lib
    #   'FreeTDS' : file not found (0) (SQLDriverConnect)")

    # Success example:
    #
    #   pyodbc.OperationalError: ('08S01', '[08S01] [FreeTDS][SQL Server]Unable
    #   to connect: Adaptive Server is unavailable or does not exist (20009)
    #   (SQLDriverConnect)')

    with pytest.raises(pyodbc.OperationalError):
        pyodbc.connect(dsn)
