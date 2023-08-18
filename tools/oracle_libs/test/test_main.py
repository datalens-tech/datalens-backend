from __future__ import annotations

import pytest


def test_paths():
    import os
    data = {
        path: os.listdir(path)
        for path in os.environ['LD_LIBRARY_PATH'].split(':')}
    print('LD_LIBRARY_PATH contents:', data)
    assert data


def test_oracle():
    import oracledb
    oracledb.init_oracle_client()
    dsn = (
        # bi-formula's devenv port is `5030`,
        # but expecting this test to get connrefused.
        '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=15030))'
        '(CONNECT_DATA=(SERVICE_NAME=ORCLPDB1.localdomain)))'
    )
    # Failure example:
    #
    #   cx_Oracle.DatabaseError: DPI-1047: Cannot locate a 64-bit Oracle Client
    #   library: "libclntsh.so: cannot open shared object file: No such file or
    #   directory". See
    #   https://oracle.github.io/odpi/doc/installation.html#linux for help

    # Success example:
    #
    #   ...

    with pytest.raises(oracledb.DatabaseError) as exc_res:
        oracledb.connect(dsn=dsn, user='datalens', password='qwerty')
    assert str(exc_res.value) == 'ORA-12541: TNS:no listener'
