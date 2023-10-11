import sys

import oracledb


oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
