BI MSSQL SQLAlchemy dialect
================================

A subclass of the standard mssql dialect with datalens-backend-specific modifications (e.g. parameterized queries with GROUP BY: https://github.com/level12/sqlalchemy_pyodbc_mssql)


Connection Parameters
=====================

Syntax for the connection string:

    .. code-block:: python

     'bi_mssql+pyodbc:///DRIVER={FreeTDS};Server=<host>;Port=<port>;Database=<db>;'
     'UID=<user>;PWD=<password>;TDS_Version=8.0'
