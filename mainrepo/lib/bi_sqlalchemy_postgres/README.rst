BI PostgreSQL SQLAlchemy dialect
================================

A subclass of the standard postgresql dialect with datalens-backend-specific modifications.


Connection Parameters
=====================

Syntax for the connection string:

    .. code-block:: python

     'bi_postgresql://<user>:<password>@<host>:<port>/<database>[?key=value..]'
