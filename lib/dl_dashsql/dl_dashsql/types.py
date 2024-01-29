import datetime


# Types for incoming parameter data
IncomingDSQLParamType = int | float | str | bool | None | datetime.datetime | datetime.date
IncomingDSQLParamTypeExt = IncomingDSQLParamType | list[IncomingDSQLParamType] | tuple[IncomingDSQLParamType, ...]
