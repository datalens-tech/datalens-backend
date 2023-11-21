try:
    from ydb_proto_stubs_import import init_ydb_stubs

    init_ydb_stubs()
except ImportError:
    pass  # stubs will be initialized from the ydb package
