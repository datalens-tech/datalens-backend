import sys

import google.protobuf
import packaging.version


protobuf_version = packaging.version.Version(google.protobuf.__version__)


if protobuf_version < packaging.version.Version("4.0"):
    from ydb._grpc.v3.draft import *  # noqa
    from ydb._grpc.v3.draft.protos import *  # noqa

    sys.modules["ydb._grpc.common.draft"] = sys.modules["ydb._grpc.v3.draft"]
    sys.modules["ydb._grpc.common.draft.protos"] = sys.modules["ydb._grpc.v3.draft.protos"]
elif protobuf_version < packaging.version.Version("5.0"):
    from ydb._grpc.v4.draft import *  # noqa
    from ydb._grpc.v4.draft.protos import *  # noqa

    sys.modules["ydb._grpc.common.draft"] = sys.modules["ydb._grpc.v4.draft"]
    sys.modules["ydb._grpc.common.draft.protos"] = sys.modules["ydb._grpc.v4.draft.protos"]
else:
    from ydb._grpc.v5.draft import *  # noqa
    from ydb._grpc.v5.draft.protos import *  # noqa

    sys.modules["ydb._grpc.common.draft"] = sys.modules["ydb._grpc.v5.draft"]
    sys.modules["ydb._grpc.common.draft.protos"] = sys.modules["ydb._grpc.v5.draft.protos"]
