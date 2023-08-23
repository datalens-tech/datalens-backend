# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from yandex.cloud.priv.mdb.clickhouse.v1 import database_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__pb2
from yandex.cloud.priv.mdb.clickhouse.v1 import database_service_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2
from yandex.cloud.priv.operation import operation_pb2 as yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2


class DatabaseServiceStub(object):
    """A set of methods for managing ClickHouse databases.
    NOTE: these methods are available only if database management through SQL is disabled.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Get = channel.unary_unary(
                '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/Get',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.GetDatabaseRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__pb2.Database.FromString,
                )
        self.List = channel.unary_unary(
                '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/List',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesResponse.FromString,
                )
        self.ListAtRevision = channel.unary_unary(
                '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/ListAtRevision',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesAtRevisionRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesResponse.FromString,
                )
        self.Create = channel.unary_unary(
                '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/Create',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.CreateDatabaseRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2.Operation.FromString,
                )
        self.Delete = channel.unary_unary(
                '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/Delete',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.DeleteDatabaseRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2.Operation.FromString,
                )


class DatabaseServiceServicer(object):
    """A set of methods for managing ClickHouse databases.
    NOTE: these methods are available only if database management through SQL is disabled.
    """

    def Get(self, request, context):
        """Returns the specified ClickHouse database.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def List(self, request, context):
        """Retrieves a list of ClickHouse databases.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListAtRevision(self, request, context):
        """Retrieves a list of ClickHouse databases in the specified cluster at specified revision.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Create(self, request, context):
        """Creates a new ClickHouse database.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Delete(self, request, context):
        """Deletes the specified ClickHouse database.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DatabaseServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Get': grpc.unary_unary_rpc_method_handler(
                    servicer.Get,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.GetDatabaseRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__pb2.Database.SerializeToString,
            ),
            'List': grpc.unary_unary_rpc_method_handler(
                    servicer.List,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesResponse.SerializeToString,
            ),
            'ListAtRevision': grpc.unary_unary_rpc_method_handler(
                    servicer.ListAtRevision,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesAtRevisionRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesResponse.SerializeToString,
            ),
            'Create': grpc.unary_unary_rpc_method_handler(
                    servicer.Create,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.CreateDatabaseRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2.Operation.SerializeToString,
            ),
            'Delete': grpc.unary_unary_rpc_method_handler(
                    servicer.Delete,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.DeleteDatabaseRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2.Operation.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class DatabaseService(object):
    """A set of methods for managing ClickHouse databases.
    NOTE: these methods are available only if database management through SQL is disabled.
    """

    @staticmethod
    def Get(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/Get',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.GetDatabaseRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__pb2.Database.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def List(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/List',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ListAtRevision(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/ListAtRevision',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesAtRevisionRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.ListDatabasesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Create(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/Create',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.CreateDatabaseRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2.Operation.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Delete(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.clickhouse.v1.DatabaseService/Delete',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_database__service__pb2.DeleteDatabaseRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2.Operation.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
