# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from yandex.cloud.priv.mdb.clickhouse.v1 import versions_service_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_versions__service__pb2


class VersionsServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.List = channel.unary_unary(
                '/yandex.cloud.priv.mdb.clickhouse.v1.VersionsService/List',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_versions__service__pb2.ListVersionsRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_versions__service__pb2.ListVersionsResponse.FromString,
                )


class VersionsServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def List(self, request, context):
        """Returns list of available ClickHouse versions.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_VersionsServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'List': grpc.unary_unary_rpc_method_handler(
                    servicer.List,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_versions__service__pb2.ListVersionsRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_versions__service__pb2.ListVersionsResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'yandex.cloud.priv.mdb.clickhouse.v1.VersionsService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class VersionsService(object):
    """Missing associated documentation comment in .proto file."""

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
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.clickhouse.v1.VersionsService/List',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_versions__service__pb2.ListVersionsRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_versions__service__pb2.ListVersionsResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
