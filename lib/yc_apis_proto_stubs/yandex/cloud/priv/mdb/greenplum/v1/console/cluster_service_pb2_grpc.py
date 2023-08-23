# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from yandex.cloud.priv.mdb.greenplum.v1 import cluster_service_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_cluster__service__pb2
from yandex.cloud.priv.mdb.greenplum.v1.console import cluster_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2
from yandex.cloud.priv.mdb.greenplum.v1.console import cluster_service_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2


class ClusterServiceStub(object):
    """A set of methods for managing Greenplum console support.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Get = channel.unary_unary(
                '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/Get',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetGreenplumClustersConfigRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.GreenplumClustersConfig.FromString,
                )
        self.EstimateCreate = channel.unary_unary(
                '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/EstimateCreate',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_cluster__service__pb2.CreateClusterRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.BillingEstimateResponse.FromString,
                )
        self.GetClustersStats = channel.unary_unary(
                '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/GetClustersStats',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetClustersStatsRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.ClustersStats.FromString,
                )
        self.GetCreateConfig = channel.unary_unary(
                '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/GetCreateConfig',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.CreateClusterConfigRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.JSONSchema.FromString,
                )
        self.GetRecommendedConfig = channel.unary_unary(
                '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/GetRecommendedConfig',
                request_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetRecommendedConfigRequest.SerializeToString,
                response_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetRecommendedConfigResponse.FromString,
                )


class ClusterServiceServicer(object):
    """A set of methods for managing Greenplum console support.
    """

    def Get(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def EstimateCreate(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetClustersStats(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetCreateConfig(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetRecommendedConfig(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ClusterServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Get': grpc.unary_unary_rpc_method_handler(
                    servicer.Get,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetGreenplumClustersConfigRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.GreenplumClustersConfig.SerializeToString,
            ),
            'EstimateCreate': grpc.unary_unary_rpc_method_handler(
                    servicer.EstimateCreate,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_cluster__service__pb2.CreateClusterRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.BillingEstimateResponse.SerializeToString,
            ),
            'GetClustersStats': grpc.unary_unary_rpc_method_handler(
                    servicer.GetClustersStats,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetClustersStatsRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.ClustersStats.SerializeToString,
            ),
            'GetCreateConfig': grpc.unary_unary_rpc_method_handler(
                    servicer.GetCreateConfig,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.CreateClusterConfigRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.JSONSchema.SerializeToString,
            ),
            'GetRecommendedConfig': grpc.unary_unary_rpc_method_handler(
                    servicer.GetRecommendedConfig,
                    request_deserializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetRecommendedConfigRequest.FromString,
                    response_serializer=yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetRecommendedConfigResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ClusterService(object):
    """A set of methods for managing Greenplum console support.
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
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/Get',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetGreenplumClustersConfigRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.GreenplumClustersConfig.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def EstimateCreate(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/EstimateCreate',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_cluster__service__pb2.CreateClusterRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.BillingEstimateResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetClustersStats(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/GetClustersStats',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetClustersStatsRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__pb2.ClustersStats.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetCreateConfig(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/GetCreateConfig',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.CreateClusterConfigRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.JSONSchema.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetRecommendedConfig(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/yandex.cloud.priv.mdb.greenplum.v1.console.ClusterService/GetRecommendedConfig',
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetRecommendedConfigRequest.SerializeToString,
            yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_console_dot_cluster__service__pb2.GetRecommendedConfigResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
