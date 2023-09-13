import abc

from yandex.cloud.priv.mdb.clickhouse.v1 import cluster_service_pb2 as clickhouse_cluster_service_pb2
from yandex.cloud.priv.mdb.clickhouse.v1 import cluster_service_pb2_grpc as clickhouse_cluster_service_pb2_grpc
from yandex.cloud.priv.mdb.postgresql.v1 import cluster_service_pb2 as postgresql_cluster_service_pb2
from yandex.cloud.priv.mdb.postgresql.v1 import cluster_service_pb2_grpc as postgresql_cluster_service_pb2_grpc
from yandex.cloud.priv.mdb.mysql.v1 import cluster_service_pb2 as mysql_cluster_service_pb2
from yandex.cloud.priv.mdb.mysql.v1 import cluster_service_pb2_grpc as mysql_cluster_service_pb2_grpc
from yandex.cloud.priv.mdb.greenplum.v1 import cluster_service_pb2 as greenplum_cluster_service_pb2
from yandex.cloud.priv.mdb.greenplum.v1 import cluster_service_pb2_grpc as greenplum_cluster_service_pb2_grpc

from bi_cloud_integration.yc_client_base import DLYCSingleServiceClient


class MDBClusterServiceBaseClient(DLYCSingleServiceClient):
    # endpoint: 'mdb-internal-api.private-api.cloud.yandex.net:443'

    @abc.abstractmethod
    async def get_cluster_folder_id(self, cluster_id: str) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_cluster_hosts(self, cluster_id: str) -> list[str]:
        raise NotImplementedError


class MDBClickHouseClusterServiceClient(MDBClusterServiceBaseClient):
    service_cls = clickhouse_cluster_service_pb2_grpc.ClusterServiceStub

    async def get_cluster_folder_id(self, cluster_id: str) -> str:
        req = clickhouse_cluster_service_pb2.GetClusterRequest(cluster_id=cluster_id)
        resp = await self.service.Get.aio(req)
        return resp.folder_id

    async def get_cluster_hosts(self, cluster_id: str) -> list[str]:
        req = clickhouse_cluster_service_pb2.ListClusterHostsRequest(cluster_id=cluster_id)
        resp = await self.service.ListHosts.aio(req)
        return [h.name for h in resp.hosts]


class MDBPostgreSQLClusterServiceClient(MDBClusterServiceBaseClient):
    service_cls = postgresql_cluster_service_pb2_grpc.ClusterServiceStub

    async def get_cluster_folder_id(self, cluster_id: str) -> str:
        req = postgresql_cluster_service_pb2.GetClusterRequest(cluster_id=cluster_id)
        resp = await self.service.Get.aio(req)
        return resp.folder_id

    async def get_cluster_hosts(self, cluster_id: str) -> list[str]:
        req = postgresql_cluster_service_pb2.ListClusterHostsRequest(cluster_id=cluster_id)
        resp = await self.service.ListHosts.aio(req)
        return [h.name for h in resp.hosts]


class MDBMySQLClusterServiceClient(MDBClusterServiceBaseClient):
    service_cls = mysql_cluster_service_pb2_grpc.ClusterServiceStub

    async def get_cluster_folder_id(self, cluster_id: str) -> str:
        req = mysql_cluster_service_pb2.GetClusterRequest(cluster_id=cluster_id)
        resp = await self.service.Get.aio(req)
        return resp.folder_id

    async def get_cluster_hosts(self, cluster_id: str) -> list[str]:
        req = mysql_cluster_service_pb2.ListClusterHostsRequest(cluster_id=cluster_id)
        resp = await self.service.ListHosts.aio(req)
        return [h.name for h in resp.hosts]


class MDBGreenplumClusterServiceClient(MDBClusterServiceBaseClient):
    service_cls = greenplum_cluster_service_pb2_grpc.ClusterServiceStub

    async def get_cluster_folder_id(self, cluster_id: str) -> str:
        req = greenplum_cluster_service_pb2.GetClusterRequest(cluster_id=cluster_id)
        resp = await self.service.Get.aio(req)
        return resp.folder_id

    async def get_cluster_hosts(self, cluster_id: str) -> list[str]:
        req = greenplum_cluster_service_pb2.ListClusterHostsRequest(cluster_id=cluster_id)
        resp = await self.service.ListHosts.aio(req)
        return [h.name for h in resp.hosts]
