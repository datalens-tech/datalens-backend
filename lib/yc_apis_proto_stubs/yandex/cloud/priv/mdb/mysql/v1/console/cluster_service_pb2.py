# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/mysql/v1/console/cluster_service.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.api import annotations_pb2 as google_dot_api_dot_annotations__pb2
from yandex.cloud.api.tools import options_pb2 as yandex_dot_cloud_dot_api_dot_tools_dot_options__pb2
from yandex.cloud.priv.mdb.mysql.v1.console import cluster_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_mysql_dot_v1_dot_console_dot_cluster__pb2
from yandex.cloud.priv.mdb.mysql.v1 import cluster_service_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_mysql_dot_v1_dot_cluster__service__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n<yandex/cloud/priv/mdb/mysql/v1/console/cluster_service.proto\x12&yandex.cloud.priv.mdb.mysql.v1.console\x1a\x1cgoogle/api/annotations.proto\x1a$yandex/cloud/api/tools/options.proto\x1a\x34yandex/cloud/priv/mdb/mysql/v1/console/cluster.proto\x1a\x34yandex/cloud/priv/mdb/mysql/v1/cluster_service.proto\"J\n\x1dGetMysqlClustersConfigRequest\x12\x11\n\tfolder_id\x18\x01 \x01(\t\x12\x16\n\x0ehost_group_ids\x18\x02 \x03(\t\"\x19\n\x17GetClustersStatsRequest2\xcc\x04\n\x0e\x43lusterService\x12\xb8\x01\n\x03Get\x12\x45.yandex.cloud.priv.mdb.mysql.v1.console.GetMysqlClustersConfigRequest\x1a;.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig\"-\x82\xd3\xe4\x93\x02\'\x12%/mdb/mysql/v1/console/clusters:config\x12\xbb\x01\n\x0e\x45stimateCreate\x12\x34.yandex.cloud.priv.mdb.mysql.v1.CreateClusterRequest\x1a?.yandex.cloud.priv.mdb.mysql.v1.console.BillingEstimateResponse\"2\x82\xd3\xe4\x93\x02,\"\'/mdb/mysql/v1/console/clusters:estimate:\x01*\x12\xc0\x01\n\x10GetClustersStats\x12?.yandex.cloud.priv.mdb.mysql.v1.console.GetClustersStatsRequest\x1a\x35.yandex.cloud.priv.mdb.mysql.v1.console.ClustersStats\"4\x82\xd3\xe4\x93\x02&\x12$/mdb/mysql/v1/console/clusters:stats\xca\xef \x04\n\x02\x10\x01\x42jB\x05PMCOSZaa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/mysql/v1/console;mysql_consoleb\x06proto3')



_GETMYSQLCLUSTERSCONFIGREQUEST = DESCRIPTOR.message_types_by_name['GetMysqlClustersConfigRequest']
_GETCLUSTERSSTATSREQUEST = DESCRIPTOR.message_types_by_name['GetClustersStatsRequest']
GetMysqlClustersConfigRequest = _reflection.GeneratedProtocolMessageType('GetMysqlClustersConfigRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETMYSQLCLUSTERSCONFIGREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.GetMysqlClustersConfigRequest)
  })
_sym_db.RegisterMessage(GetMysqlClustersConfigRequest)

GetClustersStatsRequest = _reflection.GeneratedProtocolMessageType('GetClustersStatsRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETCLUSTERSSTATSREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.GetClustersStatsRequest)
  })
_sym_db.RegisterMessage(GetClustersStatsRequest)

_CLUSTERSERVICE = DESCRIPTOR.services_by_name['ClusterService']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\005PMCOSZaa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/mysql/v1/console;mysql_console'
  _CLUSTERSERVICE.methods_by_name['Get']._options = None
  _CLUSTERSERVICE.methods_by_name['Get']._serialized_options = b'\202\323\344\223\002\'\022%/mdb/mysql/v1/console/clusters:config'
  _CLUSTERSERVICE.methods_by_name['EstimateCreate']._options = None
  _CLUSTERSERVICE.methods_by_name['EstimateCreate']._serialized_options = b'\202\323\344\223\002,\"\'/mdb/mysql/v1/console/clusters:estimate:\001*'
  _CLUSTERSERVICE.methods_by_name['GetClustersStats']._options = None
  _CLUSTERSERVICE.methods_by_name['GetClustersStats']._serialized_options = b'\202\323\344\223\002&\022$/mdb/mysql/v1/console/clusters:stats\312\357 \004\n\002\020\001'
  _GETMYSQLCLUSTERSCONFIGREQUEST._serialized_start=280
  _GETMYSQLCLUSTERSCONFIGREQUEST._serialized_end=354
  _GETCLUSTERSSTATSREQUEST._serialized_start=356
  _GETCLUSTERSSTATSREQUEST._serialized_end=381
  _CLUSTERSERVICE._serialized_start=384
  _CLUSTERSERVICE._serialized_end=972
# @@protoc_insertion_point(module_scope)
