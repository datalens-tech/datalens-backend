# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/greenplum/v1/cluster.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import wrappers_pb2 as google_dot_protobuf_dot_wrappers__pb2
from google.type import timeofday_pb2 as google_dot_type_dot_timeofday__pb2
from yandex.cloud.api.tools import options_pb2 as yandex_dot_cloud_dot_api_dot_tools_dot_options__pb2
from yandex.cloud.priv import validation_pb2 as yandex_dot_cloud_dot_priv_dot_validation__pb2
from yandex.cloud.priv.mdb.greenplum.v1 import config_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_config__pb2
from yandex.cloud.priv.mdb.greenplum.v1 import maintenance_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_maintenance__pb2
from yandex.cloud.priv.mdb.greenplum.v1 import pxf_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_pxf__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n0yandex/cloud/priv/mdb/greenplum/v1/cluster.proto\x12\"yandex.cloud.priv.mdb.greenplum.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/wrappers.proto\x1a\x1bgoogle/type/timeofday.proto\x1a$yandex/cloud/api/tools/options.proto\x1a\"yandex/cloud/priv/validation.proto\x1a/yandex/cloud/priv/mdb/greenplum/v1/config.proto\x1a\x34yandex/cloud/priv/mdb/greenplum/v1/maintenance.proto\x1a,yandex/cloud/priv/mdb/greenplum/v1/pxf.proto\"\xc9\x0c\n\x07\x43luster\x12\n\n\x02id\x18\x01 \x01(\t\x12\x11\n\tfolder_id\x18\x02 \x01(\t\x12.\n\ncreated_at\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0c\n\x04name\x18\x04 \x01(\t\x12\x43\n\x06\x63onfig\x18\x05 \x01(\x0b\x32\x33.yandex.cloud.priv.mdb.greenplum.v1.GreenplumConfig\x12\x13\n\x0b\x64\x65scription\x18\x06 \x01(\t\x12G\n\x06labels\x18\x07 \x03(\x0b\x32\x37.yandex.cloud.priv.mdb.greenplum.v1.Cluster.LabelsEntry\x12L\n\x0b\x65nvironment\x18\x08 \x01(\x0e\x32\x37.yandex.cloud.priv.mdb.greenplum.v1.Cluster.Environment\x12\x42\n\nmonitoring\x18\t \x03(\x0b\x32..yandex.cloud.priv.mdb.greenplum.v1.Monitoring\x12Q\n\rmaster_config\x18\n \x01(\x0b\x32:.yandex.cloud.priv.mdb.greenplum.v1.MasterSubclusterConfig\x12S\n\x0esegment_config\x18\x0b \x01(\x0b\x32;.yandex.cloud.priv.mdb.greenplum.v1.SegmentSubclusterConfig\x12\x19\n\x11master_host_count\x18\x0c \x01(\x03\x12\x1a\n\x12segment_host_count\x18\r \x01(\x03\x12\x17\n\x0fsegment_in_host\x18\x0e \x01(\x03\x12\x12\n\nnetwork_id\x18\x0f \x01(\t\x12\x42\n\x06health\x18\x10 \x01(\x0e\x32\x32.yandex.cloud.priv.mdb.greenplum.v1.Cluster.Health\x12\x42\n\x06status\x18\x11 \x01(\x0e\x32\x32.yandex.cloud.priv.mdb.greenplum.v1.Cluster.Status\x12Q\n\x12maintenance_window\x18\x12 \x01(\x0b\x32\x35.yandex.cloud.priv.mdb.greenplum.v1.MaintenanceWindow\x12S\n\x11planned_operation\x18\x13 \x01(\x0b\x32\x38.yandex.cloud.priv.mdb.greenplum.v1.MaintenanceOperation\x12\x1a\n\x12security_group_ids\x18\x14 \x03(\t\x12\x11\n\tuser_name\x18\x15 \x01(\t\x12\x1b\n\x13\x64\x65letion_protection\x18\x16 \x01(\x08\x12\x16\n\x0ehost_group_ids\x18\x17 \x03(\t\x12\x1a\n\x12placement_group_id\x18\x18 \x01(\t\x12L\n\x0e\x63luster_config\x18\x19 \x01(\x0b\x32\x34.yandex.cloud.priv.mdb.greenplum.v1.ClusterConfigSet\x12G\n\rcloud_storage\x18\x1a \x01(\x0b\x32\x30.yandex.cloud.priv.mdb.greenplum.v1.CloudStorage\x1a-\n\x0bLabelsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"I\n\x0b\x45nvironment\x12\x1b\n\x17\x45NVIRONMENT_UNSPECIFIED\x10\x00\x12\x0e\n\nPRODUCTION\x10\x01\x12\r\n\tPRESTABLE\x10\x02\"Y\n\x06Health\x12\x12\n\x0eHEALTH_UNKNOWN\x10\x00\x12\t\n\x05\x41LIVE\x10\x01\x12\x08\n\x04\x44\x45\x41\x44\x10\x02\x12\x0c\n\x08\x44\x45GRADED\x10\x03\x12\x0e\n\nUNBALANCED\x10\x04\x1a\x08\xca\xef \x04\x12\x02\x18\x01\"\x83\x01\n\x06Status\x12\x12\n\x0eSTATUS_UNKNOWN\x10\x00\x12\x0c\n\x08\x43REATING\x10\x01\x12\x0b\n\x07RUNNING\x10\x02\x12\t\n\x05\x45RROR\x10\x03\x12\x0c\n\x08UPDATING\x10\x04\x12\x0c\n\x08STOPPING\x10\x05\x12\x0b\n\x07STOPPED\x10\x06\x12\x0c\n\x08STARTING\x10\x07\x1a\x08\xca\xef \x04\x12\x02\x18\x01\"=\n\nMonitoring\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x02 \x01(\t\x12\x0c\n\x04link\x18\x03 \x01(\t\"\xdf\x02\n\x0fGreenplumConfig\x12\x0f\n\x07version\x18\x01 \x01(\t\x12\x33\n\x13\x62\x61\x63kup_window_start\x18\x02 \x01(\x0b\x32\x16.google.type.TimeOfDay\x12:\n\x06\x61\x63\x63\x65ss\x18\x03 \x01(\x0b\x32*.yandex.cloud.priv.mdb.greenplum.v1.Access\x12\x19\n\x07zone_id\x18\x04 \x01(\tB\x08\xca\x89\x31\x04<=50\x12\x1b\n\tsubnet_id\x18\x05 \x01(\tB\x08\xca\x89\x31\x04<=50\x12\x18\n\x10\x61ssign_public_ip\x18\x06 \x01(\x08\x12<\n\x18segment_mirroring_enable\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.BoolValue\x12:\n\x16segment_auto_rebalance\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.BoolValue\"\xaf\x07\n\x10\x43lusterConfigSet\x12x\n\x19greenplum_config_set_6_17\x18\x01 \x01(\x0b\x32:.yandex.cloud.priv.mdb.greenplum.v1.GreenplumConfigSet6_17H\x00R\x17greenplumConfigSet_6_17\x12x\n\x19greenplum_config_set_6_19\x18\x02 \x01(\x0b\x32:.yandex.cloud.priv.mdb.greenplum.v1.GreenplumConfigSet6_19H\x00R\x17greenplumConfigSet_6_19\x12x\n\x19greenplum_config_set_6_21\x18\x03 \x01(\x0b\x32:.yandex.cloud.priv.mdb.greenplum.v1.GreenplumConfigSet6_21H\x00R\x17greenplumConfigSet_6_21\x12x\n\x19greenplum_config_set_6_22\x18\x05 \x01(\x0b\x32:.yandex.cloud.priv.mdb.greenplum.v1.GreenplumConfigSet6_22H\x00R\x17greenplumConfigSet_6_22\x12o\n\x16greenplum_config_set_6\x18\t \x01(\x0b\x32\x37.yandex.cloud.priv.mdb.greenplum.v1.GreenplumConfigSet6H\x00R\x14greenplumConfigSet_6\x12K\n\x04pool\x18\x04 \x01(\x0b\x32=.yandex.cloud.priv.mdb.greenplum.v1.ConnectionPoolerConfigSet\x12]\n\x15\x62\x61\x63kground_activities\x18\x06 \x01(\x0b\x32>.yandex.cloud.priv.mdb.greenplum.v1.BackgroundActivitiesConfig\x12<\n\x04ldap\x18\x07 \x01(\x0b\x32..yandex.cloud.priv.mdb.greenplum.v1.LdapConfig\x12\x44\n\npxf_config\x18\x08 \x01(\x0b\x32\x30.yandex.cloud.priv.mdb.greenplum.v1.PXFConfigSetB\x12\n\x10greenplum_config\"\xdb\x01\n\x16GreenplumRestoreConfig\x12\x33\n\x13\x62\x61\x63kup_window_start\x18\x01 \x01(\x0b\x32\x16.google.type.TimeOfDay\x12:\n\x06\x61\x63\x63\x65ss\x18\x02 \x01(\x0b\x32*.yandex.cloud.priv.mdb.greenplum.v1.Access\x12\x19\n\x07zone_id\x18\x03 \x01(\tB\x08\xca\x89\x31\x04<=50\x12\x1b\n\tsubnet_id\x18\x04 \x01(\tB\x08\xca\x89\x31\x04<=50\x12\x18\n\x10\x61ssign_public_ip\x18\x05 \x01(\x08\"W\n\x06\x41\x63\x63\x65ss\x12\x11\n\tdata_lens\x18\x01 \x01(\x08\x12\x0f\n\x07web_sql\x18\x02 \x01(\x08\x12\x15\n\rdata_transfer\x18\x03 \x01(\x08\x12\x12\n\nserverless\x18\x04 \x01(\x08\"W\n\x10RestoreResources\x12\x1a\n\x12resource_preset_id\x18\x01 \x01(\t\x12\x11\n\tdisk_size\x18\x02 \x01(\x03\x12\x14\n\x0c\x64isk_type_id\x18\x03 \x01(\t\"\xb4\x03\n\x0cRestoreHints\x12L\n\x0b\x65nvironment\x18\x01 \x01(\x0e\x32\x37.yandex.cloud.priv.mdb.greenplum.v1.Cluster.Environment\x12\x12\n\nnetwork_id\x18\x02 \x01(\t\x12N\n\x10master_resources\x18\x03 \x01(\x0b\x32\x34.yandex.cloud.priv.mdb.greenplum.v1.RestoreResources\x12O\n\x11segment_resources\x18\x04 \x01(\x0b\x32\x34.yandex.cloud.priv.mdb.greenplum.v1.RestoreResources\x12\x0f\n\x07version\x18\x05 \x01(\t\x12(\n\x04time\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x19\n\x11master_host_count\x18\x07 \x01(\x03\x12\x1a\n\x12segment_host_count\x18\x08 \x01(\x03\x12\x17\n\x0fsegment_in_host\x18\t \x01(\x03\x12\x16\n\x0ehost_group_ids\x18\n \x03(\t\"\x1e\n\x0c\x43loudStorage\x12\x0e\n\x06\x65nable\x18\x01 \x01(\x08\x42`B\x03GPCZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplumb\x06proto3')



_CLUSTER = DESCRIPTOR.message_types_by_name['Cluster']
_CLUSTER_LABELSENTRY = _CLUSTER.nested_types_by_name['LabelsEntry']
_MONITORING = DESCRIPTOR.message_types_by_name['Monitoring']
_GREENPLUMCONFIG = DESCRIPTOR.message_types_by_name['GreenplumConfig']
_CLUSTERCONFIGSET = DESCRIPTOR.message_types_by_name['ClusterConfigSet']
_GREENPLUMRESTORECONFIG = DESCRIPTOR.message_types_by_name['GreenplumRestoreConfig']
_ACCESS = DESCRIPTOR.message_types_by_name['Access']
_RESTORERESOURCES = DESCRIPTOR.message_types_by_name['RestoreResources']
_RESTOREHINTS = DESCRIPTOR.message_types_by_name['RestoreHints']
_CLOUDSTORAGE = DESCRIPTOR.message_types_by_name['CloudStorage']
_CLUSTER_ENVIRONMENT = _CLUSTER.enum_types_by_name['Environment']
_CLUSTER_HEALTH = _CLUSTER.enum_types_by_name['Health']
_CLUSTER_STATUS = _CLUSTER.enum_types_by_name['Status']
Cluster = _reflection.GeneratedProtocolMessageType('Cluster', (_message.Message,), {

  'LabelsEntry' : _reflection.GeneratedProtocolMessageType('LabelsEntry', (_message.Message,), {
    'DESCRIPTOR' : _CLUSTER_LABELSENTRY,
    '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.Cluster.LabelsEntry)
    })
  ,
  'DESCRIPTOR' : _CLUSTER,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.Cluster)
  })
_sym_db.RegisterMessage(Cluster)
_sym_db.RegisterMessage(Cluster.LabelsEntry)

Monitoring = _reflection.GeneratedProtocolMessageType('Monitoring', (_message.Message,), {
  'DESCRIPTOR' : _MONITORING,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.Monitoring)
  })
_sym_db.RegisterMessage(Monitoring)

GreenplumConfig = _reflection.GeneratedProtocolMessageType('GreenplumConfig', (_message.Message,), {
  'DESCRIPTOR' : _GREENPLUMCONFIG,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.GreenplumConfig)
  })
_sym_db.RegisterMessage(GreenplumConfig)

ClusterConfigSet = _reflection.GeneratedProtocolMessageType('ClusterConfigSet', (_message.Message,), {
  'DESCRIPTOR' : _CLUSTERCONFIGSET,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.ClusterConfigSet)
  })
_sym_db.RegisterMessage(ClusterConfigSet)

GreenplumRestoreConfig = _reflection.GeneratedProtocolMessageType('GreenplumRestoreConfig', (_message.Message,), {
  'DESCRIPTOR' : _GREENPLUMRESTORECONFIG,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.GreenplumRestoreConfig)
  })
_sym_db.RegisterMessage(GreenplumRestoreConfig)

Access = _reflection.GeneratedProtocolMessageType('Access', (_message.Message,), {
  'DESCRIPTOR' : _ACCESS,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.Access)
  })
_sym_db.RegisterMessage(Access)

RestoreResources = _reflection.GeneratedProtocolMessageType('RestoreResources', (_message.Message,), {
  'DESCRIPTOR' : _RESTORERESOURCES,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.RestoreResources)
  })
_sym_db.RegisterMessage(RestoreResources)

RestoreHints = _reflection.GeneratedProtocolMessageType('RestoreHints', (_message.Message,), {
  'DESCRIPTOR' : _RESTOREHINTS,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.RestoreHints)
  })
_sym_db.RegisterMessage(RestoreHints)

CloudStorage = _reflection.GeneratedProtocolMessageType('CloudStorage', (_message.Message,), {
  'DESCRIPTOR' : _CLOUDSTORAGE,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.CloudStorage)
  })
_sym_db.RegisterMessage(CloudStorage)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\003GPCZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplum'
  _CLUSTER_LABELSENTRY._options = None
  _CLUSTER_LABELSENTRY._serialized_options = b'8\001'
  _CLUSTER_HEALTH._options = None
  _CLUSTER_HEALTH._serialized_options = b'\312\357 \004\022\002\030\001'
  _CLUSTER_STATUS._options = None
  _CLUSTER_STATUS._serialized_options = b'\312\357 \004\022\002\030\001'
  _GREENPLUMCONFIG.fields_by_name['zone_id']._options = None
  _GREENPLUMCONFIG.fields_by_name['zone_id']._serialized_options = b'\312\2111\004<=50'
  _GREENPLUMCONFIG.fields_by_name['subnet_id']._options = None
  _GREENPLUMCONFIG.fields_by_name['subnet_id']._serialized_options = b'\312\2111\004<=50'
  _GREENPLUMRESTORECONFIG.fields_by_name['zone_id']._options = None
  _GREENPLUMRESTORECONFIG.fields_by_name['zone_id']._serialized_options = b'\312\2111\004<=50'
  _GREENPLUMRESTORECONFIG.fields_by_name['subnet_id']._options = None
  _GREENPLUMRESTORECONFIG.fields_by_name['subnet_id']._serialized_options = b'\312\2111\004<=50'
  _CLUSTER._serialized_start=406
  _CLUSTER._serialized_end=2015
  _CLUSTER_LABELSENTRY._serialized_start=1670
  _CLUSTER_LABELSENTRY._serialized_end=1715
  _CLUSTER_ENVIRONMENT._serialized_start=1717
  _CLUSTER_ENVIRONMENT._serialized_end=1790
  _CLUSTER_HEALTH._serialized_start=1792
  _CLUSTER_HEALTH._serialized_end=1881
  _CLUSTER_STATUS._serialized_start=1884
  _CLUSTER_STATUS._serialized_end=2015
  _MONITORING._serialized_start=2017
  _MONITORING._serialized_end=2078
  _GREENPLUMCONFIG._serialized_start=2081
  _GREENPLUMCONFIG._serialized_end=2432
  _CLUSTERCONFIGSET._serialized_start=2435
  _CLUSTERCONFIGSET._serialized_end=3378
  _GREENPLUMRESTORECONFIG._serialized_start=3381
  _GREENPLUMRESTORECONFIG._serialized_end=3600
  _ACCESS._serialized_start=3602
  _ACCESS._serialized_end=3689
  _RESTORERESOURCES._serialized_start=3691
  _RESTORERESOURCES._serialized_end=3778
  _RESTOREHINTS._serialized_start=3781
  _RESTOREHINTS._serialized_end=4217
  _CLOUDSTORAGE._serialized_start=4219
  _CLOUDSTORAGE._serialized_end=4249
# @@protoc_insertion_point(module_scope)
