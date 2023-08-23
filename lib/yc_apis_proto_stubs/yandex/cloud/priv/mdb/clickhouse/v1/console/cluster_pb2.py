# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/clickhouse/v1/console/cluster.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from yandex.cloud.priv.mdb.clickhouse.v1 import cluster_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_cluster__pb2
from yandex.cloud.priv.mdb.clickhouse.v1 import version_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_version__pb2
from yandex.cloud.priv.mdb.clickhouse.v1.config import clickhouse_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_clickhouse_dot_v1_dot_config_dot_clickhouse__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n9yandex/cloud/priv/mdb/clickhouse/v1/console/cluster.proto\x12+yandex.cloud.priv.mdb.clickhouse.v1.console\x1a\x31yandex/cloud/priv/mdb/clickhouse/v1/cluster.proto\x1a\x31yandex/cloud/priv/mdb/clickhouse/v1/version.proto\x1a;yandex/cloud/priv/mdb/clickhouse/v1/config/clickhouse.proto\"\xfd\x13\n\x0e\x43lustersConfig\x12P\n\x0c\x63luster_name\x18\x01 \x01(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator\x12K\n\x07\x64\x62_name\x18\x02 \x01(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator\x12M\n\tuser_name\x18\x03 \x01(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator\x12P\n\x08password\x18\x04 \x01(\x0b\x32>.yandex.cloud.priv.mdb.clickhouse.v1.console.PasswordValidator\x12Q\n\rml_model_name\x18\x05 \x01(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator\x12P\n\x0cml_model_uri\x18\x06 \x01(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator\x12V\n\x12\x66ormat_schema_name\x18\x07 \x01(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator\x12U\n\x11\x66ormat_schema_uri\x18\x08 \x01(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator\x12\x66\n\x11host_count_limits\x18\t \x01(\x0b\x32K.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostCountLimits\x12X\n\nhost_types\x18\n \x03(\x0b\x32\x44.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType\x12\x10\n\x08versions\x18\x0b \x03(\t\x12H\n\x12\x61vailable_versions\x18\x0c \x03(\x0b\x32,.yandex.cloud.priv.mdb.clickhouse.v1.Version\x12\x17\n\x0f\x64\x65\x66\x61ult_version\x18\r \x01(\t\x1a\x8c\x02\n\x0fHostCountLimits\x12\x16\n\x0emin_host_count\x18\x01 \x01(\x03\x12\x16\n\x0emax_host_count\x18\x02 \x01(\x03\x12\x82\x01\n\x18host_count_per_disk_type\x18\x03 \x03(\x0b\x32`.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostCountLimits.HostCountPerDiskType\x1a\x44\n\x14HostCountPerDiskType\x12\x14\n\x0c\x64isk_type_id\x18\x01 \x01(\t\x12\x16\n\x0emin_host_count\x18\x02 \x01(\x03\x1a\x90\n\n\x08HostType\x12<\n\x04type\x18\x01 \x01(\x0e\x32..yandex.cloud.priv.mdb.clickhouse.v1.Host.Type\x12m\n\x10resource_presets\x18\x02 \x03(\x0b\x32S.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset\x12p\n\x11\x64\x65\x66\x61ult_resources\x18\x03 \x01(\x0b\x32U.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.DefaultResources\x1a\xdd\x06\n\x0eResourcePreset\x12\x11\n\tpreset_id\x18\x01 \x01(\t\x12\x11\n\tcpu_limit\x18\x02 \x01(\x03\x12\x14\n\x0cmemory_limit\x18\x03 \x01(\x03\x12\x12\n\ngeneration\x18\x05 \x01(\x03\x12\x17\n\x0fgeneration_name\x18\x06 \x01(\t\x12\x0c\n\x04type\x18\x07 \x01(\t\x12\x14\n\x0c\x63pu_fraction\x18\x08 \x01(\x03\x12\x17\n\x0f\x64\x65\x63ommissioning\x18\t \x01(\x08\x12g\n\x05zones\x18\x04 \x03(\x0b\x32X.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone\x1a\xbb\x04\n\x04Zone\x12\x0f\n\x07zone_id\x18\x01 \x01(\t\x12u\n\ndisk_types\x18\x02 \x03(\x0b\x32\x61.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone.DiskType\x1a\xaa\x03\n\x08\x44iskType\x12\x14\n\x0c\x64isk_type_id\x18\x01 \x01(\t\x12\x11\n\tmin_hosts\x18\x04 \x01(\x03\x12\x11\n\tmax_hosts\x18\x05 \x01(\x03\x12\x8a\x01\n\x0f\x64isk_size_range\x18\x02 \x01(\x0b\x32o.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone.DiskType.DiskSizeRangeH\x00\x12\x81\x01\n\ndisk_sizes\x18\x03 \x01(\x0b\x32k.yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone.DiskType.DiskSizesH\x00\x1a)\n\rDiskSizeRange\x12\x0b\n\x03min\x18\x01 \x01(\x03\x12\x0b\n\x03max\x18\x02 \x01(\x03\x1a\x1a\n\tDiskSizes\x12\r\n\x05sizes\x18\x01 \x03(\x03\x42\n\n\x08\x44iskSize\x1a\x84\x01\n\x10\x44\x65\x66\x61ultResources\x12\x1a\n\x12resource_preset_id\x18\x01 \x01(\t\x12\x14\n\x0c\x64isk_type_id\x18\x02 \x01(\t\x12\x11\n\tdisk_size\x18\x03 \x01(\x03\x12\x12\n\ngeneration\x18\x04 \x01(\x03\x12\x17\n\x0fgeneration_name\x18\x05 \x01(\t\"L\n\rNameValidator\x12\x0e\n\x06regexp\x18\x01 \x01(\t\x12\x0b\n\x03min\x18\x02 \x01(\x03\x12\x0b\n\x03max\x18\x03 \x01(\x03\x12\x11\n\tblacklist\x18\x04 \x03(\t\"\xfb\x01\n\x11PasswordValidator\x12\x0e\n\x06regexp\x18\x01 \x01(\t\x12\x0b\n\x03min\x18\x02 \x01(\x03\x12\x0b\n\x03max\x18\x03 \x01(\x03\x12\x11\n\tblacklist\x18\x04 \x03(\t\x12\x62\n\rpassword_type\x18\x05 \x01(\x0e\x32K.yandex.cloud.priv.mdb.clickhouse.v1.console.PasswordValidator.PasswordType\"E\n\x0cPasswordType\x12\x1d\n\x19PASSWORD_TYPE_UNSPECIFIED\x10\x00\x12\n\n\x06NORMAL\x10\x01\x12\n\n\x06STRICT\x10\x02\"\xb7\x03\n\rBillingMetric\x12\x11\n\tfolder_id\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12T\n\x04tags\x18\x03 \x01(\x0b\x32\x46.yandex.cloud.priv.mdb.clickhouse.v1.console.BillingMetric.BillingTags\x1a\xac\x02\n\x0b\x42illingTags\x12\x11\n\tpublic_ip\x18\x01 \x01(\x03\x12\x14\n\x0c\x64isk_type_id\x18\x02 \x01(\t\x12\x14\n\x0c\x63luster_type\x18\x03 \x01(\t\x12\x11\n\tdisk_size\x18\x04 \x01(\x03\x12\x1a\n\x12resource_preset_id\x18\x05 \x01(\t\x12\x13\n\x0bplatform_id\x18\x06 \x01(\t\x12\r\n\x05\x63ores\x18\x07 \x01(\x03\x12\x15\n\rcore_fraction\x18\x08 \x01(\x03\x12\x0e\n\x06memory\x18\t \x01(\x03\x12*\n\"software_accelerated_network_cores\x18\n \x01(\x03\x12\r\n\x05roles\x18\x0b \x03(\t\x12\x0e\n\x06online\x18\x0c \x01(\x03\x12\x19\n\x11on_dedicated_host\x18\r \x01(\x03\"^\n\x0f\x42illingEstimate\x12K\n\x07metrics\x18\x01 \x03(\x0b\x32:.yandex.cloud.priv.mdb.clickhouse.v1.console.BillingMetric\"\'\n\rClustersStats\x12\x16\n\x0e\x63lusters_count\x18\x01 \x01(\x03\"p\n\x10RestoreResources\x12\x1a\n\x12resource_preset_id\x18\x01 \x01(\t\x12\x14\n\x0c\x64isk_type_id\x18\x02 \x01(\t\x12\x11\n\tdisk_size\x18\x03 \x01(\x03\x12\x17\n\x0fmin_hosts_count\x18\x04 \x01(\x03\"\xde\x03\n\x0cRestoreHints\x12[\n\x14\x63lickhouse_resources\x18\x01 \x01(\x0b\x32=.yandex.cloud.priv.mdb.clickhouse.v1.console.RestoreResources\x12Z\n\x13zookeeper_resources\x18\x02 \x01(\x0b\x32=.yandex.cloud.priv.mdb.clickhouse.v1.console.RestoreResources\x12\x0f\n\x07version\x18\x03 \x01(\t\x12M\n\x0b\x65nvironment\x18\x04 \x01(\x0e\x32\x38.yandex.cloud.priv.mdb.clickhouse.v1.Cluster.Environment\x12\x12\n\nnetwork_id\x18\x05 \x01(\t\x12W\n\x11\x63lickhouse_config\x18\x06 \x01(\x0b\x32<.yandex.cloud.priv.mdb.clickhouse.v1.config.ClickhouseConfig\x12H\n\rcloud_storage\x18\x07 \x01(\x0b\x32\x31.yandex.cloud.priv.mdb.clickhouse.v1.CloudStorageBsB\x04PCCOZka.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/clickhouse/v1/console;clickhouse_consoleb\x06proto3')



_CLUSTERSCONFIG = DESCRIPTOR.message_types_by_name['ClustersConfig']
_CLUSTERSCONFIG_HOSTCOUNTLIMITS = _CLUSTERSCONFIG.nested_types_by_name['HostCountLimits']
_CLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE = _CLUSTERSCONFIG_HOSTCOUNTLIMITS.nested_types_by_name['HostCountPerDiskType']
_CLUSTERSCONFIG_HOSTTYPE = _CLUSTERSCONFIG.nested_types_by_name['HostType']
_CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET = _CLUSTERSCONFIG_HOSTTYPE.nested_types_by_name['ResourcePreset']
_CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE = _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET.nested_types_by_name['Zone']
_CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE = _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE.nested_types_by_name['DiskType']
_CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE = _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE.nested_types_by_name['DiskSizeRange']
_CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES = _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE.nested_types_by_name['DiskSizes']
_CLUSTERSCONFIG_HOSTTYPE_DEFAULTRESOURCES = _CLUSTERSCONFIG_HOSTTYPE.nested_types_by_name['DefaultResources']
_NAMEVALIDATOR = DESCRIPTOR.message_types_by_name['NameValidator']
_PASSWORDVALIDATOR = DESCRIPTOR.message_types_by_name['PasswordValidator']
_BILLINGMETRIC = DESCRIPTOR.message_types_by_name['BillingMetric']
_BILLINGMETRIC_BILLINGTAGS = _BILLINGMETRIC.nested_types_by_name['BillingTags']
_BILLINGESTIMATE = DESCRIPTOR.message_types_by_name['BillingEstimate']
_CLUSTERSSTATS = DESCRIPTOR.message_types_by_name['ClustersStats']
_RESTORERESOURCES = DESCRIPTOR.message_types_by_name['RestoreResources']
_RESTOREHINTS = DESCRIPTOR.message_types_by_name['RestoreHints']
_PASSWORDVALIDATOR_PASSWORDTYPE = _PASSWORDVALIDATOR.enum_types_by_name['PasswordType']
ClustersConfig = _reflection.GeneratedProtocolMessageType('ClustersConfig', (_message.Message,), {

  'HostCountLimits' : _reflection.GeneratedProtocolMessageType('HostCountLimits', (_message.Message,), {

    'HostCountPerDiskType' : _reflection.GeneratedProtocolMessageType('HostCountPerDiskType', (_message.Message,), {
      'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE,
      '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
      # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostCountLimits.HostCountPerDiskType)
      })
    ,
    'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTCOUNTLIMITS,
    '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostCountLimits)
    })
  ,

  'HostType' : _reflection.GeneratedProtocolMessageType('HostType', (_message.Message,), {

    'ResourcePreset' : _reflection.GeneratedProtocolMessageType('ResourcePreset', (_message.Message,), {

      'Zone' : _reflection.GeneratedProtocolMessageType('Zone', (_message.Message,), {

        'DiskType' : _reflection.GeneratedProtocolMessageType('DiskType', (_message.Message,), {

          'DiskSizeRange' : _reflection.GeneratedProtocolMessageType('DiskSizeRange', (_message.Message,), {
            'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE,
            '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
            # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone.DiskType.DiskSizeRange)
            })
          ,

          'DiskSizes' : _reflection.GeneratedProtocolMessageType('DiskSizes', (_message.Message,), {
            'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES,
            '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
            # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone.DiskType.DiskSizes)
            })
          ,
          'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE,
          '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
          # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone.DiskType)
          })
        ,
        'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE,
        '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
        # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset.Zone)
        })
      ,
      'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET,
      '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
      # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.ResourcePreset)
      })
    ,

    'DefaultResources' : _reflection.GeneratedProtocolMessageType('DefaultResources', (_message.Message,), {
      'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTTYPE_DEFAULTRESOURCES,
      '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
      # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType.DefaultResources)
      })
    ,
    'DESCRIPTOR' : _CLUSTERSCONFIG_HOSTTYPE,
    '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig.HostType)
    })
  ,
  'DESCRIPTOR' : _CLUSTERSCONFIG,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersConfig)
  })
_sym_db.RegisterMessage(ClustersConfig)
_sym_db.RegisterMessage(ClustersConfig.HostCountLimits)
_sym_db.RegisterMessage(ClustersConfig.HostCountLimits.HostCountPerDiskType)
_sym_db.RegisterMessage(ClustersConfig.HostType)
_sym_db.RegisterMessage(ClustersConfig.HostType.ResourcePreset)
_sym_db.RegisterMessage(ClustersConfig.HostType.ResourcePreset.Zone)
_sym_db.RegisterMessage(ClustersConfig.HostType.ResourcePreset.Zone.DiskType)
_sym_db.RegisterMessage(ClustersConfig.HostType.ResourcePreset.Zone.DiskType.DiskSizeRange)
_sym_db.RegisterMessage(ClustersConfig.HostType.ResourcePreset.Zone.DiskType.DiskSizes)
_sym_db.RegisterMessage(ClustersConfig.HostType.DefaultResources)

NameValidator = _reflection.GeneratedProtocolMessageType('NameValidator', (_message.Message,), {
  'DESCRIPTOR' : _NAMEVALIDATOR,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.NameValidator)
  })
_sym_db.RegisterMessage(NameValidator)

PasswordValidator = _reflection.GeneratedProtocolMessageType('PasswordValidator', (_message.Message,), {
  'DESCRIPTOR' : _PASSWORDVALIDATOR,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.PasswordValidator)
  })
_sym_db.RegisterMessage(PasswordValidator)

BillingMetric = _reflection.GeneratedProtocolMessageType('BillingMetric', (_message.Message,), {

  'BillingTags' : _reflection.GeneratedProtocolMessageType('BillingTags', (_message.Message,), {
    'DESCRIPTOR' : _BILLINGMETRIC_BILLINGTAGS,
    '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.BillingMetric.BillingTags)
    })
  ,
  'DESCRIPTOR' : _BILLINGMETRIC,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.BillingMetric)
  })
_sym_db.RegisterMessage(BillingMetric)
_sym_db.RegisterMessage(BillingMetric.BillingTags)

BillingEstimate = _reflection.GeneratedProtocolMessageType('BillingEstimate', (_message.Message,), {
  'DESCRIPTOR' : _BILLINGESTIMATE,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.BillingEstimate)
  })
_sym_db.RegisterMessage(BillingEstimate)

ClustersStats = _reflection.GeneratedProtocolMessageType('ClustersStats', (_message.Message,), {
  'DESCRIPTOR' : _CLUSTERSSTATS,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.ClustersStats)
  })
_sym_db.RegisterMessage(ClustersStats)

RestoreResources = _reflection.GeneratedProtocolMessageType('RestoreResources', (_message.Message,), {
  'DESCRIPTOR' : _RESTORERESOURCES,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.RestoreResources)
  })
_sym_db.RegisterMessage(RestoreResources)

RestoreHints = _reflection.GeneratedProtocolMessageType('RestoreHints', (_message.Message,), {
  'DESCRIPTOR' : _RESTOREHINTS,
  '__module__' : 'yandex.cloud.priv.mdb.clickhouse.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.clickhouse.v1.console.RestoreHints)
  })
_sym_db.RegisterMessage(RestoreHints)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\004PCCOZka.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/clickhouse/v1/console;clickhouse_console'
  _CLUSTERSCONFIG._serialized_start=270
  _CLUSTERSCONFIG._serialized_end=2827
  _CLUSTERSCONFIG_HOSTCOUNTLIMITS._serialized_start=1260
  _CLUSTERSCONFIG_HOSTCOUNTLIMITS._serialized_end=1528
  _CLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE._serialized_start=1460
  _CLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE._serialized_end=1528
  _CLUSTERSCONFIG_HOSTTYPE._serialized_start=1531
  _CLUSTERSCONFIG_HOSTTYPE._serialized_end=2827
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET._serialized_start=1831
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET._serialized_end=2692
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE._serialized_start=2121
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE._serialized_end=2692
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE._serialized_start=2266
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE._serialized_end=2692
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE._serialized_start=2611
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE._serialized_end=2652
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES._serialized_start=2654
  _CLUSTERSCONFIG_HOSTTYPE_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES._serialized_end=2680
  _CLUSTERSCONFIG_HOSTTYPE_DEFAULTRESOURCES._serialized_start=2695
  _CLUSTERSCONFIG_HOSTTYPE_DEFAULTRESOURCES._serialized_end=2827
  _NAMEVALIDATOR._serialized_start=2829
  _NAMEVALIDATOR._serialized_end=2905
  _PASSWORDVALIDATOR._serialized_start=2908
  _PASSWORDVALIDATOR._serialized_end=3159
  _PASSWORDVALIDATOR_PASSWORDTYPE._serialized_start=3090
  _PASSWORDVALIDATOR_PASSWORDTYPE._serialized_end=3159
  _BILLINGMETRIC._serialized_start=3162
  _BILLINGMETRIC._serialized_end=3601
  _BILLINGMETRIC_BILLINGTAGS._serialized_start=3301
  _BILLINGMETRIC_BILLINGTAGS._serialized_end=3601
  _BILLINGESTIMATE._serialized_start=3603
  _BILLINGESTIMATE._serialized_end=3697
  _CLUSTERSSTATS._serialized_start=3699
  _CLUSTERSSTATS._serialized_end=3738
  _RESTORERESOURCES._serialized_start=3740
  _RESTORERESOURCES._serialized_end=3852
  _RESTOREHINTS._serialized_start=3855
  _RESTOREHINTS._serialized_end=4333
# @@protoc_insertion_point(module_scope)
