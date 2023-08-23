# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/mysql/v1/console/cluster.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n4yandex/cloud/priv/mdb/mysql/v1/console/cluster.proto\x12&yandex.cloud.priv.mdb.mysql.v1.console\x1a\x19google/protobuf/any.proto\"\xac\x10\n\x13MysqlClustersConfig\x12K\n\x0c\x63luster_name\x18\x01 \x01(\x0b\x32\x35.yandex.cloud.priv.mdb.mysql.v1.console.NameValidator\x12\x46\n\x07\x64\x62_name\x18\x02 \x01(\x0b\x32\x35.yandex.cloud.priv.mdb.mysql.v1.console.NameValidator\x12H\n\tuser_name\x18\x03 \x01(\x0b\x32\x35.yandex.cloud.priv.mdb.mysql.v1.console.NameValidator\x12K\n\x08password\x18\x04 \x01(\x0b\x32\x39.yandex.cloud.priv.mdb.mysql.v1.console.PasswordValidator\x12\x66\n\x11host_count_limits\x18\x05 \x01(\x0b\x32K.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.HostCountLimits\x12\x64\n\x10resource_presets\x18\x06 \x03(\x0b\x32J.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset\x12\x10\n\x08versions\x18\x07 \x03(\t\x12g\n\x11\x64\x65\x66\x61ult_resources\x18\x08 \x01(\x0b\x32L.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.DefaultResources\x12\x63\n\x12\x61vailable_versions\x18\t \x03(\x0b\x32G.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.VersionInfo\x12\x17\n\x0f\x64\x65\x66\x61ult_version\x18\n \x01(\t\x1a\x8c\x02\n\x0fHostCountLimits\x12\x16\n\x0emin_host_count\x18\x01 \x01(\x03\x12\x16\n\x0emax_host_count\x18\x02 \x01(\x03\x12\x82\x01\n\x18host_count_per_disk_type\x18\x03 \x03(\x0b\x32`.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.HostCountLimits.HostCountPerDiskType\x1a\x44\n\x14HostCountPerDiskType\x12\x14\n\x0c\x64isk_type_id\x18\x01 \x01(\t\x12\x16\n\x0emin_host_count\x18\x02 \x01(\x03\x1a\xb8\x06\n\x0eResourcePreset\x12\x11\n\tpreset_id\x18\x01 \x01(\t\x12\x11\n\tcpu_limit\x18\x02 \x01(\x03\x12\x14\n\x0cmemory_limit\x18\x03 \x01(\x03\x12\x12\n\ngeneration\x18\x05 \x01(\x03\x12\x17\n\x0fgeneration_name\x18\x06 \x01(\t\x12\x0c\n\x04type\x18\x07 \x01(\t\x12\x14\n\x0c\x63pu_fraction\x18\x08 \x01(\x03\x12\x17\n\x0f\x64\x65\x63ommissioning\x18\t \x01(\x08\x12^\n\x05zones\x18\x04 \x03(\x0b\x32O.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone\x1a\x9f\x04\n\x04Zone\x12\x0f\n\x07zone_id\x18\x01 \x01(\t\x12l\n\ndisk_types\x18\x02 \x03(\x0b\x32X.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone.DiskType\x1a\x97\x03\n\x08\x44iskType\x12\x14\n\x0c\x64isk_type_id\x18\x01 \x01(\t\x12\x11\n\tmin_hosts\x18\x04 \x01(\x03\x12\x11\n\tmax_hosts\x18\x05 \x01(\x03\x12\x81\x01\n\x0f\x64isk_size_range\x18\x02 \x01(\x0b\x32\x66.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone.DiskType.DiskSizeRangeH\x00\x12x\n\ndisk_sizes\x18\x03 \x01(\x0b\x32\x62.yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone.DiskType.DiskSizesH\x00\x1a)\n\rDiskSizeRange\x12\x0b\n\x03min\x18\x01 \x01(\x03\x12\x0b\n\x03max\x18\x02 \x01(\x03\x1a\x1a\n\tDiskSizes\x12\r\n\x05sizes\x18\x01 \x03(\x03\x42\n\n\x08\x44iskSize\x1a\x84\x01\n\x10\x44\x65\x66\x61ultResources\x12\x1a\n\x12resource_preset_id\x18\x01 \x01(\t\x12\x14\n\x0c\x64isk_type_id\x18\x02 \x01(\t\x12\x11\n\tdisk_size\x18\x03 \x01(\x03\x12\x12\n\ngeneration\x18\x04 \x01(\x03\x12\x17\n\x0fgeneration_name\x18\x05 \x01(\t\x1aQ\n\x0bVersionInfo\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x12\n\ndeprecated\x18\x03 \x01(\x08\x12\x14\n\x0cupdatable_to\x18\x04 \x03(\t\"L\n\rNameValidator\x12\x0e\n\x06regexp\x18\x01 \x01(\t\x12\x0b\n\x03min\x18\x02 \x01(\x03\x12\x0b\n\x03max\x18\x03 \x01(\x03\x12\x11\n\tblacklist\x18\x04 \x03(\t\"\xf6\x01\n\x11PasswordValidator\x12\x0e\n\x06regexp\x18\x01 \x01(\t\x12\x0b\n\x03min\x18\x02 \x01(\x03\x12\x0b\n\x03max\x18\x03 \x01(\x03\x12\x11\n\tblacklist\x18\x04 \x03(\t\x12]\n\rpassword_type\x18\x05 \x01(\x0e\x32\x46.yandex.cloud.priv.mdb.mysql.v1.console.PasswordValidator.PasswordType\"E\n\x0cPasswordType\x12\x1d\n\x19PASSWORD_TYPE_UNSPECIFIED\x10\x00\x12\n\n\x06NORMAL\x10\x01\x12\n\n\x06STRICT\x10\x02\"\xc4\x01\n\rBillingMetric\x12\x11\n\tfolder_id\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12M\n\x04tags\x18\x03 \x03(\x0b\x32?.yandex.cloud.priv.mdb.mysql.v1.console.BillingMetric.TagsEntry\x1a\x41\n\tTagsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.google.protobuf.Any:\x02\x38\x01\"a\n\x17\x42illingEstimateResponse\x12\x46\n\x07metrics\x18\x01 \x03(\x0b\x32\x35.yandex.cloud.priv.mdb.mysql.v1.console.BillingMetric\"\'\n\rClustersStats\x12\x16\n\x0e\x63lusters_count\x18\x01 \x01(\x03\x42iB\x04PMCOZaa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/mysql/v1/console;mysql_consoleb\x06proto3')



_MYSQLCLUSTERSCONFIG = DESCRIPTOR.message_types_by_name['MysqlClustersConfig']
_MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS = _MYSQLCLUSTERSCONFIG.nested_types_by_name['HostCountLimits']
_MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE = _MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS.nested_types_by_name['HostCountPerDiskType']
_MYSQLCLUSTERSCONFIG_RESOURCEPRESET = _MYSQLCLUSTERSCONFIG.nested_types_by_name['ResourcePreset']
_MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE = _MYSQLCLUSTERSCONFIG_RESOURCEPRESET.nested_types_by_name['Zone']
_MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE = _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE.nested_types_by_name['DiskType']
_MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE = _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE.nested_types_by_name['DiskSizeRange']
_MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES = _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE.nested_types_by_name['DiskSizes']
_MYSQLCLUSTERSCONFIG_DEFAULTRESOURCES = _MYSQLCLUSTERSCONFIG.nested_types_by_name['DefaultResources']
_MYSQLCLUSTERSCONFIG_VERSIONINFO = _MYSQLCLUSTERSCONFIG.nested_types_by_name['VersionInfo']
_NAMEVALIDATOR = DESCRIPTOR.message_types_by_name['NameValidator']
_PASSWORDVALIDATOR = DESCRIPTOR.message_types_by_name['PasswordValidator']
_BILLINGMETRIC = DESCRIPTOR.message_types_by_name['BillingMetric']
_BILLINGMETRIC_TAGSENTRY = _BILLINGMETRIC.nested_types_by_name['TagsEntry']
_BILLINGESTIMATERESPONSE = DESCRIPTOR.message_types_by_name['BillingEstimateResponse']
_CLUSTERSSTATS = DESCRIPTOR.message_types_by_name['ClustersStats']
_PASSWORDVALIDATOR_PASSWORDTYPE = _PASSWORDVALIDATOR.enum_types_by_name['PasswordType']
MysqlClustersConfig = _reflection.GeneratedProtocolMessageType('MysqlClustersConfig', (_message.Message,), {

  'HostCountLimits' : _reflection.GeneratedProtocolMessageType('HostCountLimits', (_message.Message,), {

    'HostCountPerDiskType' : _reflection.GeneratedProtocolMessageType('HostCountPerDiskType', (_message.Message,), {
      'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE,
      '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
      # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.HostCountLimits.HostCountPerDiskType)
      })
    ,
    'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS,
    '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.HostCountLimits)
    })
  ,

  'ResourcePreset' : _reflection.GeneratedProtocolMessageType('ResourcePreset', (_message.Message,), {

    'Zone' : _reflection.GeneratedProtocolMessageType('Zone', (_message.Message,), {

      'DiskType' : _reflection.GeneratedProtocolMessageType('DiskType', (_message.Message,), {

        'DiskSizeRange' : _reflection.GeneratedProtocolMessageType('DiskSizeRange', (_message.Message,), {
          'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE,
          '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
          # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone.DiskType.DiskSizeRange)
          })
        ,

        'DiskSizes' : _reflection.GeneratedProtocolMessageType('DiskSizes', (_message.Message,), {
          'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES,
          '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
          # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone.DiskType.DiskSizes)
          })
        ,
        'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE,
        '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
        # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone.DiskType)
        })
      ,
      'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE,
      '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
      # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset.Zone)
      })
    ,
    'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_RESOURCEPRESET,
    '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.ResourcePreset)
    })
  ,

  'DefaultResources' : _reflection.GeneratedProtocolMessageType('DefaultResources', (_message.Message,), {
    'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_DEFAULTRESOURCES,
    '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.DefaultResources)
    })
  ,

  'VersionInfo' : _reflection.GeneratedProtocolMessageType('VersionInfo', (_message.Message,), {
    'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG_VERSIONINFO,
    '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig.VersionInfo)
    })
  ,
  'DESCRIPTOR' : _MYSQLCLUSTERSCONFIG,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.MysqlClustersConfig)
  })
_sym_db.RegisterMessage(MysqlClustersConfig)
_sym_db.RegisterMessage(MysqlClustersConfig.HostCountLimits)
_sym_db.RegisterMessage(MysqlClustersConfig.HostCountLimits.HostCountPerDiskType)
_sym_db.RegisterMessage(MysqlClustersConfig.ResourcePreset)
_sym_db.RegisterMessage(MysqlClustersConfig.ResourcePreset.Zone)
_sym_db.RegisterMessage(MysqlClustersConfig.ResourcePreset.Zone.DiskType)
_sym_db.RegisterMessage(MysqlClustersConfig.ResourcePreset.Zone.DiskType.DiskSizeRange)
_sym_db.RegisterMessage(MysqlClustersConfig.ResourcePreset.Zone.DiskType.DiskSizes)
_sym_db.RegisterMessage(MysqlClustersConfig.DefaultResources)
_sym_db.RegisterMessage(MysqlClustersConfig.VersionInfo)

NameValidator = _reflection.GeneratedProtocolMessageType('NameValidator', (_message.Message,), {
  'DESCRIPTOR' : _NAMEVALIDATOR,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.NameValidator)
  })
_sym_db.RegisterMessage(NameValidator)

PasswordValidator = _reflection.GeneratedProtocolMessageType('PasswordValidator', (_message.Message,), {
  'DESCRIPTOR' : _PASSWORDVALIDATOR,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.PasswordValidator)
  })
_sym_db.RegisterMessage(PasswordValidator)

BillingMetric = _reflection.GeneratedProtocolMessageType('BillingMetric', (_message.Message,), {

  'TagsEntry' : _reflection.GeneratedProtocolMessageType('TagsEntry', (_message.Message,), {
    'DESCRIPTOR' : _BILLINGMETRIC_TAGSENTRY,
    '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
    # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.BillingMetric.TagsEntry)
    })
  ,
  'DESCRIPTOR' : _BILLINGMETRIC,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.BillingMetric)
  })
_sym_db.RegisterMessage(BillingMetric)
_sym_db.RegisterMessage(BillingMetric.TagsEntry)

BillingEstimateResponse = _reflection.GeneratedProtocolMessageType('BillingEstimateResponse', (_message.Message,), {
  'DESCRIPTOR' : _BILLINGESTIMATERESPONSE,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.BillingEstimateResponse)
  })
_sym_db.RegisterMessage(BillingEstimateResponse)

ClustersStats = _reflection.GeneratedProtocolMessageType('ClustersStats', (_message.Message,), {
  'DESCRIPTOR' : _CLUSTERSSTATS,
  '__module__' : 'yandex.cloud.priv.mdb.mysql.v1.console.cluster_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.mysql.v1.console.ClustersStats)
  })
_sym_db.RegisterMessage(ClustersStats)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\004PMCOZaa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/mysql/v1/console;mysql_console'
  _BILLINGMETRIC_TAGSENTRY._options = None
  _BILLINGMETRIC_TAGSENTRY._serialized_options = b'8\001'
  _MYSQLCLUSTERSCONFIG._serialized_start=124
  _MYSQLCLUSTERSCONFIG._serialized_end=2216
  _MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS._serialized_start=903
  _MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS._serialized_end=1171
  _MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE._serialized_start=1103
  _MYSQLCLUSTERSCONFIG_HOSTCOUNTLIMITS_HOSTCOUNTPERDISKTYPE._serialized_end=1171
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET._serialized_start=1174
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET._serialized_end=1998
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE._serialized_start=1455
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE._serialized_end=1998
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE._serialized_start=1591
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE._serialized_end=1998
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE._serialized_start=1917
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZERANGE._serialized_end=1958
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES._serialized_start=1960
  _MYSQLCLUSTERSCONFIG_RESOURCEPRESET_ZONE_DISKTYPE_DISKSIZES._serialized_end=1986
  _MYSQLCLUSTERSCONFIG_DEFAULTRESOURCES._serialized_start=2001
  _MYSQLCLUSTERSCONFIG_DEFAULTRESOURCES._serialized_end=2133
  _MYSQLCLUSTERSCONFIG_VERSIONINFO._serialized_start=2135
  _MYSQLCLUSTERSCONFIG_VERSIONINFO._serialized_end=2216
  _NAMEVALIDATOR._serialized_start=2218
  _NAMEVALIDATOR._serialized_end=2294
  _PASSWORDVALIDATOR._serialized_start=2297
  _PASSWORDVALIDATOR._serialized_end=2543
  _PASSWORDVALIDATOR_PASSWORDTYPE._serialized_start=2474
  _PASSWORDVALIDATOR_PASSWORDTYPE._serialized_end=2543
  _BILLINGMETRIC._serialized_start=2546
  _BILLINGMETRIC._serialized_end=2742
  _BILLINGMETRIC_TAGSENTRY._serialized_start=2677
  _BILLINGMETRIC_TAGSENTRY._serialized_end=2742
  _BILLINGESTIMATERESPONSE._serialized_start=2744
  _BILLINGESTIMATERESPONSE._serialized_end=2841
  _CLUSTERSSTATS._serialized_start=2843
  _CLUSTERSSTATS._serialized_end=2882
# @@protoc_insertion_point(module_scope)
