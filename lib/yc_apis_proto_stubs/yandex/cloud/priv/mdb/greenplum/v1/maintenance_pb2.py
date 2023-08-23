# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/greenplum/v1/maintenance.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from yandex.cloud.priv import validation_pb2 as yandex_dot_cloud_dot_priv_dot_validation__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n4yandex/cloud/priv/mdb/greenplum/v1/maintenance.proto\x12\"yandex.cloud.priv.mdb.greenplum.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\"yandex/cloud/priv/validation.proto\"\xd6\x01\n\x11MaintenanceWindow\x12O\n\x07\x61nytime\x18\x01 \x01(\x0b\x32<.yandex.cloud.priv.mdb.greenplum.v1.AnytimeMaintenanceWindowH\x00\x12`\n\x19weekly_maintenance_window\x18\x02 \x01(\x0b\x32;.yandex.cloud.priv.mdb.greenplum.v1.WeeklyMaintenanceWindowH\x00\x42\x0e\n\x06policy\x12\x04\x80\x83\x31\x01\"\x1a\n\x18\x41nytimeMaintenanceWindow\"\xe7\x01\n\x17WeeklyMaintenanceWindow\x12P\n\x03\x64\x61y\x18\x01 \x01(\x0e\x32\x43.yandex.cloud.priv.mdb.greenplum.v1.WeeklyMaintenanceWindow.WeekDay\x12\x16\n\x04hour\x18\x02 \x01(\x03\x42\x08\xba\x89\x31\x04\x31-24\"b\n\x07WeekDay\x12\x18\n\x14WEEK_DAY_UNSPECIFIED\x10\x00\x12\x07\n\x03MON\x10\x01\x12\x07\n\x03TUE\x10\x02\x12\x07\n\x03WED\x10\x03\x12\x07\n\x03THU\x10\x04\x12\x07\n\x03\x46RI\x10\x05\x12\x07\n\x03SAT\x10\x06\x12\x07\n\x03SUN\x10\x07\"\xe1\x01\n\x14MaintenanceOperation\x12\x17\n\x04info\x18\x01 \x01(\tB\t\xca\x89\x31\x05<=256\x12\x31\n\rdelayed_until\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12;\n\x17latest_maintenance_time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12@\n\x1cnext_maintenance_window_time\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.TimestampBcB\x06GPMONTZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplumb\x06proto3')



_MAINTENANCEWINDOW = DESCRIPTOR.message_types_by_name['MaintenanceWindow']
_ANYTIMEMAINTENANCEWINDOW = DESCRIPTOR.message_types_by_name['AnytimeMaintenanceWindow']
_WEEKLYMAINTENANCEWINDOW = DESCRIPTOR.message_types_by_name['WeeklyMaintenanceWindow']
_MAINTENANCEOPERATION = DESCRIPTOR.message_types_by_name['MaintenanceOperation']
_WEEKLYMAINTENANCEWINDOW_WEEKDAY = _WEEKLYMAINTENANCEWINDOW.enum_types_by_name['WeekDay']
MaintenanceWindow = _reflection.GeneratedProtocolMessageType('MaintenanceWindow', (_message.Message,), {
  'DESCRIPTOR' : _MAINTENANCEWINDOW,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.maintenance_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.MaintenanceWindow)
  })
_sym_db.RegisterMessage(MaintenanceWindow)

AnytimeMaintenanceWindow = _reflection.GeneratedProtocolMessageType('AnytimeMaintenanceWindow', (_message.Message,), {
  'DESCRIPTOR' : _ANYTIMEMAINTENANCEWINDOW,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.maintenance_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.AnytimeMaintenanceWindow)
  })
_sym_db.RegisterMessage(AnytimeMaintenanceWindow)

WeeklyMaintenanceWindow = _reflection.GeneratedProtocolMessageType('WeeklyMaintenanceWindow', (_message.Message,), {
  'DESCRIPTOR' : _WEEKLYMAINTENANCEWINDOW,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.maintenance_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.WeeklyMaintenanceWindow)
  })
_sym_db.RegisterMessage(WeeklyMaintenanceWindow)

MaintenanceOperation = _reflection.GeneratedProtocolMessageType('MaintenanceOperation', (_message.Message,), {
  'DESCRIPTOR' : _MAINTENANCEOPERATION,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.maintenance_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.MaintenanceOperation)
  })
_sym_db.RegisterMessage(MaintenanceOperation)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\006GPMONTZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplum'
  _MAINTENANCEWINDOW.oneofs_by_name['policy']._options = None
  _MAINTENANCEWINDOW.oneofs_by_name['policy']._serialized_options = b'\200\2031\001'
  _WEEKLYMAINTENANCEWINDOW.fields_by_name['hour']._options = None
  _WEEKLYMAINTENANCEWINDOW.fields_by_name['hour']._serialized_options = b'\272\2111\0041-24'
  _MAINTENANCEOPERATION.fields_by_name['info']._options = None
  _MAINTENANCEOPERATION.fields_by_name['info']._serialized_options = b'\312\2111\005<=256'
  _MAINTENANCEWINDOW._serialized_start=162
  _MAINTENANCEWINDOW._serialized_end=376
  _ANYTIMEMAINTENANCEWINDOW._serialized_start=378
  _ANYTIMEMAINTENANCEWINDOW._serialized_end=404
  _WEEKLYMAINTENANCEWINDOW._serialized_start=407
  _WEEKLYMAINTENANCEWINDOW._serialized_end=638
  _WEEKLYMAINTENANCEWINDOW_WEEKDAY._serialized_start=540
  _WEEKLYMAINTENANCEWINDOW_WEEKDAY._serialized_end=638
  _MAINTENANCEOPERATION._serialized_start=641
  _MAINTENANCEOPERATION._serialized_end=866
# @@protoc_insertion_point(module_scope)
