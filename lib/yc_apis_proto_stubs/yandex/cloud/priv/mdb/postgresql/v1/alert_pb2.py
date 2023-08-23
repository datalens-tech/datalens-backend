# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/postgresql/v1/alert.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n/yandex/cloud/priv/mdb/postgresql/v1/alert.proto\x12#yandex.cloud.priv.mdb.postgresql.v1\"\x91\x01\n\x05\x41lert\x12\x0e\n\x06metric\x18\x01 \x01(\t\x12\x1a\n\x12\x63ritical_threshold\x18\x02 \x01(\x01\x12\x19\n\x11warning_threshold\x18\x03 \x01(\x01\x12\x41\n\tcondition\x18\x04 \x01(\x0e\x32..yandex.cloud.priv.mdb.postgresql.v1.Condition\"\xc3\x01\n\nAlertGroup\x12\x16\n\x0e\x61lert_group_id\x18\x01 \x01(\t\x12\x12\n\ncluster_id\x18\x02 \x01(\t\x12:\n\x06\x61lerts\x18\x03 \x03(\x0b\x32*.yandex.cloud.priv.mdb.postgresql.v1.Alert\x12\x1c\n\x14monitoring_folder_id\x18\x04 \x01(\t\x12\x1d\n\x15notification_channels\x18\x05 \x03(\t\x12\x10\n\x08\x64isabled\x18\x06 \x01(\x08*\x94\x01\n\tCondition\x12\x19\n\x15\x43ONDITION_UNSPECIFIED\x10\x00\x12\x10\n\x0c\x43ONDITION_EQ\x10\x01\x12\x10\n\x0c\x43ONDITION_NE\x10\x02\x12\x10\n\x0c\x43ONDITION_GT\x10\x03\x12\x10\n\x0c\x43ONDITION_LT\x10\x04\x12\x11\n\rCONDITION_GTE\x10\x05\x12\x11\n\rCONDITION_LTE\x10\x06\x42\x62\x42\x03PPAZ[a.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/postgresql/v1;postgresqlb\x06proto3')

_CONDITION = DESCRIPTOR.enum_types_by_name['Condition']
Condition = enum_type_wrapper.EnumTypeWrapper(_CONDITION)
CONDITION_UNSPECIFIED = 0
CONDITION_EQ = 1
CONDITION_NE = 2
CONDITION_GT = 3
CONDITION_LT = 4
CONDITION_GTE = 5
CONDITION_LTE = 6


_ALERT = DESCRIPTOR.message_types_by_name['Alert']
_ALERTGROUP = DESCRIPTOR.message_types_by_name['AlertGroup']
Alert = _reflection.GeneratedProtocolMessageType('Alert', (_message.Message,), {
  'DESCRIPTOR' : _ALERT,
  '__module__' : 'yandex.cloud.priv.mdb.postgresql.v1.alert_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.postgresql.v1.Alert)
  })
_sym_db.RegisterMessage(Alert)

AlertGroup = _reflection.GeneratedProtocolMessageType('AlertGroup', (_message.Message,), {
  'DESCRIPTOR' : _ALERTGROUP,
  '__module__' : 'yandex.cloud.priv.mdb.postgresql.v1.alert_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.postgresql.v1.AlertGroup)
  })
_sym_db.RegisterMessage(AlertGroup)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\003PPAZ[a.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/postgresql/v1;postgresql'
  _CONDITION._serialized_start=435
  _CONDITION._serialized_end=583
  _ALERT._serialized_start=89
  _ALERT._serialized_end=234
  _ALERTGROUP._serialized_start=237
  _ALERTGROUP._serialized_end=432
# @@protoc_insertion_point(module_scope)
